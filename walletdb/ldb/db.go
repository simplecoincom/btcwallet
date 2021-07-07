// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/pingcap/goleveldb/leveldb"
	"github.com/pingcap/goleveldb/leveldb/comparer"
	"github.com/pingcap/goleveldb/leveldb/filter"
	"github.com/pingcap/goleveldb/leveldb/iterator"
	"github.com/pingcap/goleveldb/leveldb/opt"
	"github.com/pingcap/goleveldb/leveldb/util"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcwallet/walletdb"
)

const (
	// metadataDbName is the name used for the metadata database.
	metadataDbName = "metadata"
)

var (
	// byteOrder is the preferred byte order used through the database.
	// Sometimes big endian will be used to allow ordered byte sortable
	// integer values.
	byteOrder = binary.LittleEndian

	// bucketIndexPrefix is the prefix used for all entries in the bucket
	// index.
	bucketIndexPrefix = []byte("bidx")

	// bucketSequencePrefix is the prefix used for all entries in the
	// bucket sequence index.
	bucketSeqPrefix = []byte("bseq")

	// curBucketIDKeyName is the name of the key used to keep track of the
	// current bucket ID counter.
	curBucketIDKeyName = []byte("bidx-cbid")

	// metadataBucketID is the ID of the top-level metadata bucket.
	// It is the value 0 encoded as an unsigned big-endian uint32.
	metadataBucketID = [4]byte{}
)

// convertErr converts the passed leveldb error into a database error with an
// equivalent error code  and the passed description.  It also sets the passed
// error as the underlying error.
func convertErr(ldbErr error) error {
	switch {
	// Database open/create errors.
	case ldbErr == leveldb.ErrClosed:
		return walletdb.ErrDbNotOpen

	// Transaction errors.
	case ldbErr == leveldb.ErrSnapshotReleased:
		return walletdb.ErrTxClosed
	case ldbErr == leveldb.ErrIterReleased:
		return walletdb.ErrTxClosed
	}

	return ldbErr
}

// copySlice returns a copy of the passed slice.  This is mostly used to copy
// leveldb iterator keys and values since they are only valid until the iterator
// is moved instead of during the entirety of the transaction.
func copySlice(slice []byte) []byte {
	ret := make([]byte, len(slice))
	copy(ret, slice)
	return ret
}

// cursor is an internal type used to represent a cursor over key/value pairs
// and nested buckets of a bucket and implements the database.Cursor interface.
type cursor struct {
	bucket      *bucket
	dbIter      iterator.Iterator
}

// Enforce cursor implements the database.Cursor interface.
var _ walletdb.ReadWriteCursor = (*cursor)(nil)
var _ walletdb.ReadCursor = (*cursor)(nil)

// Delete removes the current key/value pair the cursor is at without
// invalidating the cursor.
//
// Returns the following errors as required by the interface contract:
//   - ErrIncompatibleValue if attempted when the cursor points to a nested
//     bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the walletdb.ReadWriteCursor interface implementation.
func (c *cursor) Delete() error {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return err
	}

	// Do not allow buckets to be deleted via the cursor.
	key := c.dbIter.Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		return walletdb.ErrIncompatibleValue
	}

	c.bucket.tx.ldbTx.Delete(copySlice(key), nil)
	return nil
}

// First positions the cursor at the first key/value pair and returns whether or
// not the pair exists.
//
// This function is part of the walletdb.ReadCursor interface implementation.
func (c *cursor) First() (key, value []byte) {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil, nil
	}

	// Seek to the first key in both the database and pending iterators and
	// choose the iterator that is both valid and has the smaller key.
	if c.dbIter.First() {
		return c.Key(), c.Value()
	}
	return nil, nil
}

// Last positions the cursor at the last key/value pair and returns whether or
// not the pair exists.
//
// This function is part of the walletdb.ReadCursor interface implementation.
func (c *cursor) Last() (key, value []byte) {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil, nil
	}

	// Seek to the last key in both the database and pending iterators and
	// choose the iterator that is both valid and has the larger key.
	if c.dbIter.Last() {
		return c.Key(), c.Value()
	}
	return nil, nil
}

// Next moves the cursor one key/value pair forward and returns whether or not
// the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Next() (key, value []byte) {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil, nil
	}

	// Move the current iterator to the next entry and choose the iterator
	// that is both valid and has the smaller key.
	if c.dbIter.Next() {
		return c.Key(), c.Value()
	}
	return nil, nil
}

// Prev moves the cursor one key/value pair backward and returns whether or not
// the pair exists.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Prev() (key, value []byte) {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil, nil
	}

	// Move the current iterator to the previous entry and choose the
	// iterator that is both valid and has the larger key.
	if c.dbIter.Prev() {
		return c.Key(), c.Value()
	}
	return nil, nil
}

// Seek positions the cursor at the first key/value pair that is greater than or
// equal to the passed seek key.  Returns false if no suitable key was found.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Seek(seek []byte) (key, value []byte) {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil, nil
	}

	// Seek to the provided key in both the database and pending iterators
	// then choose the iterator that is both valid and has the larger key.
	seekKey := bucketizedKey(c.bucket.id, seek)
	if c.dbIter.Seek(seekKey) {
		return c.Key(), c.Value()
	}
	return nil, nil
}

// rawKey returns the current key the cursor is pointing to without stripping
// the current bucket prefix or bucket index prefix.
func (c *cursor) rawKey() []byte {
	return copySlice(c.dbIter.Key())
}

// Key returns the current key the cursor is pointing to.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Key() []byte {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	// Slice out the actual key name and make a copy since it is no longer
	// valid after iterating to the next item.
	//
	// The key is after the bucket index prefix and parent ID when the
	// cursor is pointing to a nested bucket.
	key := c.dbIter.Key()
	if bytes.HasPrefix(key, bucketIndexPrefix) {
		key = key[len(bucketIndexPrefix)+4:]
		return copySlice(key)
	}

	// The key is after the bucket ID when the cursor is pointing to a
	// normal entry.
	key = key[len(c.bucket.id):]
	return copySlice(key)
}

// rawValue returns the current value the cursor is pointing to without
// stripping without filtering bucket index values.
func (c *cursor) rawValue() []byte {
	return copySlice(c.dbIter.Value())
}

// Value returns the current value the cursor is pointing to.  This will be nil
// for nested buckets.
//
// This function is part of the database.Cursor interface implementation.
func (c *cursor) Value() []byte {
	// Ensure transaction state is valid.
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}

	// Return nil for the value when the cursor is pointing to a nested
	// bucket.
	if bytes.HasPrefix(c.dbIter.Key(), bucketIndexPrefix) {
		return nil
	}

	return copySlice(c.dbIter.Value())
}

// cursorType defines the type of cursor to create.
type cursorType int

// The following constants define the allowed cursor types.
const (
	// ctKeys iterates through all of the keys in a given bucket.
	ctKeys cursorType = iota

	// ctBuckets iterates through all directly nested buckets in a given
	// bucket.
	ctBuckets

	// ctFull iterates through both the keys and the directly nested buckets
	// in a given bucket.
	ctFull
)

// cursorFinalizer is either invoked when a cursor is being garbage collected or
// called manually to ensure the underlying cursor iterators are released.
func cursorFinalizer(c *cursor) {
	c.dbIter.Release()
}

// newCursor returns a new cursor for the given bucket, bucket ID, and cursor
// type.
//
// NOTE: The caller is responsible for calling the cursorFinalizer function on
// the returned cursor.
func newCursor(b *bucket, bucketID []byte, cursorTyp cursorType) *cursor {
	var dbIter iterator.Iterator
	switch cursorTyp {
	case ctKeys:
		keyRange := util.BytesPrefix(bucketID)
		dbIter = b.tx.ldbTx.NewIterator(keyRange, nil)

	case ctBuckets:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>

		// Create an iterator for the both the database and the pending
		// keys which are prefixed by the bucket index identifier and
		// the provided bucket ID.
		prefix := make([]byte, len(bucketIndexPrefix)+4)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		bucketRange := util.BytesPrefix(prefix)
		dbIter = b.tx.ldbTx.NewIterator(bucketRange, nil)

	case ctFull:
		fallthrough
	default:
		// The serialized bucket index key format is:
		//   <bucketindexprefix><parentbucketid><bucketname>
		prefix := make([]byte, len(bucketIndexPrefix)+4)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], bucketID)
		bucketRange := util.BytesPrefix(prefix)
		keyRange := util.BytesPrefix(bucketID)

		// Since both keys and buckets are needed from the database,
		// create an individual iterator for each prefix and then create
		// a merged iterator from them.
		dbKeyIter := b.tx.ldbTx.NewIterator(keyRange, nil)
		dbBucketIter := b.tx.ldbTx.NewIterator(bucketRange, nil)
		iters := []iterator.Iterator{dbKeyIter, dbBucketIter}
		dbIter = iterator.NewMergedIterator(iters,
			comparer.DefaultComparer, true)
	}

	// Create the cursor using the iterators.
	return &cursor{bucket: b, dbIter: dbIter}
}

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the database.Bucket interface.
type bucket struct {
	tx  *transaction
	id  [4]byte
}

// Enforce bucket implements the database.Bucket interface.
var _ walletdb.ReadWriteBucket = (*bucket)(nil)
var _ walletdb.ReadBucket = (*bucket)(nil)

func (b *bucket) Sequence() uint64 {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return 0
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		return 0
	}

	// Attempt to fetch the sequence for the bucket.
	seq := b.tx.fetchKey(bucketSeqKey(b.id))
	if seq == nil {
		return 0
	}
	seqInt := byteOrder.Uint64(seq)

	return seqInt
}

func (b *bucket) NextSequence() (uint64, error) {
	seq := b.Sequence()
	seq++
	return seq, b.SetSequence(seq)
}

func (b *bucket) SetSequence(seq uint64) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		return walletdb.ErrTxNotWritable
	}

	// Attempt to fetch the sequence for the bucket.
	seqBytes := make([]byte, 8)
	byteOrder.PutUint64(seqBytes, seq)

	return b.tx.ldbTx.Put(bucketSeqKey(b.id), seqBytes, nil)
}

func (b *bucket) Tx() walletdb.ReadWriteTx {
	return b.tx
}

func (b *bucket) DeleteNestedBucket(key []byte) error {
	return b.DeleteBucket(key)
}

func (b *bucket) NestedReadBucket(key []byte) walletdb.ReadBucket {
	return b.Bucket(key)
}

func (b *bucket) NestedReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	return b.Bucket(key)
}

func (b *bucket) ReadCursor() walletdb.ReadCursor {
	return b.cursor()
}

func (b *bucket) ReadWriteCursor() walletdb.ReadWriteCursor {
	return b.cursor()
}

// bucketIndexKey returns the actual key to use for storing and retrieving a
// child bucket in the bucket index.  This is required because additional
// information is needed to distinguish nested buckets with the same name.
func bucketIndexKey(parentID [4]byte, key []byte) []byte {
	// The serialized bucket index key format is:
	//   <bucketindexprefix><parentbucketid><bucketname>
	indexKey := make([]byte, len(bucketIndexPrefix)+4+len(key))
	copy(indexKey, bucketIndexPrefix)
	copy(indexKey[len(bucketIndexPrefix):], parentID[:])
	copy(indexKey[len(bucketIndexPrefix)+4:], key)
	return indexKey
}

// bucketSeqKey returns the actual key to use for storing and retrieving a
// sequence in the bucket sequence index. This is required to support BDB-like
// functionality required by the walletdb interface.
func bucketSeqKey(parentID [4]byte) []byte {
	seqKey := make([]byte, len(bucketSeqPrefix)+4)
	copy(seqKey, bucketSeqPrefix)
	copy(seqKey[len(bucketSeqPrefix):], parentID[:])
	return seqKey
}

// bucketizedKey returns the actual key to use for storing and retrieving a key
// for the provided bucket ID.  This is required because bucketizing is handled
// through the use of a unique prefix per bucket.
func bucketizedKey(bucketID [4]byte, key []byte) []byte {
	// The serialized block index key format is:
	//   <bucketid><key>
	bKey := make([]byte, 4+len(key))
	copy(bKey, bucketID[:])
	copy(bKey[4:], key)
	return bKey
}

// Bucket retrieves a nested bucket with the given key.  Returns nil if
// the bucket does not exist.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Bucket(key []byte) walletdb.ReadWriteBucket {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// Attempt to fetch the ID for the child bucket.  The bucket does not
	// exist if the bucket index entry does not exist.
	childID := b.tx.fetchKey(bucketIndexKey(b.id, key))
	if childID == nil {
		return nil
	}

	childBucket := &bucket{tx: b.tx}
	copy(childBucket.id[:], childID)
	return childBucket
}

// CreateBucket creates and returns a new nested bucket with the given key.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketExists if the bucket already exists
//   - ErrBucketNameRequired if the key is empty
//   - ErrIncompatibleValue if the key is otherwise invalid for the particular
//     implementation
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) CreateBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		return nil, walletdb.ErrTxNotWritable
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		return nil, walletdb.ErrBucketNameRequired
	}

	// Ensure bucket does not already exist.
	bidxKey := bucketIndexKey(b.id, key)
	hasIdx, err := b.tx.ldbTx.Has(bidxKey, nil)
	if err != nil {
		return nil, convertErr(err)
	}
	if hasIdx {
		return nil, walletdb.ErrBucketExists
	}

	// the case of the special internal block index, keep the fixed ID.
	childID, err := b.tx.nextBucketID()
	if err != nil {
		return nil, err
	}

	// Find the appropriate next bucket ID to use for the new bucket.  In
	// Add the new bucket to the bucket index.
	if err = b.tx.ldbTx.Put(bidxKey, childID[:], nil); err != nil {
		return nil, convertErr(err)
	}
	bseqKey := bucketSeqKey(childID)
	if err = b.tx.ldbTx.Put(bseqKey, []byte{0,0,0,0,0,0,0,0}, nil); err != nil {
		return nil, convertErr(err)
	}
	return &bucket{tx: b.tx, id: childID}, nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with the
// given key if it does not already exist.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketNameRequired if the key is empty
//   - ErrIncompatibleValue if the key is otherwise invalid for the particular
//     implementation
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) CreateBucketIfNotExists(key []byte) (walletdb.ReadWriteBucket, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		return nil, walletdb.ErrTxNotWritable
	}

	// Return existing bucket if it already exists, otherwise create it.
	if bucket := b.Bucket(key); bucket != nil {
		return bucket, nil
	}
	return b.CreateBucket(key)
}

// DeleteBucket removes a nested bucket with the given key.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketNotFound if the specified bucket does not exist
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) DeleteBucket(key []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		return walletdb.ErrTxNotWritable
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		return walletdb.ErrIncompatibleValue
	}

	// Attempt to fetch the ID for the child bucket.  The bucket does not
	// exist if the bucket index entry does not exist.  In the case of the
	// special internal block index, keep the fixed ID.
	bidxKey := bucketIndexKey(b.id, key)
	childID := b.tx.fetchKey(bidxKey)
	if childID == nil {
		return walletdb.ErrBucketNotFound
	}

	// Remove all nested buckets and their keys.
	childIDs := [][]byte{childID}
	for len(childIDs) > 0 {
		childID = childIDs[len(childIDs)-1]
		childIDs = childIDs[:len(childIDs)-1]

		// Delete all keys in the nested bucket.
		keyCursor := newCursor(b, childID, ctKeys)
		for key, _ := keyCursor.First(); key != nil; key, _ = keyCursor.Next() {
			b.tx.ldbTx.Delete(keyCursor.rawKey(), nil)
		}
		cursorFinalizer(keyCursor)

		// Iterate through all nested buckets.
		bucketCursor := newCursor(b, childID, ctBuckets)
		for key, _ := bucketCursor.First(); key != nil; key, _ = bucketCursor.Next() {
			// Push the id of the nested bucket onto the stack for
			// the next iteration.
			childID := bucketCursor.rawValue()
			childIDs = append(childIDs, childID)

			// Remove the nested bucket from the bucket index.
			b.tx.ldbTx.Delete(bucketCursor.rawKey(), nil)
		}
		cursorFinalizer(bucketCursor)
	}

	// Remove the nested bucket from the bucket index.  Any buckets nested
	// under it were already removed above.
	b.tx.ldbTx.Delete(bidxKey, nil)
	return nil
}

// Cursor returns a new cursor, allowing for iteration over the bucket's
// key/value pairs and nested buckets in forward or backward order.
//
// You must seek to a position using the First, Last, or Seek functions before
// calling the Next, Prev, Key, or Value functions.  Failure to do so will
// result in the same return values as an exhausted cursor, which is false for
// the Prev and Next functions and nil for Key and Value functions.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) cursor() walletdb.ReadWriteCursor {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	// Create the cursor and setup a runtime finalizer to ensure the
	// iterators are released when the cursor is garbage collected.
	c := newCursor(b, b.id[:], ctFull)
	runtime.SetFinalizer(c, cursorFinalizer)
	return c
}

// ForEach invokes the passed function with every key/value pair in the bucket.
// This include nested buckets with a nil value but not the key/value pairs
// within those nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys and/or values.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Invoke the callback for each cursor item.  Return the error returned
	// from the callback when it is non-nil.
	c := newCursor(b, b.id[:], ctFull)
	defer cursorFinalizer(c)
	for key, value := c.First(); key != nil; key, value = c.Next() {
		err := fn(key, value)
		if err != nil {
			return err
		}
	}

	return nil
}

// Put saves the specified key/value pair to the bucket.  Keys that do not
// already exist are added and keys that already exist are overwritten.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Put(key, value []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		return walletdb.ErrTxNotWritable
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		return walletdb.ErrKeyRequired
	}

	return b.tx.ldbTx.Put(bucketizedKey(b.id, key), value, nil)
}

// Get returns the value for the given key.  Returns nil if the key does not
// exist in this bucket.  An empty slice is returned for keys that exist but
// have no value assigned.
//
// NOTE: The value returned by this function is only valid during a transaction.
// Attempting to access it after a transaction has ended results in undefined
// behavior.  Additionally, the value must NOT be modified by the caller.
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Get(key []byte) []byte {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// Nothing to return if there is no key.
	if len(key) == 0 {
		return nil
	}

	val, err := b.tx.ldbTx.Get(bucketizedKey(b.id, key), nil)
	if err != nil {
		return nil
	}

	return val
}

// Delete removes the specified key from the bucket.  Deleting a key that does
// not exist does not return an error.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Bucket interface implementation.
func (b *bucket) Delete(key []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		return walletdb.ErrTxNotWritable
	}

	// Nothing to do if there is no key.
	if len(key) == 0 {
		return nil
	}

	return b.tx.ldbTx.Delete(bucketizedKey(b.id, key), nil)
}

// pendingBlock houses a block that will be written to disk when the database
// transaction is committed.
type pendingBlock struct {
	hash  *chainhash.Hash
	bytes []byte
}

// transaction represents a database transaction.  It can either be read-only or
// read-write and implements the database.Bucket interface.  The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	closed     bool                 // Is the transaction closed?
	writable   bool                 // Is the transaction writable?
	db         *db                  // DB instance the tx was created from.
	ldbTx      *leveldb.Transaction // Underlying transaction.
	metaBucket *bucket              // The root metadata bucket.
	onCommit   func()               // Successful commit callback.
}

// Enforce transaction implements the database.Tx interface.
var _ walletdb.ReadWriteTx = (*transaction)(nil)
var _ walletdb.ReadTx = (*transaction)(nil)

// checkClosed returns an error if the the database or transaction is closed.
func (tx *transaction) checkClosed() error {
	// The transaction is no longer valid if it has been closed.
	if tx.closed {
		return walletdb.ErrTxClosed
	}

	return nil
}

// fetchKey attempts to fetch the provided key from the underlying database.
// Returns nil if the key does not exist.
func (tx *transaction) fetchKey(key []byte) []byte {
	value, err := tx.ldbTx.Get(key, nil)
	if err != nil {
		return nil
	}
	return value
}

// nextBucketID returns the next bucket ID to use for creating a new bucket.
//
// NOTE: This function must only be called on a writable transaction.  Since it
// is an internal helper function, it does not check.
func (tx *transaction) nextBucketID() ([4]byte, error) {
	// Load the currently highest used bucket ID.
	curIDBytes := tx.fetchKey(curBucketIDKeyName)
	curBucketNum := binary.BigEndian.Uint32(curIDBytes)

	// Increment and update the current bucket ID and return it.
	var nextBucketID [4]byte
	binary.BigEndian.PutUint32(nextBucketID[:], curBucketNum+1)
	if err := tx.ldbTx.Put(curBucketIDKeyName, nextBucketID[:], nil); err != nil {
		return [4]byte{}, err
	}
	return nextBucketID, nil
}

// close marks the transaction closed then releases any pending data, the
// underlying snapshot, the transaction read lock, and the write lock when the
// transaction is writable.
func (tx *transaction) close() {
	tx.closed = true

	// Release the snapshot.
	if tx.ldbTx != nil {
		tx.ldbTx.Discard()
		tx.ldbTx = nil
	}

	tx.db.closeLock.RUnlock()

	// Release the writer lock for writable transactions to unblock any
	// other write transaction which are possibly waiting.
	if tx.writable {
		tx.db.writeLock.Unlock()
	}
}

func (tx *transaction) CreateTopLevelBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	return tx.metaBucket.CreateBucketIfNotExists(key)
}

func (tx *transaction) DeleteTopLevelBucket(key []byte) error {
	return tx.metaBucket.DeleteBucket(key)
}

func (tx *transaction) ReadBucket(key []byte) walletdb.ReadBucket {
	return tx.metaBucket.Bucket(key)
}

// ForEachBucket will iterate through all top level buckets.
func (tx *transaction) ForEachBucket(fn func(key []byte) error) error {
	return tx.metaBucket.ForEach(func(k, v []byte) error {
		if v == nil || len(v) == 0 {
			return fn(k)
		}
		return nil
	})
}

func (tx *transaction) ReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	return tx.metaBucket.Bucket(key)
}

// OnCommit sets a function that gets called when a transaction gets committed
// successfully.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) OnCommit(onCommit func()) {
	tx.onCommit = onCommit
}

// Commit commits all changes that have been made to the root metadata bucket
// and all of its sub-buckets to the database cache which is periodically synced
// to persistent storage.  In addition, it commits all new blocks directly to
// persistent storage bypassing the db cache.  Blocks can be rather large, so
// this help increase the amount of cache available for the metadata updates and
// is safe since blocks are immutable.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) Commit() error {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !tx.writable {
		return walletdb.ErrTxNotWritable
	}

	// Regardless of whether the commit succeeds, the transaction is closed
	// on return.
	defer tx.close()

	// Write pending data.  The function will rollback if any errors occur.
	if err := tx.ldbTx.Commit(); err != nil {
		return err
	}

	// Call OnCommit(), set ldbTx to nil, and return no error.
	tx.onCommit()
	tx.ldbTx = nil
	return nil
}

// Rollback undoes all changes that have been made to the root bucket and all of
// its sub-buckets.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) Rollback() error {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return err
	}

	tx.close()
	return nil
}

// db represents a collection of namespaces which are persisted and implements
// the database.DB interface.  All database access is performed through
// transactions which are obtained through the specific Namespace.
type db struct {
	writeLock sync.Mutex   // Limit to one write transaction at a time.
	closeLock sync.RWMutex // Make database close block while txns active.
	closed    bool         // Is the database closed?
	ldb       *leveldb.DB  // underlying leveldb DB.
}

// Enforce db implements the database.DB interface.
var _ walletdb.DB = (*db)(nil)

// begin is the implementation function for the BeginRead(Write)Tx database
// methods.  See their documentation for more details.
func (db *db) begin(writable bool) (*transaction, error) {
	// Whenever a new writable transaction is started, grab the write lock
	// to ensure only a single write transaction can be active at the same
	// time.  This lock will not be released until the transaction is
	// closed (via Rollback or Commit).
	if writable {
		db.writeLock.Lock()
	}

	// Whenever a new transaction is started, grab a read lock against the
	// database to ensure Close will wait for the transaction to finish.
	// This lock will not be released until the transaction is closed (via
	// Rollback or Commit).
	db.closeLock.RLock()
	if db.closed {
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, walletdb.ErrDbNotOpen
	}

	// Grab a transaction. Since we're only allowing one transaction at a
	// time, we're also within the underlying leveldb's limit of one
	// transaction at a time.
	ldbTx, err := db.ldb.OpenTransaction()
	if err != nil {
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, convertErr(err)
	}

	// The metadata bucket is an internal-only bucket, so it has a
	// defined IDs.
	tx := &transaction{
		writable:      writable,
		db:            db,
		ldbTx:         ldbTx,
		onCommit:      func() {return},
	}
	tx.metaBucket = &bucket{tx: tx, id: metadataBucketID}
	return tx, nil
}

// BeginReadTx starts a read-only transaction.  Multiple read-only transactions
// can be started simultaneously while only a single read-write transaction can
// be started at a time.
//
// NOTE: The transaction must be closed by calling Rollbackon it when it is no
// longer needed.  Failure to do so will result in unclaimed memory.
//
// This function is part of the walletdb.DB interface implementation.
func (db *db) BeginReadTx() (walletdb.ReadTx, error) {
	return db.begin(false)
}

// BeginReadTx starts a read-write transaction.  Multiple read-only transactions
// can be started simultaneously while only a single read-write transaction can
// be started at a time.  The call will block when a read-write transaction is
// already open.
//
// NOTE: The transaction must be closed by calling Rollback or Commit on it when
// it is no longer needed.  Failure to do so will result in unclaimed memory.
//
// This function is part of the walletdb.DB interface implementation.
func (db *db) BeginReadWriteTx() (walletdb.ReadWriteTx, error) {
	return db.begin(true)
}

// TODO(aakselrod): implement this
func (db *db) Copy(w io.Writer) error {
	return errors.New("Not implemented yet")
}

// Batch is required by the BatchDB interface
func (db *db) Batch(fn func(tx walletdb.ReadWriteTx) error) error {
	return walletdb.Update(db, fn)
}

// Close cleanly shuts down the database and syncs all data.  It will block
// until all database transactions have been finalized (rolled back or
// committed).
//
// This function is part of the database.DB interface implementation.
func (db *db) Close() error {
	// Since all transactions have a read lock on this mutex, this will
	// cause Close to wait for all readers to complete.
	db.closeLock.Lock()
	defer db.closeLock.Unlock()

	if db.closed {
		return walletdb.ErrDbNotOpen
	}
	db.closed = true

	// NOTE: Since the above lock waits for all transactions to finish and
	// prevents any new ones from being started, it is safe to clear all state
	// without the individual locks.

	// Close the underlying leveldb database.  Any error is saved
	// and returned at the end after the remaining cleanup since the
	// database will be marked closed even if this fails given there is no
	// good way for the caller to recover from a failure here anyways.
	return db.ldb.Close()
}

// filesExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// initDB creates the initial buckets and values used by the package.  This is
// mainly in a separate function for testing purposes.
func initDB(ldb *leveldb.DB) error {
	batch := new(leveldb.Batch)
	// Initialize the current bucket ID as the metadata bucket's ID.
	batch.Put(curBucketIDKeyName, metadataBucketID[:])

	// Write everything as a single batch.
	if err := ldb.Write(batch, nil); err != nil {
		return convertErr(err)
	}

	return nil
}

// openDB opens the database at the provided path.  database.ErrDbDoesNotExist
// is returned if the database doesn't exist and the create flag is not set.
func openDB(dbPath string, create bool) (walletdb.DB, error) {
	// Error if the database doesn't exist and the create flag is not set.
	metadataDbPath := filepath.Join(dbPath, metadataDbName)
	dbExists := fileExists(metadataDbPath)
	if !create && !dbExists {
		return nil, walletdb.ErrDbDoesNotExist
	}

	// Ensure the full path to the database exists.
	if !dbExists {
		// The error can be ignored here since the call to
		// leveldb.OpenFile will fail if the directory couldn't be
		// created.
		_ = os.MkdirAll(dbPath, 0700)
	}

	// Open the metadata database (will create it if needed).
	opts := opt.Options{
		Strict:          opt.DefaultStrict,
		Compression:     opt.NoCompression,
		Filter:          filter.NewBloomFilter(10),
		OpenFilesCacher: opt.NoCacher,
	}
	ldb, err := leveldb.OpenFile(metadataDbPath, &opts)
	if err != nil {
		return nil, convertErr(err)
	}

	// Create the database cache which wraps the underlying leveldb
	// database to provide write caching.
	pdb := &db{ldb: ldb}

	// Perform initial internal bucket and value creation during database
	// creation.
	if create && !dbExists {
		if err := initDB(pdb.ldb); err != nil {
			return nil, err
		}
	}

	// Perform any reconciliation needed between the block and metadata as
	// well as database initialization, if needed.
	return pdb, nil
}

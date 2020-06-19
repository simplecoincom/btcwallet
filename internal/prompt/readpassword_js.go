// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// +build js

package prompt

import (
	"os"
)

func readPassword() ([]byte, error) {
	buf := make([]byte, 1000)
	n, err := os.Stdin.Read(buf)
	buf = buf[:n]
	return buf, err
}

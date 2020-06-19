// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// +build !js

package prompt

import (
	"os"

	"golang.org/x/crypto/ssh/terminal"
)

func readPassword() ([]byte, error) {
	return terminal.ReadPassword(int(os.Stdin.Fd()))
}

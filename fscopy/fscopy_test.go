// Copyright 2023 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package fscopy

// This file was extracted from cockroachdb/replicator at ee8e2894.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopy(t *testing.T) {
	r := require.New(t)

	root, err := os.MkdirTemp("", "TestCopy")
	r.NoError(err)
	defer func() {
		r.NoError(os.RemoveAll(root))
	}()

	r.NoError(Copy(os.DirFS("./testdata/"), root))

	buf, err := os.ReadFile(filepath.Join(root, "a/b/c/hello.txt"))
	r.NoError(err)
	r.Equal([]byte("Hello World!\n"), buf)
}

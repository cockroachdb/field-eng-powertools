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

// Package fscopy contains a utility for copying the contents of an
// [fs.FS] into the OS filesystem.
package fscopy

// This file was extracted from cockroachdb/replicator at ee8e2894.

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Copy writes the contents of the given FS to files within the given
// output path in the OS filesystem.
func Copy(from fs.FS, toPath string) error {
	absPath, err := filepath.Abs(toPath)
	if err != nil {
		return fmt.Errorf("%s: %w", toPath, err)
	}
	return fs.WalkDir(from, ".",
		func(walkPath string, d fs.DirEntry, walkErr error) (err error) {
			outPath := filepath.Join(absPath, walkPath)
			defer func() {
				if err != nil {
					err = fmt.Errorf("%s -> %s: %w", walkPath, outPath, err)
				}
			}()
			// The error argument is non-nil if WalkDir cannot traverse
			// into the given directory. We'll return it to stop.
			if walkErr != nil {
				return walkErr
			}
			// Ensure that directories exist.
			if d.IsDir() {
				return os.MkdirAll(outPath, 0755)
			}
			// Ignore anything else that's not a regular file.
			if !d.Type().IsRegular() {
				return nil
			}
			// Open the source file.
			in, err := from.Open(walkPath)
			if err != nil {
				return err
			}
			// Overwrite the target file.
			out, err := os.OpenFile(outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}

			if _, err := io.Copy(out, in); err != nil {
				_ = out.Close()
				return err
			}

			return out.Close()
		})
}

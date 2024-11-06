// Copyright 2024 The Cockroach Authors
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

package subfs

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestSubFS(t *testing.T) {
	r := require.New(t)

	sub := &SubstitutingFS{
		FS: &fstest.MapFS{
			"test.txt": &fstest.MapFile{
				Data: []byte("Hello __FOO__"),
			},
		},
		Replacer: strings.NewReplacer("__FOO__", "world!"),
	}

	data, err := fs.ReadFile(sub, "test.txt")
	r.NoError(err)
	r.Equal("Hello world!", string(data))
}

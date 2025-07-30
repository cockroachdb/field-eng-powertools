// Copyright 2025 The Cockroach Authors
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

package retry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExpBackoff(t *testing.T) {
	tests := []struct {
		name      string
		base, max time.Duration
		expected  []time.Duration
		err       string
	}{
		{
			"millis",
			time.Millisecond,
			4 * time.Millisecond,
			[]time.Duration{
				time.Millisecond,
				2 * time.Millisecond,
				4 * time.Millisecond,
				4 * time.Millisecond,
			},
			"",
		},

		{
			"seconds",
			time.Second,
			64 * time.Second,
			[]time.Duration{
				time.Second,
				2 * time.Second,
				4 * time.Second,
				8 * time.Second,
				16 * time.Second,
				32 * time.Second,
				64 * time.Second,
				64 * time.Second,
				64 * time.Second,
			},
			"",
		},
		{
			"minutes",
			2 * time.Minute,
			time.Hour,
			[]time.Duration{
				2 * time.Minute,
				4 * time.Minute,
				8 * time.Minute,
				16 * time.Minute,
				32 * time.Minute,
				time.Hour,
				time.Hour,
			},
			"",
		},
		{
			"invalid base",
			2 * time.Hour,
			4 * time.Hour,
			nil,
			"invalid argument",
		},
		{
			"invalid max",
			2 * time.Minute,
			2 * time.Hour,
			nil,
			"invalid argument",
		},
		{
			"base  greater than max",
			2 * time.Minute,
			1 * time.Minute,
			nil,
			"invalid argument",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r, err := NewExpBackoff(tt.base, tt.max, len(tt.expected))
			if tt.err != "" {
				a.ErrorContains(err, tt.err)
				return
			}
			a.NoError(err)
			for _, e := range tt.expected {
				delay, ok := r.Next()
				a.True(ok)
				a.Equal(e, delay)
			}
			_, ok := r.Next()
			a.False(ok)
		})
	}
}

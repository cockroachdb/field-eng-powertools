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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	gr "github.com/sethvargo/go-retry"
	"github.com/stretchr/testify/assert"
)

func TestPlugin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stopCtx := stopper.WithContext(ctx)
	counter := 0
	retries := 4
	op := func(ctx *stopper.Context) error {
		counter++
		if counter <= retries {
			return ErrRetriable
		}
		return nil
	}
	a := assert.New(t)
	backoff := gr.NewConstant(time.Second)
	err := Retry(stopCtx, backoff, op)
	a.NoError(err)
	a.Equal(retries, counter-1)
}

func TestRetry(t *testing.T) {
	tests := []struct {
		name           string
		base, max      time.Duration
		limit, retries int
		opErr          string
		wantErr        string
	}{
		{
			"ok",
			time.Millisecond,
			4 * time.Millisecond,
			10,
			6,
			"",
			"",
		},
		{
			"non_retriable",
			time.Millisecond,
			4 * time.Millisecond,
			10,
			6,
			"permanent failure",
			"permanent failure",
		},
		{
			"too many retries",
			time.Millisecond,
			4 * time.Millisecond,
			5,
			6,
			"",
			"too many retries",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stopCtx := stopper.WithContext(ctx)
			counter := 0
			op := func(ctx *stopper.Context) error {
				if tt.opErr != "" {
					return errors.New(tt.opErr)
				}
				counter++
				if counter <= tt.retries {
					return ErrRetriable
				}
				return nil
			}
			a := assert.New(t)
			backoff, err := NewExpBackoff(tt.base, tt.max, tt.limit)
			a.NoError(err)
			err = Retry(stopCtx, backoff, op)
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.NoError(err)
			a.Equal(tt.retries, counter-1)
		})
	}
}

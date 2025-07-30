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

// Package retry provides a utility to retry operations that
// fail with a transient error, based on a supplied backoff strategy.
package retry

import (
	"errors"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
)

var (
	// ErrMaxRetries is raised when we reach the maximum number of retries.
	ErrMaxRetries = errors.New("too many retries")
	// ErrRetriable tags errors from operation that can be retried.
	ErrRetriable = errors.New("retriable error")
)

// Operation to be retried.
type Operation func(*stopper.Context) error

// Backoff strategy
type Backoff interface {
	// Next determines how long we have to wait in this current iteration
	// Returns true if we have to stop.
	Next() (time.Duration, bool)
}

// Retry the operation, using the given backoff strategy, if you get a retriable error.
// Backoff strategies from https://github.com/sethvargo/go-retry can be used as well.
func Retry(ctx *stopper.Context, strategy Backoff, op Operation) error {
	for {
		if err := op(ctx); err == nil || !errors.Is(err, ErrRetriable) {
			return err
		}
		backoff, ok := strategy.Next()
		if ok {
			return ErrMaxRetries
		}
		select {
		case <-time.Tick(backoff):
			// try again
		case <-ctx.Stopping():
			return nil
		}
	}
}

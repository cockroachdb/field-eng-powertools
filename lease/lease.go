// Copyright 2026 The Cockroach Authors
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

// Package lease coordinates global, singleton activities using
// exclusive locks on named resources backed by CockroachDB.
package lease

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrCancelSingleton may be returned by callbacks passed to
// [Leases.Singleton] to shut down cleanly.
var ErrCancelSingleton = errors.New("singleton requested cancellation")

// leaseKey is a [context.Context.Value] key used to store a [Lease]
// in its own context.
type leaseKey struct{}

// A Lease represents a time-based, exclusive lock.
type Lease interface {
	// Context will be canceled when the lease has expired.
	Context() context.Context

	// Release terminates the Lease.
	Release()
}

// Leases coordinates behavior across multiple processes.
type Leases interface {
	// Acquire the named leases in an all-or-nothing fashion. A
	// [BusyError] will be returned if another caller has already
	// acquired any of the leases.
	Acquire(ctx context.Context, names ...string) (Lease, error)

	// Singleton executes a callback when all named leases are acquired.
	//
	// The lease will be released in the following circumstances:
	//   - The callback function returns.
	//   - The lease cannot be renewed before it expires.
	//   - The outer context is canceled.
	//
	// If the callback returns a non-nil error, the error will be
	// logged. If the callback returns [ErrCancelSingleton], it will not
	// be retried. In all other cases, the callback function is retried
	// once the leases are re-acquired.
	Singleton(ctx context.Context, names []string, fn func(ctx context.Context) error) error
}

// BusyError is returned by [Leases.Acquire] if another caller holds
// the lease.
type BusyError struct {
	Expiration time.Time
}

func (e *BusyError) Error() string {
	return fmt.Sprintf("lease is held by another caller until %s", e.Expiration)
}

// IsBusy returns the error as a [*BusyError] if it represents a busy
// lease.
func IsBusy(err error) (*BusyError, bool) {
	var busy *BusyError
	return busy, errors.As(err, &busy)
}

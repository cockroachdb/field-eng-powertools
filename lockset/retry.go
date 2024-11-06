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

package lockset

// This file was extracted from cockroachdb/replicator at ee8e2894.

// RetryAtHead returns an error that tasks can use to be retried later,
// once all preceding tasks have completed. If this error is returned
// when there are no preceding tasks, the causal error will be emitted
// from [Executor.Schedule].
func RetryAtHead(cause error) *RetryAtHeadErr {
	return &RetryAtHeadErr{cause, nil}
}

// RetryAtHeadErr is returned by [RetryAtHead].
type RetryAtHeadErr struct {
	cause    error
	fallback func()
}

// Error returns a message.
func (e *RetryAtHeadErr) Error() string { return "callback requested a retry" }

// Or sets a fallback function to invoke if the task was already
// at the head of the global queue. This is used if a cleanup task
// must be run if the task is not going to be retried. The receiver
// is returned.
func (e *RetryAtHeadErr) Or(fn func()) *RetryAtHeadErr { e.fallback = fn; return e }

// Unwrap returns the causal error passed to [RetryAtHead].
func (e *RetryAtHeadErr) Unwrap() error { return e.cause }

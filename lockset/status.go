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

import (
	"fmt"

	"github.com/cockroachdb/field-eng-powertools/notify"
)

// Outcome is a convenience type alias.
type Outcome = *notify.Var[*Status]

// NewOutcome is a convenience method to allocate an Outcome.
func NewOutcome() Outcome {
	return notify.VarOf(executing)
}

// Status is returned by [Executor.Schedule].
type Status struct {
	err error
}

// StatusFor constructs a successful status if err is null. Otherwise,
// it returns a new Status object that returns the error.
func StatusFor(err error) *Status {
	if err == nil {
		return success
	}
	return &Status{err: err}
}

// Sentinel instances of Status.
var (
	executing      = &Status{}
	queued         = &Status{}
	retryQueued    = &Status{}
	retryRequested = &Status{}
	success        = &Status{}
)

// Completed returns true if the callback has been called.
// See also [Status.Success].
func (s *Status) Completed() bool {
	return s == success || s.err != nil
}

// Err returns any error returned by the Task.
func (s *Status) Err() error {
	return s.err
}

// Executing returns true if the Task is currently executing.
func (s *Status) Executing() bool {
	return s == executing
}

// Queued returns true if the Task has not been executed yet.
func (s *Status) Queued() bool {
	return s == queued
}

// Retrying returns true if the callback returned [RetryAtHead] and it
// has not yet been re-attempted.
func (s *Status) Retrying() bool {
	return s == retryRequested || s == retryQueued
}

// Success returns true if the Status represents the successful
// completion of a scheduled waiter.
func (s *Status) Success() bool {
	return s == success
}

func (s *Status) String() string {
	switch s {
	case executing:
		return "executing"
	case queued:
		return "queued"
	case retryQueued:
		return "retryQueued"
	case retryRequested:
		return "retryRequested"
	case success:
		return "success"
	default:
		return fmt.Sprintf("error: %v", s.err)
	}
}

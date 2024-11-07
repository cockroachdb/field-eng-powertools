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

import "context"

// A Task is provided to [Executor.Schedule].
type Task[K any] interface {
	// Call contains the logic associated with the task.
	Call(ctx context.Context) error
	// Keys returns the set of keys that the Task depends upon.
	Keys() []K
}

// TaskFunc returns a [Task] that acquires locks on the given keys and
// then invokes the function callback.
func TaskFunc[K comparable](keys []K, fn func(ctx context.Context, keys []K) error) Task[K] {
	return &taskFunc[K]{fn, dedup(keys)}
}

// canceledTask is used internally for tasks that are canceled before
// being executed.
type canceledTask[K any] struct{}

func (t *canceledTask[K]) Call(context.Context) error { return ErrScheduleCancel }
func (t *canceledTask[K]) Keys() []K                  { return nil }

type taskFunc[K any] struct {
	fn   func(ctx context.Context, keys []K) error
	keys []K
}

func (t *taskFunc[K]) Call(ctx context.Context) error { return t.fn(ctx, t.keys) }
func (t *taskFunc[K]) Keys() []K                      { return t.keys }

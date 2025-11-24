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

import "time"

// Events provides an [Executor] with optional callbacks to monitor the
// performance of enqueued tasks.
//
// See [Executor.SetEvents].
type Events[K any] struct {
	OnComplete func(task Task[K], sinceScheduled time.Duration)
	OnRetried  func(task Task[K])
	OnSchedule func(task Task[K], deferred bool)
	OnStarted  func(task Task[K], sinceScheduled time.Duration)
}

func (e *Events[K]) doComplete(task Task[K], sinceScheduled time.Duration) {
	if e != nil && e.OnComplete != nil {
		e.OnComplete(task, sinceScheduled)
	}
}

func (e *Events[K]) doRetried(task Task[K]) {
	if e != nil && e.OnRetried != nil {
		e.OnRetried(task)
	}
}

func (e *Events[K]) doSchedule(task Task[K], deferred bool) {
	if e != nil && e.OnSchedule != nil {
		e.OnSchedule(task, deferred)
	}
}

func (e *Events[K]) doStarted(task Task[K], sinceScheduled time.Duration) {
	if e != nil && e.OnStarted != nil {
		e.OnStarted(task, sinceScheduled)
	}
}

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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
)

// ErrScheduleCancel will be returned from [context.Cause] if a task's
// context was canceled via the function returned from
// [Executor.Schedule].
var ErrScheduleCancel = fmt.Errorf("%w: Executor.Schedule cancel()", context.Canceled)

// A waiter represents a request to acquire locks on some number of
// keys. Instances of this type should only be accessed while
// holding the parent [Executor.waiterMu] lock.
type waiter[K any] struct {
	keys          []K                 // Desired key set.
	result        notify.Var[*Status] // The outbox for the waiter.
	scheduleStart time.Time           // The time at which Schedule was called.

	mu struct {
		sync.Mutex
		cancel func()  // Non-nil when the task is executing.
		task   Task[K] // nil if already executed.
	}
}

// Executor invokes callbacks based on an in-order admission [Queue] for
// potentially-overlapping sets of keys.
//
// An Executor is internally synchronized and is safe for concurrent
// use. An Executor should not be copied after it has been created.
type Executor[K comparable] struct {
	events *Events[K]            // Injectable callbacks.
	queue  *Queue[K, *waiter[K]] // Internally synchronized.
	runner Runner                // Executes callbacks.
}

// NewExecutor construct an Executor that executes tasks using the given
// [Runner]. If runner is nil, tasks will be executed using
// [context.Background].
//
// See [GoRunner] or
// [github.com/cockroachdb/field-eng-powertools/workgroup.Group].
func NewExecutor[K comparable](runner Runner) *Executor[K] {
	if runner == nil {
		runner = GoRunner(context.Background())
	}
	return &Executor[K]{
		queue:  NewQueue[K, *waiter[K]](),
		runner: runner,
	}
}

// Schedule executes the [Task] once all keys have been locked. The
// result from [Task.Call] is available through the returned [Outcome].
//
// Tasks that need to be retried may return [RetryAtHead]. This will
// execute the task again when all other tasks scheduled before it have
// been completed. A retrying task will continue to hold its key locks
// until the retry has taken place.
//
// A task may return an empty key slice; the task will be executed
// immediately.
//
// Tasks must not schedule new tasks and proceed to wait upon them. This
// will lead to deadlocks.
//
// The cancel function may be called to asynchronously dequeue and
// cancel the task. If the task has already started executing, the
// cancel callback will cancel the task's context.
func (e *Executor[K]) Schedule(task Task[K]) (outcome Outcome, cancel func()) {
	scheduleStart := time.Now()
	keys := task.Keys()

	w := &waiter[K]{
		keys:          keys,
		scheduleStart: scheduleStart,
	}
	w.mu.task = task
	w.result.Set(queued)
	ready, err := e.queue.Enqueue(keys, w)
	if err != nil {
		w.result.Set(StatusFor(err))
		return &w.result, func() {}
	}
	if ready {
		e.events.doSchedule(task, false)
		e.dispose(w, false)
	} else {
		e.events.doSchedule(task, true)
	}
	return &w.result, func() {
		// Swap the callback so that it does nothing. We want to guard
		// against revivifying an already completed waiter, so we
		// look at whether a function is still defined.
		w.mu.Lock()
		needsDispose := w.mu.task != nil
		if needsDispose {
			w.mu.task = &canceledTask[K]{}
		}
		if w.mu.cancel != nil {
			w.mu.cancel()
		}
		w.mu.Unlock()

		// Async cleanup.
		if needsDispose {
			e.dispose(w, true)
		}
	}
}

// SetEvents allows performance-monitoring callbacks to be injected into
// the Executor. This method should be called prior to any call to
// [Executor.Schedule].
func (e *Executor[K]) SetEvents(events *Events[K]) {
	e.events = events
}

// dispose of the waiter callback in a separate goroutine. The waiter
// will be dequeued from the Executor, possibly leading to cascading
// callbacks.
func (e *Executor[K]) dispose(w *waiter[K], cancel bool) {
	work := func(ctx context.Context) {
		ctx, cancelCtx := context.WithCancelCause(ctx)

		// Clear the function reference to make the effects of dispose a
		// one-shot.
		w.mu.Lock()
		w.mu.cancel = func() { cancelCtx(ErrScheduleCancel) }
		task := w.mu.task
		w.mu.task = nil
		w.mu.Unlock()
		startedAtHead := e.queue.IsHead(w)

		// Already executed and/or canceled.
		if task == nil {
			return
		}

		// Executor canceled status or execute the callback.
		var err error
		if cancel {
			err = ErrScheduleCancel
		} else {
			w.result.Set(executing)
			e.events.doStarted(task, time.Since(w.scheduleStart))
			err = tryCall(ctx, task)
			w.mu.Lock()
			w.mu.cancel = nil
			w.mu.Unlock()
			e.events.doComplete(task, time.Since(w.scheduleStart))
		}

		// Once the waiter has been called, update its status and call
		// dequeue to find any tasks that have been unblocked.
		switch t := err.(type) {
		case nil:
			w.result.Set(success)

		case *RetryAtHeadErr:
			// The callback requested to be retried later.
			if startedAtHead {
				// The waiter was already executing at the global head
				// of the queue. Reject the request and execute any
				// fallback handler that may have been provided.
				if t.fallback != nil {
					t.fallback()
				}
				retryErr := t.Unwrap()
				if retryErr == nil {
					w.result.Set(success)
				} else {
					w.result.Set(&Status{err: retryErr})
				}
			} else {
				e.events.doRetried(task)

				// Otherwise, re-enable the waiter. The status will be
				// set to retryRequested for later re-dispatching by the
				// dispose method.
				w.mu.Lock()
				w.mu.cancel = nil
				w.mu.task = task
				w.mu.Unlock()
				w.result.Set(retryRequested)
				endedAtHead := e.queue.IsHead(w)

				// It's possible that another task completed while this
				// one was executing, which moved it to the head of the
				// global queue. If this happens, we need to immediately
				// queue up its retry.
				if !startedAtHead && endedAtHead {
					e.dispose(w, false)
				}

				// We can't dequeue the waiter if it's going to retry at
				// some later point in time. Since we know that the task
				// was running somewhere in the middle of the global
				// queue, there's nothing more that we need to do.
				return
			}
		default:
			w.result.Set(&Status{err: err})
		}

		// Remove the waiter's locks and get a slice of newly-unblocked
		// tasks to kick off.
		next, _ := e.queue.Dequeue(w)
		// Calling dequeue also advances the global queue. If the
		// element at the head of the queue wants to be retried, also
		// add it to the list.
		if head, ok := e.queue.PeekHead(); ok && head != nil {
			if status, _ := head.result.Get(); status == retryRequested {
				head.result.Set(retryQueued)
				next = append(next, head)
			}
		}
		for _, unblocked := range next {
			e.dispose(unblocked, false)
		}
	}

	if err := e.runner.Go(work); err != nil {
		w.result.Set(&Status{err: err})
	}
}

// Wait returns the first non-nil error.
func Wait(ctx context.Context, outcomes []Outcome) error {
outcome:
	for _, outcome := range outcomes {
		for {
			status, changed := outcome.Get()
			if status.Success() {
				continue outcome
			}
			if err := status.Err(); err != nil {
				return err
			}
			select {
			case <-changed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// tryCall invokes the function with a panic handler.
func tryCall[K any](ctx context.Context, task Task[K]) (err error) {
	// Install panic handler before executing user code.
	defer func() {
		x := recover()
		switch t := x.(type) {
		case nil:
		// Success.
		case error:
			err = t
		default:
			err = fmt.Errorf("panic in task: %v", t)
		}
	}()

	return task.Call(ctx)
}

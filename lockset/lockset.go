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

/*
Package lockset contains utilities for ordering access to
potentially-overlapping resources.

A simplified dining philosophers problem might look like this:

	// This function is a placeholder for work to be performed while holding zero or more resource locks
	dine := func(ctx context.Context, forks []string) error { return nil }

	// Construct a new executor that will control access to resources identified by strings
	exe := NewExecutor[string](GoRunner(ctx))

	// Set up our tasks. We have three tasks, each of which needs two resources labeled a-c
	alice := TaskFunc([]string{"a", "b"}, dine)
	bob := TaskFunc([]string{"b", "c"}, dine)
	carol := TaskFunc([]string{"c", "a"}, dine)

	// Schedule our tasks
	aliceOut, _ := exe.Schedule(alice)
	bobOut, _ := exe.Schedule(bob)
	carolOut, _ := exe.Schedule(carol)

	// wait until everyone is done
	Wait(ctx, []Outcome{aliceOut, bobOut, carolOut})

You construct an Executor, which will coordinate tasks. Each Task needs zero or more "locks": these are not actually
mutexes, or even software objects per se, but are simply identifiers for the resource being protected under the lock.
When you Schedule a Task, you get an Outcome, which can be awaited.

Also included in this package is Queue, which implements the core lock-queueing logic, but is generic across value
types and could be used in other applications with similar dependency-planning needs.
*/
package lockset

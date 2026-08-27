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

import "context"

// A Runner is passed to [NewExecutor] to begin the execution of tasks.
type Runner interface {
	// Go should execute the function in a non-blocking fashion.
	Go(func(context.Context)) error
}

// GoRunner returns a Runner that executes tasks using the go keyword
// and the specified context.
func GoRunner(ctx context.Context) Runner { return &goRunner{ctx} }

type goRunner struct {
	ctx context.Context
}

func (r *goRunner) Go(fn func(context.Context)) error {
	go fn(r.ctx)
	return nil
}

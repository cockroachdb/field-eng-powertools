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
	"errors"
	"time"
)

// ErrInvalidArg is raised if an invalid argument is passed to a backoff strategy.
var ErrInvalidArg = errors.New("invalid argument")

type expBackoff struct {
	baseDelay    time.Duration
	currentDelay time.Duration
	limit        int // 0 = forever
	maxDelay     time.Duration
	tryCount     int
}

var _ Backoff = &expBackoff{}

// NewExpBackoff build an exponential backoff strategy.
// Valid maxDelay must be within a millisecond and one hour.
// Use limit=0 for unlimited retries.
func NewExpBackoff(baseDelay time.Duration, maxDelay time.Duration, limit int) (Backoff, error) {
	if maxDelay > time.Hour {
		return nil, ErrInvalidArg
	}
	if baseDelay > maxDelay {
		return nil, ErrInvalidArg
	}
	if maxDelay < time.Millisecond {
		return nil, ErrInvalidArg
	}
	if limit < 0 {
		return nil, ErrInvalidArg
	}
	return &expBackoff{
		baseDelay: baseDelay,
		limit:     limit,
		maxDelay:  maxDelay,
	}, nil
}

// Next implements Backoff
func (e *expBackoff) Next() (time.Duration, bool) {
	if e.limit != 0 && e.tryCount >= e.limit {
		return 0, false
	}
	e.tryCount++
	if e.currentDelay >= e.maxDelay {
		return e.maxDelay, true
	}
	e.currentDelay = e.baseDelay << (e.tryCount - 1)
	if e.maxDelay > 0 && e.currentDelay > e.maxDelay {
		return e.maxDelay, true
	}
	return e.currentDelay, true
}

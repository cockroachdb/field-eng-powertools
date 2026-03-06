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

package lease

import (
	"context"
	_ "embed" // embedding sql statements
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"time"

	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	defaultLifetime = time.Minute
	defaultPoll     = time.Second
	defaultRetry    = time.Second
)

var (
	//go:embed sql/create_table.sql
	createTableSQL string
	//go:embed sql/acquire.sql
	acquireSQL string
	//go:embed sql/release.sql
	releaseSQL string
	//go:embed sql/renew.sql
	renewSQL string
)

// Config is passed to [New].
type Config struct {
	Conn crdbpgx.Conn // Database access.
	// Table is the fully-qualified lease table name. A [fmt.Stringer]
	// is used instead of a plain string so that implementations can
	// provide proper SQL identifier quoting and sanitization.
	Table fmt.Stringer

	// Guard provides a quiescent period between when a lease is
	// considered to be expired (i.e. a lease callback's context is
	// canceled) and its published expiration time. This is useful for
	// situations where it may not be possible to immediately cancel all
	// side effects of a lease callback (e.g. it makes an external
	// network request).
	Guard time.Duration

	Lifetime   time.Duration // Duration of the lease.
	Poll       time.Duration // How often to re-check for an available lease.
	RetryDelay time.Duration // Delay between re-executing a callback.
}

// sanitize checks for mis-configuration and applies sane defaults.
func (c *Config) sanitize() error {
	if c.Conn == nil {
		return errors.New("pool must not be nil")
	}
	if c.Table == nil || c.Table.String() == "" {
		return errors.New("table must be set")
	}
	// OK for Guard to be zero.
	if c.Lifetime == 0 {
		c.Lifetime = defaultLifetime
	}
	if c.Poll == 0 {
		c.Poll = defaultPoll
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = defaultRetry
	}
	return nil
}

type leaseRow struct {
	expires time.Time
	names   []string
	nonce   uuid.UUID
}

// holder coordinates global, singleton activities.
type holder struct {
	cfg      Config
	identity string // For operator convenience, not correctness.
	sql      struct {
		acquire string
		release string
		renew   string
	}
}

// leaseFacade implements the public [Lease] interface.
type leaseFacade struct {
	cancel func()
	ctx    context.Context
}

var _ Lease = (*leaseFacade)(nil)

// Context implements [Lease].
func (f *leaseFacade) Context() context.Context {
	return f.ctx
}

// Release implements [Lease].
func (f *leaseFacade) Release() {
	f.cancel()
}

var _ Leases = (*holder)(nil)

// Init creates the lease table and grants access to the given role.
// The table and role parameters use [fmt.Stringer] to allow
// implementations to provide proper SQL identifier quoting.
func Init(ctx context.Context, conn crdbpgx.Conn, table fmt.Stringer, role fmt.Stringer) error {
	return crdbpgx.ExecuteTx(ctx, conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, fmt.Sprintf(createTableSQL, table, role))
		return err
	})
}

// New constructs an instance of [Leases].
func New(ctx context.Context, cfg Config) (Leases, error) {
	if err := cfg.sanitize(); err != nil {
		return nil, err
	}
	l := &holder{cfg: cfg}
	l.sql.acquire = fmt.Sprintf(acquireSQL, cfg.Table)
	l.sql.release = fmt.Sprintf(releaseSQL, cfg.Table)
	l.sql.renew = fmt.Sprintf(renewSQL, cfg.Table)
	l.identity = identity()
	return l, nil
}

func identity() string {
	if name, err := os.Hostname(); err == nil {
		return fmt.Sprintf("%s:%d", name, os.Getpid())
	}
	slog.Warn("could not determine OS hostname, will use UUID instead")
	return uuid.NewString()
}

// Acquire the named leases, keep them alive, and return a facade.
func (l *holder) Acquire(ctx context.Context, names ...string) (Lease, error) {
	lr, ok, err := l.acquire(ctx, names)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &BusyError{Expiration: lr.expires}
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		l.keepRenewed(ctx, lr)
		cancel()
	}()

	ret := &leaseFacade{
		cancel: func() {
			_, _ = l.release(context.Background(), lr)
			cancel()
		},
	}
	// Export the lease for testing purposes.
	ret.ctx = context.WithValue(ctx, leaseKey{}, Lease(ret))

	runtime.SetFinalizer(ret, func(f *leaseFacade) { f.Release() })

	return ret, nil
}

// Singleton executes a callback when the named leases are acquired.
func (l *holder) Singleton(
	ctx context.Context, names []string, fn func(ctx context.Context) error,
) error {
	loopBehavior := func() (time.Duration, error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		lease, err := l.Acquire(ctx, names...)
		if err != nil {
			if _, busy := IsBusy(err); busy {
				slog.Debug("lease is busy, waiting", "lease", names)
				// Jitter the polling time to even out the load.
				return l.cfg.Poll + time.Duration(rand.Int31n(10))*time.Millisecond, nil
			}

			slog.Error("unable to acquire lease", "lease", names, "error", err)
			return l.cfg.RetryDelay, nil
		}
		defer lease.Release()

		// Execute the callback.
		err = fn(lease.Context())

		if errors.Is(err, ErrCancelSingleton) || errors.Is(err, context.Canceled) {
			slog.Debug("callback requested shutdown or was canceled", "lease", names)
			return -1, err
		}
		slog.Error("lease callback exited; continuing", "lease", names, "error", err)
		return l.cfg.RetryDelay, err
	}

	for {
		delay, err := loopBehavior()
		if delay < 0 {
			return nil
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return err
		}
	}
}

// waitToAcquire blocks until the named lease can be acquired. If the
// context is canceled, this method will return a zero-value lease.
func (l *holder) waitToAcquire(ctx context.Context, names []string) (acquired leaseRow, ok bool) {
	// The zero value means the first read to the timer channel will
	// return immediately.
	t := time.NewTimer(0)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Debug("context canceled before acquisition", "lease", names)
			return leaseRow{}, false
		case <-t.C:
		}

		slog.Debug("attempting to acquire", "lease", names)
		ret, ok, err := l.acquire(ctx, names)

		switch {
		case err != nil:
			slog.Warn("unable to acquire, will retry", "lease", names, "error", err)
		case ok:
			slog.Debug("acquired", "lease", names)
			return ret, true
		default:
			slog.Debug("waiting", "lease", names)
		}

		// We want to poll, rather than to wait for the known lease to
		// expire to detect a deleted lease sooner. The random delay
		// helps to smear the requests over time.
		pollDelay := l.cfg.Poll/2 + time.Duration(rand.Int63n(int64(l.cfg.Poll)))
		t.Reset(pollDelay)
	}
}

// keepRenewed will return when the lease cannot be renewed or when the
// context is canceled. It returns the status of the lease at the last
// successful renewal to aid in testing.
func (l *holder) keepRenewed(ctx context.Context, tgt leaseRow) leaseRow {
	var retry bool
	for {
		tgt, retry = l.keepRenewedOnce(ctx, tgt, time.Now().UTC())
		if !retry {
			return tgt
		}
	}
}

// keepRenewedOnce allows the keepRenewed behavior to be tested without
// depending on the system clock.
func (l *holder) keepRenewedOnce(
	ctx context.Context, tgt leaseRow, now time.Time,
) (_ leaseRow, retry bool) {
	remaining := tgt.expires.Sub(now)
	// We haven't been able to renew before hitting the guard
	// duration, so return and allow the lease to be canceled.
	if remaining < l.cfg.Guard {
		return tgt, false
	}
	// Wait up to half of the remaining validity time before
	// attempting to renew, but rate-limit to the polling interval.
	delay := remaining / 2
	if delay < l.cfg.Poll {
		delay = l.cfg.Poll
	}

	// Wait until it's time to do something, or we're canceled.
	select {
	case <-ctx.Done():
		return tgt, false
	case <-time.After(delay):
	}

	var ok bool
	var err error
	tgt, ok, err = l.renew(ctx, tgt)

	switch {
	case errors.Is(err, context.Canceled):
		slog.Debug("context canceled", "lease", tgt.names, "expires", tgt.expires)
		return tgt, false
	case err != nil:
		slog.Warn("could not renew lease", "lease", tgt.names, "expires", tgt.expires, "error", err)
		return tgt, true
	case !ok:
		slog.Debug("lease was hijacked", "lease", tgt.names, "expires", tgt.expires)
		return tgt, false
	default:
		slog.Debug("renewed successfully", "lease", tgt.names, "expires", tgt.expires)
		return tgt, true
	}
}

// acquire returns a non-nil leaseRow if it was able to acquire the
// named lease.
func (l *holder) acquire(
	ctx context.Context, names []string,
) (lr leaseRow, acquired bool, err error) {
	if len(names) == 0 {
		err = errors.New("no lease names provided")
		return
	}
	err = crdbpgx.ExecuteTx(ctx, l.cfg.Conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var innerErr error
		lr, acquired, innerErr = l.tryAcquire(ctx, tx, names, time.Now())
		return innerErr
	})
	return
}

// copy is used by test code to return a shallow copy of the receiver.
func (l *holder) copy() *holder {
	cpy := *l
	return &cpy
}

// tryAcquire returns the current state of the named lease in the
// database. The ok value will be true if this call acquired the lease.
func (l *holder) tryAcquire(
	ctx context.Context, tx pgx.Tx, names []string, now time.Time,
) (lr leaseRow, acquired bool, err error) {
	// We only have millisecond-level resolution in the db.
	now = now.UTC().Truncate(time.Millisecond)
	expires := now.Add(l.cfg.Lifetime)
	var blockedUntil *time.Time
	var nonce uuid.UUID

	if err := tx.QueryRow(ctx,
		l.sql.acquire,
		names,
		expires,
		l.identity,
		now,
	).Scan(&blockedUntil, &nonce); err != nil {
		return leaseRow{}, false, fmt.Errorf("acquire lease: %w", err)
	}
	if blockedUntil != nil {
		return leaseRow{*blockedUntil, names, uuid.UUID{}}, false, nil
	}
	return leaseRow{expires, names, nonce}, true, nil
}

// release destroys the given lease.
func (l *holder) release(ctx context.Context, rel leaseRow) (bool, error) {
	var ret bool
	err := crdbpgx.ExecuteTx(ctx, l.cfg.Conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var innerErr error
		ret, innerErr = l.tryRelease(ctx, tx, rel)
		return innerErr
	})
	return ret, err
}

// tryRelease deletes the lease from the database.
func (l *holder) tryRelease(ctx context.Context, tx pgx.Tx, rel leaseRow) (ok bool, err error) {
	tag, err := tx.Exec(ctx, l.sql.release, rel.names, rel.nonce)
	if err != nil {
		return false, fmt.Errorf("release lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	return true, nil
}

func (l *holder) renew(ctx context.Context, tgt leaseRow) (renewed leaseRow, ok bool, err error) {
	err = crdbpgx.ExecuteTx(ctx, l.cfg.Conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		var innerErr error
		renewed, ok, innerErr = l.tryRenew(ctx, tx, tgt, time.Now())
		return innerErr
	})
	return
}

// tryRenew updates the lease record in the database. If successful, the
// input leaseRow will be updated with the new expiration time and
// returned to the caller. The boolean return value will be false if the
// lease was stolen.
func (l *holder) tryRenew(
	ctx context.Context, tx pgx.Tx, tgt leaseRow, now time.Time,
) (leaseRow, bool, error) {
	now = now.UTC()
	expires := now.Add(l.cfg.Lifetime)

	tag, err := tx.Exec(ctx, l.sql.renew, expires, tgt.names, tgt.nonce)
	if err != nil {
		return tgt, false, fmt.Errorf("renew lease: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return tgt, false, nil
	}

	tgt.expires = expires
	return tgt, true, nil
}

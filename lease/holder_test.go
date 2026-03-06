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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	crdbpgx "github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgxv5"
	"github.com/cockroachdb/cockroach-go/v2/testserver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type ident struct {
	name string
}

// String implements [fmt.Stringer].
func (t ident) String() string {
	return t.name
}

var _ fmt.Stringer = ident{}

// testSetup starts a CockroachDB test server and returns a pool and
// cleanup function.
func testSetup(t *testing.T) (*pgxpool.Pool, fmt.Stringer) {
	t.Helper()

	ts, err := testserver.NewTestServer()
	require.NoError(t, err)
	t.Cleanup(ts.Stop)

	pgURL := ts.PGURL()
	require.NotNil(t, pgURL)

	pool, err := pgxpool.New(context.Background(), pgURL.String())
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	return pool, ident{fmt.Sprintf("test_leases_%d", time.Now().UnixNano())}
}

func TestLeases(t *testing.T) {
	r := require.New(t)

	pool, table := testSetup(t)
	ctx := context.Background()

	r.NoError(Init(ctx, pool, table, ident{"root"}))
	intf, err := New(ctx, Config{
		Conn:  pool,
		Table: table,
	})
	r.NoError(err)
	l := intf.(*holder)

	now := time.Now().UTC()

	t.Run("tryAcquire", func(t *testing.T) {
		a := assert.New(t)
		names := []string{t.Name()}

		// No present state, this should succeed.
		var initial leaseRow
		var ok bool
		err := crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			initial, ok, txErr = l.tryAcquire(ctx, tx, names, now)
			return txErr
		})
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Acquiring at the same time should not do anything.
		var blocked leaseRow
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			blocked, ok, txErr = l.tryAcquire(ctx, tx, names, now)
			return txErr
		})
		a.NoError(err)
		a.False(ok)
		a.Equal(initial.expires, blocked.expires)

		// Acquire within the validity period should be a no-op.
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			blocked, ok, txErr = l.tryAcquire(ctx, tx, names, now.Add(l.cfg.Lifetime/2))
			return txErr
		})
		a.NoError(err)
		a.False(ok)
		a.Equal(initial.expires, blocked.expires)

		// Acquire at the expiration time should succeed.
		var next leaseRow
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			next, ok, txErr = l.tryAcquire(ctx, tx, names, now.Add(l.cfg.Lifetime))
			return txErr
		})
		a.NoError(err)
		if a.True(ok) {
			a.Equal(initial.names, next.names)
			a.NotEqual(initial.expires, next.expires)
			a.NotEqual(initial.nonce, next.nonce)
		}

		// Acquire within the extended lifetime should be a no-op.
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			blocked, ok, txErr = l.tryAcquire(ctx, tx, names, now.Add(2*l.cfg.Lifetime/3))
			return txErr
		})
		a.NoError(err)
		a.False(ok)
		a.Equal(next.expires, blocked.expires)
	})

	t.Run("tryAcquireSkew", func(t *testing.T) {
		a := assert.New(t)
		namesA := []string{t.Name(), "A"}
		namesB := []string{t.Name(), "B"}

		// No present state, this should succeed.
		var initial leaseRow
		var ok bool
		err := crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			initial, ok, txErr = l.tryAcquire(ctx, tx, namesA, now)
			return txErr
		})
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Acquiring at the same time should not do anything.
		var blocked leaseRow
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			blocked, ok, txErr = l.tryAcquire(ctx, tx, namesA, now)
			return txErr
		})
		a.NoError(err)
		a.False(ok)
		a.Equal(initial.expires, blocked.expires)

		// Ensure all-or-nothing behavior with multiple names.
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			blocked, ok, txErr = l.tryAcquire(ctx, tx, namesB, now)
			return txErr
		})
		a.NoError(err)
		a.False(ok)
		a.Equal(initial.expires, blocked.expires)
	})

	t.Run("tryRelease", func(t *testing.T) {
		a := assert.New(t)
		names := []string{t.Name(), "foo"}

		var initial leaseRow
		var ok bool
		err := crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			initial, ok, txErr = l.tryAcquire(ctx, tx, names, now)
			return txErr
		})
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Verify that we can't release with a mis-matched nonce.
		l2 := initial
		l2.nonce = uuid.Must(uuid.NewRandom())
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			ok, txErr = l.tryRelease(ctx, tx, l2)
			return txErr
		})
		a.NoError(err)
		a.False(ok)

		// Initial release should succeed.
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			ok, txErr = l.tryRelease(ctx, tx, initial)
			return txErr
		})
		a.NoError(err)
		a.True(ok)

		// Duplicate release is a no-op.
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			ok, txErr = l.tryRelease(ctx, tx, initial)
			return txErr
		})
		a.NoError(err)
		a.False(ok)
	})

	t.Run("tryRenew", func(t *testing.T) {
		a := assert.New(t)
		names := []string{t.Name()}

		var initial leaseRow
		var ok bool
		err := crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			initial, ok, txErr = l.tryAcquire(ctx, tx, names, now)
			return txErr
		})
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Extend by a second.
		var renewed leaseRow
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			renewed, ok, txErr = l.tryRenew(ctx, tx, initial, now.Add(time.Second))
			return txErr
		})
		a.NoError(err)
		a.True(ok)
		a.Equal(now.Add(time.Second+l.cfg.Lifetime), renewed.expires)
		a.Equal(initial.names, renewed.names)
		a.Equal(initial.nonce, renewed.nonce)

		// Ensure that we can't cross-renew.
		mismatched := renewed
		mismatched.nonce = uuid.Must(uuid.NewRandom())
		err = crdbpgx.ExecuteTx(ctx, pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
			var txErr error
			_, ok, txErr = l.tryRenew(ctx, tx, mismatched, now.Add(2*time.Second))
			return txErr
		})
		a.NoError(err)
		a.False(ok)
	})

	t.Run("waitToAcquire", func(t *testing.T) {
		a := assert.New(t)
		names := []string{t.Name()}

		// Increase polling rate for this test.
		l := l.copy()
		l.cfg.Poll = 10 * time.Millisecond

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		initial, ok, err := l.acquire(ctx, names)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		eg, egCtx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			acquired, ok := l.waitToAcquire(egCtx, initial.names)
			a.True(ok)
			a.NotZero(acquired)
			return nil
		})
		eg.Go(func() error {
			time.Sleep(l.cfg.Poll)
			ok, err := l.release(egCtx, initial)
			a.True(ok)
			a.NoError(err)
			return err
		})

		a.NoError(eg.Wait())
		// Make sure the context has not timed out.
		a.Nil(ctx.Err())
	})

	t.Run("keepRenewed", func(t *testing.T) {
		a := assert.New(t)
		names := []string{t.Name(), "more"}

		initial, ok, err := l.acquire(ctx, names)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Time remaining on lease.
		renewed, retry := l.keepRenewedOnce(ctx, initial, initial.expires.Add(-time.Millisecond))
		a.True(retry)
		a.Greater(renewed.expires, initial.expires)

		// Context canceled.
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, retry = l.keepRenewedOnce(canceled, initial, initial.expires.Add(-time.Millisecond))
		a.False(retry)

		// Already expired.
		_, retry = l.keepRenewedOnce(ctx, initial, initial.expires.Add(time.Millisecond))
		a.False(retry)
	})

	// Verify that keepRenewed will return if any lease row in the
	// database is deleted from underneath.
	t.Run("keepRenewedExitsIfHijacked", func(t *testing.T) {
		a := assert.New(t)
		names := []string{t.Name(), "kaboom"}

		// Increase polling rate.
		l := l.copy()
		l.cfg.Lifetime = 100 * time.Millisecond
		l.cfg.Poll = 5 * time.Millisecond

		initial, ok, err := l.acquire(ctx, names)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		eg, egCtx := errgroup.WithContext(ctx)
		// Start a goroutine to keep the lease renewed, and a
		// second one to cause it to be released.
		eg.Go(func() error {
			l.keepRenewed(egCtx, initial)
			return nil
		})
		// Release only one of the names.
		eg.Go(func() error {
			time.Sleep(l.cfg.Poll)
			leaseCopy := initial
			leaseCopy.names = names[1:]
			ok, err := l.release(egCtx, leaseCopy)
			a.True(ok)
			a.NoError(err)
			return err
		})

		a.NoError(eg.Wait())
		// Make sure the context has not timed out.
		a.Nil(ctx.Err())
	})

	t.Run("singleton", func(t *testing.T) {
		a := assert.New(t)

		// Ensure that cancel and cleanup are working; the lease
		// lifetime will be longer than that of the test.
		l := l.copy()
		l.cfg.Lifetime = time.Hour
		l.cfg.Poll = 5 * time.Millisecond

		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		eg, egCtx := errgroup.WithContext(ctx)
		var running atomic.Bool
		for i := 0; i < 10; i++ {
			eg.Go(func() error {
				l.Singleton(egCtx, []string{t.Name()}, func(ctx context.Context) error {
					a.NoError(ctx.Err())
					if a.True(running.CompareAndSwap(false, true)) {
						time.Sleep(3 * l.cfg.Poll)
						running.Store(false)
					}
					return ErrCancelSingleton
				})
				return nil
			})
		}

		a.NoError(eg.Wait())
		// Make sure the context has not timed out.
		a.NoError(ctx.Err())
	})

	t.Run("lease_facade", func(t *testing.T) {
		a := assert.New(t)

		// Initial acquisition.
		facade, err := l.Acquire(ctx, t.Name())
		a.NoError(err)
		found, ok := facade.Context().Value(leaseKey{}).(Lease)
		a.True(ok)
		a.Same(facade, found)

		// Verify that a duplicate fails.
		_, err = l.Acquire(ctx, t.Name())
		if busy, ok := IsBusy(err); a.True(ok) {
			a.NotZero(busy.Expiration)
		}

		// Verify that releasing cancels the lease.
		a.Nil(facade.Context().Err())
		facade.Release()

		a.ErrorIs(facade.Context().Err(), context.Canceled)

		// Re-acquisition should succeed.
		_, err = l.Acquire(ctx, t.Name())
		a.NoError(err)
	})
}

func TestSanitize(t *testing.T) {
	a := assert.New(t)

	cfg := Config{}
	a.EqualError(cfg.sanitize(), "pool must not be nil")

	cfg.Conn = &pgxpool.Pool{}
	a.EqualError(cfg.sanitize(), "table must be set")

	cfg.Table = ident{name: "test_leases"}
	a.NoError(cfg.sanitize())

	a.Zero(cfg.Guard)
	a.Equal(defaultLifetime, cfg.Lifetime)
	a.Equal(defaultPoll, cfg.Poll)
	a.Equal(defaultRetry, cfg.RetryDelay)
}

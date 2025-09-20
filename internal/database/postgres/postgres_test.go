// Copyright Kristopher Rahim Afful-Brown 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/adoublef/ack/internal/database/postgres"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"go.adoublef.dev/runtime/container/postgres"
	"go.adoublef.dev/testing/is"
)

func TestUp(t *testing.T) {
	ctx := t.Context()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	is.OK(t, err) // container.ConnectionString

	is.OK(t, Up(ctx, dsn))   // Up
	is.OK(t, Down(ctx, dsn)) // Down
}

func TestMaxConn(t *testing.T) {
	sleep := func(ctx context.Context, d *sqlx.DB, timeout int) error {
		_, err := d.ExecContext(ctx, "select pg_sleep($1)", timeout)
		if err != nil {
			return err
		}
		return nil
	}

	trace := func(t testing.TB, start time.Time) {
		t.Helper()
		elapsed := time.Since(start)
		t.Logf("%s (%s)", t.Name(), elapsed)
	}

	t.Run("One", func(t *testing.T) {
		var (
			d = newPool(t, 1)
		)

		numOfQueries := 4

		var wg sync.WaitGroup
		wg.Add(numOfQueries)
		for range numOfQueries {
			go func() {
				defer func(start time.Time) { wg.Done(); trace(t, start) }(time.Now())
				sleep(t.Context(), d, 1)
			}()
		}
		wg.Wait()
	})

	t.Run("Many", func(t *testing.T) {
		var (
			d  = newPool(t, 1)
			d1 = newPool(t, 1)
			d2 = newPool(t, 1)
			d3 = newPool(t, 1)

			dd = []*sqlx.DB{d, d1, d2, d3}
		)

		numOfQueries := len(dd)

		var wg sync.WaitGroup
		wg.Add(numOfQueries)
		for _, d := range dd {
			go func() {
				defer func(start time.Time) { wg.Done(); trace(t, start) }(time.Now())
				sleep(t.Context(), d, 1)
			}()
		}
		wg.Wait()
	})

	t.Run("Pool", func(t *testing.T) {
		var (
			d = newPool(t, 4)
		)

		numOfQueries := 4

		var wg sync.WaitGroup
		wg.Add(numOfQueries)
		for range numOfQueries {
			go func() {
				defer func(start time.Time) { wg.Done(); trace(t, start) }(time.Now())
				sleep(t.Context(), d, 1)
			}()
		}
		wg.Wait()
	})
}

func newPool(t testing.TB, maxConns int) *sqlx.DB {
	t.Helper()
	ctx := t.Context()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	is.OK(t, err) // postgresContainer.ConnectionString

	db, err := sql.Open("postgres", dsn)
	is.OK(t, err) // sql.Open
	t.Cleanup(func() { db.Close() })
	if maxConns > 0 {
		// avoid making and closing lots of connections, set the maximum idle size
		// db.SetMaxIdleConns(maxConns)
		// If n <= 0, then there is no limit on the number of open connections.
		db.SetMaxOpenConns(maxConns)
	}
	t.Logf("%d = db.Stats().MaxOpenConnections", db.Stats().MaxOpenConnections)

	return sqlx.NewDb(db, "postgres")
}

func TestMain(m *testing.M) {
	err := setup(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	err = cleanup(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}

var container *postgres.Container

// setup initialises containers within the pacakge.
func setup(ctx context.Context) (err error) {
	container, err = postgres.Run(ctx, "")
	if err != nil {
		return
	}
	return
}

// cleanup stops all running containers for the pacakge.
func cleanup(ctx context.Context) (err error) {
	var cc = []testcontainers.Container{container}
	for _, c := range cc {
		if c != nil {
			err = errors.Join(err, c.Terminate(ctx))
		}
	}
	return err
}

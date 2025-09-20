// Copyright Kristopher Rahim Afful-Brown 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	"maragu.dev/migrate"
)

//go:embed all:*.sql
var embedFS embed.FS

func Up(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %w", err)
	}
	defer db.Close()

	if err := migrate.Up(ctx, db, embedFS); err != nil {
		return fmt.Errorf("failed to run up migration: %w", err)
	}
	return nil
}

func Down(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %w", err)
	}
	defer db.Close()

	if err := migrate.Down(ctx, db, embedFS); err != nil {
		return fmt.Errorf("failed to run down migration: %w", err)
	}
	return nil
}

// Copyright Kristopher Rahim Afful-Brown 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package item

import (
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

type DB struct {
	RWC *sqlx.DB
}

type Item struct {
	ID   uuid.UUID `db:"id"`
	Meta Meta      `db:"metadata"`
}

func (d *DB) Item(ctx context.Context, id uuid.UUID) (Item, error) {
	var found Item
	err := d.RWC.QueryRowxContext(ctx, "select id, metadata from ack.item where id = $1", id).StructScan(&found)
	return found, err
}

type Meta struct {
	Data     []byte    `json:"data,omitempty"`
	LastSeen time.Time `json:"lastSeen,omitzero"`
}

func (m *Meta) Scan(src any) error {
	if src == nil {
		*m = Meta{}
		return nil
	}

	var data []byte
	switch v := src.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return errors.New("unsupported type for Meta Scan")
	}
	return json.Unmarshal(data, m)
}

func (m Meta) Value() (driver.Value, error) {
	return json.Marshal(m)
}

func (d *DB) Add(ctx context.Context, m Meta) (uuid.UUID, error) {
	id := uuid.Must(uuid.NewV7())
	arg := &struct {
		ID   uuid.UUID `db:"id"`
		Meta Meta      `db:"metadata"`
	}{
		ID:   id,
		Meta: m,
	}
	_, err := d.RWC.NamedExecContext(ctx, "insert into ack.item (id, metadata) values (:id, :metadata)", arg)
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func (d *DB) Mod(ctx context.Context, id uuid.UUID, m Meta) error {
	arg := &struct {
		ID   uuid.UUID `db:"id"`
		Meta Meta      `db:"metadata"`
	}{
		ID:   id,
		Meta: m,
	}
	ct, err := d.RWC.NamedExecContext(ctx, "update ack.item set metadata = :metadata where id = :id", arg)
	if err != nil {
		return err
	} else if n, err := ct.RowsAffected(); err != nil || n < 1 {
		return cmp.Or(err, sql.ErrNoRows)
	}
	return nil
}

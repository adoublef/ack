// Copyright Kristopher Rahim Afful-Brown 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package item_test

import (
	"testing"
	"time"

	. "github.com/adoublef/ack/internal/item"
	"go.adoublef.dev/testing/is"
)

func Test_DB(t *testing.T) {
	var (
		db = &DB{RWC: newDB(t, 1)}
	)

	m := Meta{
		LastSeen: time.Date(2009, time.November, 10, 0, 0, 0, 0, time.UTC),
	}
	id, err := db.Add(t.Context(), m)
	is.OK(t, err) // DB.AddDevice

	m.LastSeen = m.LastSeen.AddDate(0, 0, 1)
	err = db.Mod(t.Context(), id, m)
	is.OK(t, err) // DB.ModDevice

	dev, err := db.Item(t.Context(), id)
	is.OK(t, err) // DB.Device

	is.Equal(t, dev.Meta.LastSeen, m.LastSeen) // +1 day
}

// Copyright Kristopher Rahim Afful-Brown 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/adoublef/ack/internal/item"
	"github.com/google/uuid"
	"go.adoublef.dev/testing/is"
)

func TestHandle_handleItem(t *testing.T) {
	ctx := t.Context()

	p := newPool(t, 1)
	db := &item.DB{RWC: p}

	s := newHTTP(t, db)

	// create a new item
	resp, err := addItem(ctx, s.Client(), s.URL, item.Meta{})
	is.OK(t, err)
	is.Equal(t, resp.StatusCode, http.StatusCreated)

	var created struct {
		ID uuid.UUID `json:"id"`
	}
	err = json.NewDecoder(resp.Body).Decode(&created)
	is.OK(t, err)
	is.OK(t, resp.Body.Close()) // close after use

	// modify item
	resp, err = modItem(ctx, s.Client(), s.URL, created.ID, []byte("hello, world\n"))
	is.OK(t, err)
	is.Equal(t, resp.StatusCode, http.StatusNoContent)

	is.OK(t, resp.Body.Close()) // close after use

	// get the item
	resp, err = getItem(ctx, s.Client(), s.URL, created.ID)
	is.OK(t, err)
	is.Equal(t, resp.StatusCode, http.StatusOK)

	var found struct {
		ID   uuid.UUID `json:"id"`
		Data []byte    `json:"data"`
	}
	err = json.NewDecoder(resp.Body).Decode(&found)
	is.OK(t, err)
	is.OK(t, resp.Body.Close()) // close after use

	is.Equal(t, found.ID, created.ID)
	is.Equal(t, string(found.Data), "hello, world\n")
}

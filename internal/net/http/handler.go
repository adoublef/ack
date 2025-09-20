// Copyright Kristopher Rahim Afful-Brown 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http

import (
	"cmp"
	"encoding/json"
	"net/http"
	"time"

	"github.com/adoublef/ack/internal/item"
	"github.com/google/uuid"
)

func Handler(db *item.DB) http.Handler {
	mux := http.NewServeMux()
	handleFunc := func(pattern string, h http.Handler) {
		mux.Handle(pattern, h)
	}
	handleFunc("GET /items/{item}", handleItem(db))
	handleFunc("POST /items", handleAddItem(db))
	handleFunc("PATCH /items/{item}", handleModItem(db))

	return mux
}

func handleItem(db *item.DB) http.HandlerFunc {
	parse := func(_ http.ResponseWriter, r *http.Request) (uuid.UUID, error) {
		return uuid.Parse(r.PathValue("item"))
	}

	type response struct {
		ID       uuid.UUID `json:"id"`
		Data     []byte    `json:"data,omitempty"`
		LastSeen time.Time `json:"lastSeen,omitzero"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		id, err := parse(w, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		it, err := db.Item(r.Context(), id)
		if err != nil {
			w.WriteHeader(http.StatusFailedDependency)
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response{it.ID, it.Meta.Data, it.Meta.LastSeen})
	}
}

func handleAddItem(db *item.DB) http.HandlerFunc {
	type request struct {
		Data []byte `json:"data"`
	}

	parse := func(_ http.ResponseWriter, r *http.Request) ([]byte, error) {
		var v request
		err := json.NewDecoder(r.Body).Decode(&v)
		return v.Data, err
	}

	type response struct {
		ID uuid.UUID `json:"id"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := parse(w, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		id, err := db.Add(r.Context(), item.Meta{Data: data})
		if err != nil {
			w.WriteHeader(http.StatusFailedDependency)
			return
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(response{id})
	}
}

func handleModItem(db *item.DB) http.HandlerFunc {
	type request struct {
		Data []byte `json:"data"`
	}

	parse := func(_ http.ResponseWriter, r *http.Request) (uuid.UUID, []byte, error) {
		id, err1 := uuid.Parse(r.PathValue("item"))
		var v request
		err2 := json.NewDecoder(r.Body).Decode(&v)
		return id, v.Data, cmp.Or(err1, err2)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		id, data, err := parse(w, r)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = db.Mod(r.Context(), id, item.Meta{Data: data})
		if err != nil {
			w.WriteHeader(http.StatusFailedDependency)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

// Copyright Kristopher Rahim Afful-Brown 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package nats

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"

	"github.com/adoublef/ack/internal/item"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

func Consume(nc *nats.Conn, db *item.DB, countA, countB int) error {
	// note: close existing subscribers if an error occured
	// easy cleanup opportunity

	handleMsg := func(h MsgHandler) nats.MsgHandler {
		return func(msg *nats.Msg) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h(ctx, msg)
		}
	}

	for range countA {
		// we need to use a queue as spinning up many subscibers
		// will duplicate processing of a single message.
		_, err := nc.QueueSubscribe("a", "queue", handleMsg(lowPriorityHighFrequency(db)))
		if err != nil {
			return err
		}
	}

	for range countB {
		_, err := nc.QueueSubscribe("b", "queue", handleMsg(highPriorityLowFrequency(db)))
		if err != nil {
			return err
		}
	}
	return nil
}

// low priority + high frequency
func lowPriorityHighFrequency(db *item.DB) MsgHandler {
	return func(ctx context.Context, msg *nats.Msg) {
		var v struct {
			ID uuid.UUID `json:"id"`
		}
		err1 := json.Unmarshal(msg.Data, &v)
		it, err2 := db.Item(ctx, v.ID)
		err3 := db.Mod(ctx, it.ID, it.Meta)
		if err := cmp.Or(err1, err2, err3); err != nil {
			panicf("failed to process lowPriorityHighFrequency: %v", err)
		}
	}
}

// high priority + low frequency
func highPriorityLowFrequency(db *item.DB) MsgHandler {
	return func(ctx context.Context, msg *nats.Msg) {
		var v struct {
			ID uuid.UUID `json:"id"`
		}
		err1 := json.Unmarshal(msg.Data, &v)
		it, err2 := db.Item(ctx, v.ID)
		err3 := db.Mod(ctx, it.ID, it.Meta)
		if err := cmp.Or(err1, err2, err3); err != nil {
			panicf("failed to process highPriorityLowFrequency: %v", err)
		}
	}
}

type MsgHandler func(ctx context.Context, msg *nats.Msg)

type Conn = nats.Conn

func Open(url string) (*Conn, error) {
	return nats.Connect(url)
}

func panicf(format string, v ...any) { panic(fmt.Sprintf(format, v...)) }

// Copyright Kristopher Rahim Afful-Brown 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package nats

import (
	"cmp"
	"context"
	"log"

	"github.com/adoublef/ack/internal/item"
	"github.com/nats-io/nats.go"
)

func Consume(nc *nats.Conn, db *item.DB) error {
	// note: close existing subscribers if an error occured
	// easy cleanup opportunity

	handleMsg := func(h MsgHandler) nats.MsgHandler {
		return func(msg *nats.Msg) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h(ctx, msg)
		}
	}

	// we need to use a queue as spinning up many subscibers
	// will duplicate processing of a single message.
	_, err1 := nc.QueueSubscribe("a", "queue", handleMsg(lowPriorityHighFrequency(db)))
	_, err2 := nc.QueueSubscribe("b", "queue", handleMsg(highPriorityLowFrequency(db)))
	if err := cmp.Or(err1, err2); err != nil {
		return err
	}
	return nil
}

// low priority + high frequency
func lowPriorityHighFrequency(db *item.DB) MsgHandler {
	return func(_ context.Context, msg *nats.Msg) { log.Printf("subject=%q", msg.Subject) }
}

// high priority + low frequency
func highPriorityLowFrequency(db *item.DB) MsgHandler {
	return func(_ context.Context, msg *nats.Msg) { log.Printf("subject=%q", msg.Subject) }
}

type MsgHandler func(ctx context.Context, msg *nats.Msg)

type Conn = nats.Conn

func Open(url string) (*Conn, error) {
	return nats.Connect(url)
}

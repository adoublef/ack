// Copyright Kristopher Rahim Afful-Brown 2025. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package nats_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/adoublef/ack/internal/item"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.adoublef.dev/testing/is"
	"go.adoublef.dev/testing/wait"
	"golang.org/x/sync/errgroup"
)

func TestConsume(t *testing.T) {
	ctx := t.Context()

	db := &item.DB{RWC: newPool(t, 1)}

	s := newHTTP(t, db)
	nc := newNATS(t, db)
	// todo: mqtt for the remote items

	g, ctx := errgroup.WithContext(ctx)

	// create [item.Item] via http api
	// and return its id
	ids := ids(ctx, g, s, 1, 1)
	// create message to be sent to the consumer
	ids, msgs := msgs(ctx, g, ids, 1, 1) // shadow as we need to proxy the ids to the poll stage
	// send messages to the consumer that will
	// modify an [item.Item]
	send(ctx, g, nc, msgs, 1)
	// poll the http api to assert the changes
	// have been made on the [item.Item]
	poll(ctx, g, s, ids, 1)

	is.OK(t, g.Wait()) // OK
}

func poll(ctx context.Context, g *errgroup.Group, s *httptest.Server, ids <-chan string, parallel int) {
	g.Go(func() error {
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(parallel)

		timeout := time.Second * 15

		url := s.URL + "/items/"
		c := s.Client()

		for id := range ids {
			g.Go(func() error {
				f := func() error {
					req, err1 := http.NewRequestWithContext(ctx, http.MethodGet, url+id, nil)
					resp, err2 := c.Do(req)
					if err := cmp.Or(err1, err2); err != nil {
						return err
					}
					defer resp.Body.Close()
					// if not found then we just skip
					if resp.StatusCode != http.StatusOK {
						return fmt.Errorf("failed to fetch item: %s: %w", http.StatusText(resp.StatusCode), wait.SkipRetry)
					}

					var item struct {
						ID uuid.UUID `json:"id"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
						return err
					}
					return nil
				}
				return wait.ForFunc(ctx, timeout, f)
			})
		}

		return g.Wait()
	})
}

func send(ctx context.Context, g *errgroup.Group, nc *nats.Conn, msgs <-chan nats.Msg, parallel int) {
	g.Go(func() error {
		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(parallel)

		for msg := range msgs {
			g.Go(func() error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return nc.PublishMsg(&msg)
			})
		}

		return g.Wait()
	})
}

func msgs(ctx context.Context, g *errgroup.Group, ids <-chan string, count, parallel int) (<-chan string, <-chan nats.Msg) {
	ch := make(chan string, parallel)
	msgs := make(chan nats.Msg, parallel)
	g.Go(func() error {
		defer func() { close(ch); close(msgs) }()

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(parallel)

		for id := range ids {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- id: // proxy
			}
			g.Go(func() error {
				// create a message to be processed in another pipeline stage
				// lets create more messages soon
				for range count {
					v := &struct {
						ID string `json:"id"`
					}{
						ID: id,
					}
					p, err := json.Marshal(v)
					if err != nil {
						return err
					}
					msg := nats.Msg{
						Subject: "a", /// high frequency + low priority
						Data:    p,
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case msgs <- msg:
					}
				}
				// ready to poll?
				return nil
			})
		}
		return g.Wait()
	})
	return ch, msgs
}

func ids(ctx context.Context, g *errgroup.Group, s *httptest.Server, count, parallel int) <-chan string {
	ch := make(chan string, parallel)
	g.Go(func() error {
		defer close(ch)

		url := s.URL + "/items"
		c := s.Client()

		g, ctx := errgroup.WithContext(ctx)
		g.SetLimit(parallel)

		for range count {
			g.Go(func() error {
				p, err1 := json.Marshal(&struct {
					Data []byte `json:"data"`
				}{Data: nil}) // todo: scale
				req, err2 := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(p))
				resp, err3 := c.Do(req)
				if err := cmp.Or(err1, err2, err3); err != nil {
					return err
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusCreated {
					return fmt.Errorf("failed to create item: %s", http.StatusText(resp.StatusCode))
				}

				var created struct {
					ID string `json:"id"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
					return err
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case ch <- created.ID:
				}
				return nil
			})
		}

		return g.Wait()
	})
	return ch
}

// https://ornlu-is.github.io/go_tee_channel_pattern/
func tee[V any](ch <-chan V) (<-chan V, <-chan V) {
	c1 := make(chan V) // buffer?
	c2 := make(chan V) // buffer?

	go func() {
		defer func() {
			close(c1)
			close(c2)
		}()

		for val := range ch {
			for i := 0; i < 2; i++ {
				var c1, c2 = c1, c2
				select {
				case c1 <- val:
					c1 = nil
				case c2 <- val:
					c2 = nil
				}
			}
		}
	}()

	return c1, c2
}

// https://go.dev/blog/pipelines#fan-out-fan-in
func merge[V any](cs ...<-chan V) <-chan V {
	var wg sync.WaitGroup
	out := make(chan V)
	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan V) {
		defer wg.Done()
		for n := range c {
			out <- n
		}
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}
	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// Unit tests for InMemoryQueueBroker: spec FIFO/timeout behavior, message-loss
// regressions, requeue semantics, and concurrent stress checks.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestBroker() *InMemoryQueueBroker {
	return NewInMemoryQueueBroker()
}

// ---- spec ----

func TestBroker_FIFO(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	if err := b.Enqueue("pet", "cat"); err != nil {
		t.Fatalf("enqueue cat: %v", err)
	}
	if err := b.Enqueue("pet", "dog"); err != nil {
		t.Fatalf("enqueue dog: %v", err)
	}

	got, err := b.Dequeue(context.Background(), "pet", 0)
	if err != nil || got != "cat" {
		t.Fatalf("first dequeue: got %q, err %v", got, err)
	}
	got, err = b.Dequeue(context.Background(), "pet", 0)
	if err != nil || got != "dog" {
		t.Fatalf("second dequeue: got %q, err %v", got, err)
	}
}

// Two blocked consumers must be served in the order they started waiting.
func TestBroker_TimeoutWaitersFIFO(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	results := make([]string, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	startWaiter := func(idx int) {
		go func() {
			defer wg.Done()
			v, err := b.Dequeue(context.Background(), "pet", 2*time.Second)
			results[idx] = v
			errs[idx] = err
		}()
	}

	startWaiter(0)
	time.Sleep(50 * time.Millisecond)
	startWaiter(1)
	time.Sleep(50 * time.Millisecond)

	if err := b.Enqueue("pet", "cat"); err != nil {
		t.Fatalf("enqueue cat: %v", err)
	}
	if err := b.Enqueue("pet", "dog"); err != nil {
		t.Fatalf("enqueue dog: %v", err)
	}
	wg.Wait()

	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("unexpected errors: %v %v", errs[0], errs[1])
	}
	if results[0] != "cat" || results[1] != "dog" {
		t.Fatalf("unexpected order: %v", results)
	}
}

func TestBroker_TimeoutOnEmptyQueue(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	_, err := b.Dequeue(context.Background(), "pet", 100*time.Millisecond)
	if !errors.Is(err, ErrQueueEmpty) {
		t.Fatalf("want ErrQueueEmpty, got %v", err)
	}
}

func TestBroker_TwoQueuesIsolated(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = b.Enqueue("pet", "cat")
		_ = b.Enqueue("pet", "dog")
		got, err := b.Dequeue(context.Background(), "pet", 0)
		if err != nil || got != "cat" {
			t.Errorf("pet first: %q, %v", got, err)
		}
		got, err = b.Dequeue(context.Background(), "pet", 0)
		if err != nil || got != "dog" {
			t.Errorf("pet second: %q, %v", got, err)
		}
	}()

	go func() {
		defer wg.Done()
		_ = b.Enqueue("role", "manager")
		_ = b.Enqueue("role", "executive")
		got, err := b.Dequeue(context.Background(), "role", 0)
		if err != nil || got != "manager" {
			t.Errorf("role first: %q, %v", got, err)
		}
		got, err = b.Dequeue(context.Background(), "role", 0)
		if err != nil || got != "executive" {
			t.Errorf("role second: %q, %v", got, err)
		}
	}()

	wg.Wait()
}

// ---- message-loss regressions ----

func TestBroker_TimeoutThenEnqueueKeepsMessage(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	done := make(chan error, 1)

	go func() {
		_, err := b.Dequeue(context.Background(), "pet", 50*time.Millisecond)
		done <- err
	}()

	time.Sleep(80 * time.Millisecond)
	if err := b.Enqueue("pet", "late"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueEmpty) {
			t.Fatalf("want ErrQueueEmpty, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dequeue")
	}

	got, err := b.Dequeue(context.Background(), "pet", 0)
	if err != nil || got != "late" {
		t.Fatalf("want late, got %q, err %v", got, err)
	}
}

func TestBroker_EnqueueDuringTimeoutDelivers(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	type result struct {
		value string
		err   error
	}
	done := make(chan result, 1)

	go func() {
		v, err := b.Dequeue(context.Background(), "pet", 200*time.Millisecond)
		done <- result{v, err}
	}()

	time.Sleep(150 * time.Millisecond)
	if err := b.Enqueue("pet", "msg"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case got := <-done:
		if got.err != nil || got.value != "msg" {
			t.Fatalf("want msg, got %q, err %v", got.value, got.err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for delivery")
	}
}

// A canceled waiter must not consume a message meant for the next consumer.
func TestBroker_CanceledWaiterSkipsDelivery(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		close(ready)
		_, err := b.Dequeue(ctx, "pet", time.Second)
		done <- err
	}()

	<-ready
	time.Sleep(10 * time.Millisecond)
	cancel()

	if err := b.Enqueue("pet", "survivor"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancel")
	}

	got, err := b.Dequeue(context.Background(), "pet", 0)
	if err != nil || got != "survivor" {
		t.Fatalf("want survivor, got %q, err %v", got, err)
	}
}

func TestBroker_NoMessageLossUnderConcurrentTimeout(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	const total = 100

	var puts atomic.Int32
	var gets atomic.Int32
	var wg sync.WaitGroup

	for i := range total {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msg := fmt.Sprintf("m%d", id)

			if id%2 == 0 {
				go func() {
					if _, err := b.Dequeue(context.Background(), "q", 30*time.Millisecond); err == nil {
						gets.Add(1)
					}
				}()
				time.Sleep(time.Duration(id%7) * time.Millisecond)
			}

			if err := b.Enqueue("q", msg); err != nil {
				t.Errorf("enqueue %s: %v", msg, err)
				return
			}
			puts.Add(1)
		}(i)
	}
	wg.Wait()

	for {
		_, err := b.Dequeue(context.Background(), "q", 0)
		if errors.Is(err, ErrQueueEmpty) {
			break
		}
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		gets.Add(1)
	}

	if int(puts.Load()) != total {
		t.Fatalf("puts: want %d, got %d", total, puts.Load())
	}
	if int(gets.Load()) != total {
		t.Fatalf("delivered: want %d, got %d", total, gets.Load())
	}
}

func TestBroker_NoMessageLossUnderLoad(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	const producers = 500
	const consumers = 1000

	var sent atomic.Int32
	var received atomic.Int32
	var wg sync.WaitGroup

	for range consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			value, err := b.Dequeue(ctx, "q", 10*time.Millisecond)
			if err == nil && value != "" {
				received.Add(1)
			}
		}()
	}

	for i := range producers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(5 * time.Millisecond)
			if err := b.Enqueue("q", fmt.Sprintf("msg-%d", id)); err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			sent.Add(1)
		}(i)
	}
	wg.Wait()

	var remaining int32
	for {
		_, err := b.Dequeue(context.Background(), "q", 0)
		if errors.Is(err, ErrQueueEmpty) {
			break
		}
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		remaining++
	}

	if total := received.Load() + remaining; total != sent.Load() {
		t.Fatalf("sent %d, accounted %d (received %d, remaining %d)",
			sent.Load(), total, received.Load(), remaining)
	}
}

// ---- requeue ----

func TestBroker_RequeueToFront(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	if err := b.Enqueue("pet", "dog"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	b.Requeue("pet", "cat")

	got, err := b.Dequeue(context.Background(), "pet", 0)
	if err != nil || got != "cat" {
		t.Fatalf("first: want cat, got %q, err %v", got, err)
	}
	got, err = b.Dequeue(context.Background(), "pet", 0)
	if err != nil || got != "dog" {
		t.Fatalf("second: want dog, got %q, err %v", got, err)
	}
}

func TestBroker_RequeueToActiveWaiter(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	done := make(chan string, 1)

	go func() {
		got, err := b.Dequeue(context.Background(), "pet", time.Second)
		if err != nil {
			t.Errorf("dequeue: %v", err)
			return
		}
		done <- got
	}()

	time.Sleep(10 * time.Millisecond)
	b.Requeue("pet", "cat")

	select {
	case got := <-done:
		if got != "cat" {
			t.Fatalf("want cat, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for requeued delivery")
	}
}

// ---- context and validation ----

func TestBroker_CanceledContextBeforeWait(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := b.Dequeue(ctx, "pet", time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestBroker_CanceledContextMidWait(t *testing.T) {
	t.Parallel()

	b := newTestBroker()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		close(ready)
		_, err := b.Dequeue(ctx, "pet", time.Second)
		done <- err
	}()

	<-ready
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancel")
	}
}

func TestBroker_EnqueueEmptyQueueName(t *testing.T) {
	t.Parallel()

	err := newTestBroker().Enqueue("", "msg")
	if !errors.Is(err, ErrQueueNameAbsent) {
		t.Fatalf("want ErrQueueNameAbsent, got %v", err)
	}
}

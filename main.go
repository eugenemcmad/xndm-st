// Queue broker: an HTTP service that provides named FIFO queues.
//
// PUT /{queue}?v=<message> - enqueue a message; 400 if v is absent.
// GET /{queue}             - dequeue a message; 404 if queue is empty.
// GET /{queue}?timeout=N   - wait up to N seconds for a message; 404 on timeout.
//
// Usage: queue-broker -port 8080
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrQueueEmpty      = errors.New("queue is empty")
	ErrInvalidTimeout  = errors.New("invalid timeout")
	ErrQueueNameAbsent = errors.New("queue name is empty")
)

func main() {
	var port string
	flag.StringVar(&port, "port", "8080", "http listen port")
	flag.Parse()

	logger := log.Default()
	broker := NewInMemoryQueueBroker()
	handler := NewQueueHandler(broker, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		// Tie request contexts to ctx so a shutdown signal also cancels in-flight
		// long-polls: the GET then wakes, returns 404, and the connection goes
		// idle, letting Shutdown finish gracefully.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Printf("shutdown failed: %v", err)
		}
		close(shutdownDone)
	}()

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("server failed: %v", err)
	}

	<-shutdownDone
}

// waiter represents a GET request blocked on an empty queue.
type waiter struct {
	ctx context.Context
	ch  chan string
}

// queueState holds the messages and the ordered list of waiting consumers
// for a single named queue. Both slices are operated in FIFO order.
type queueState struct {
	messages []string
	waiters  []*waiter
}

// QueueBroker hides queue storage from the HTTP layer.
type QueueBroker interface {
	Enqueue(queueName, value string) error
	Dequeue(ctx context.Context, queueName string, timeout time.Duration) (string, error)
	Requeue(queueName, value string)
}

// InMemoryQueueBroker implements QueueBroker using in-memory maps protected by a mutex.
//
// NOTE (production): queues are created lazily and never removed, so the map
// grows with each distinct queue name — a memory-leak / DoS vector. Prod would
// evict empty queues (no messages, no waiters) on drain or via a periodic
// sweep; omitted here to keep the task minimal.
type InMemoryQueueBroker struct {
	mu     sync.Mutex
	queues map[string]*queueState
}

func NewInMemoryQueueBroker() *InMemoryQueueBroker {
	return &InMemoryQueueBroker{
		queues: make(map[string]*queueState),
	}
}

func (b *InMemoryQueueBroker) Enqueue(queueName, value string) error {
	if queueName == "" {
		return ErrQueueNameAbsent
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	state := b.ensureQueue(queueName)
	if b.deliverToWaiter(state, value) {
		return nil
	}

	state.messages = append(state.messages, value)
	return nil
}

// Requeue returns a message taken from the queue but not delivered to the HTTP
// client. It goes to the front, unless an active waiter can take it immediately.
//
// Note: redelivery can reorder against a concurrent consumer — if another GET
// grabbed the next message meanwhile, that one is seen first. The classic
// redelivery-vs-ordering trade-off; nothing is lost, front-insert stays close
// to FIFO.
func (b *InMemoryQueueBroker) Requeue(queueName, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	state := b.ensureQueue(queueName)
	if b.deliverToWaiter(state, value) {
		return
	}

	state.messages = append([]string{value}, state.messages...)
}

func (b *InMemoryQueueBroker) deliverToWaiter(state *queueState, value string) bool {
	for len(state.waiters) > 0 {
		w := state.waiters[0]
		state.waiters = state.waiters[1:]

		// A canceled long-poll GET must not consume a future PUT. Skipping it
		// prevents the message-loss race that can happen around request timeout.
		if w.ctx.Err() != nil {
			continue
		}

		w.ch <- value
		return true
	}
	return false
}

func (b *InMemoryQueueBroker) Dequeue(ctx context.Context, queueName string, timeout time.Duration) (string, error) {
	if queueName == "" {
		return "", ErrQueueNameAbsent
	}
	if timeout < 0 {
		return "", ErrInvalidTimeout
	}

	b.mu.Lock()
	state := b.ensureQueue(queueName)
	if len(state.messages) > 0 {
		value := state.messages[0]
		state.messages = state.messages[1:]
		b.mu.Unlock()
		return value, nil
	}

	if timeout == 0 {
		b.mu.Unlock()
		return "", ErrQueueEmpty
	}

	// The buffered channel lets PUT finish even if timeout/cancel wins before
	// this goroutine gets scheduled to read the delivered value.
	w := &waiter{ctx: ctx, ch: make(chan string, 1)}
	state.waiters = append(state.waiters, w)
	b.mu.Unlock()

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case value := <-w.ch:
		b.removeWaiter(queueName, w)
		return value, nil
	case <-timeoutCtx.Done():
		return b.finishWait(queueName, w, ctx)
	}
}

// finishWait resolves the race between timeout/cancel and a concurrent PUT.
// If a message was already handed to this waiter, returning it is safer than
// reporting 404 and silently losing it.
//
// Trade-off: on this tight boundary a GET may return 200 a hair after its
// timeout window instead of 404. We prefer that over dropping the message.
func (b *InMemoryQueueBroker) finishWait(queueName string, w *waiter, ctx context.Context) (string, error) {
	if value, ok := tryRecv(w.ch); ok {
		b.removeWaiter(queueName, w)
		return value, nil
	}

	b.removeWaiter(queueName, w)

	// Enqueue may have sent after removeWaiter started; check once more.
	if value, ok := tryRecv(w.ch); ok {
		return value, nil
	}

	if err := ctx.Err(); err != nil {
		return "", context.Cause(ctx)
	}
	return "", ErrQueueEmpty
}

func tryRecv(ch <-chan string) (string, bool) {
	select {
	case value := <-ch:
		return value, true
	default:
		return "", false
	}
}

// removeWaiter removes a specific waiter from the queue's waiting list.
// Called when a waiter's context is canceled or its timer fires.
func (b *InMemoryQueueBroker) removeWaiter(queueName string, target *waiter) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if state, ok := b.queues[queueName]; ok {
		if idx := slices.Index(state.waiters, target); idx != -1 {
			state.waiters = slices.Delete(state.waiters, idx, idx+1)
		}
	}
}

func (b *InMemoryQueueBroker) ensureQueue(queueName string) *queueState {
	state, ok := b.queues[queueName]
	if !ok {
		state = &queueState{}
		b.queues[queueName] = state
	}
	return state
}

// QueueHandler routes HTTP requests to the broker.
// The URL path segment after "/" is treated as the queue name.
type QueueHandler struct {
	broker QueueBroker
	logger *log.Logger
}

func NewQueueHandler(broker QueueBroker, logger *log.Logger) *QueueHandler {
	return &QueueHandler{broker: broker, logger: logger}
}

func (h *QueueHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	queueName := strings.TrimPrefix(r.URL.Path, "/")
	if queueName == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.handlePut(w, r, queueName)
	case http.MethodGet:
		h.handleGet(w, r, queueName)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *QueueHandler) handlePut(w http.ResponseWriter, r *http.Request, queueName string) {
	values, ok := r.URL.Query()["v"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// The spec rejects only a missing "v" parameter. "v=" is still a message,
	// even though it is an empty string.
	value := ""
	if len(values) > 0 {
		value = values[0]
	}

	if err := h.broker.Enqueue(queueName, value); err != nil {
		h.logger.Printf("enqueue failed: queue=%s error=%v", queueName, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *QueueHandler) handleGet(w http.ResponseWriter, r *http.Request, queueName string) {
	timeout, err := parseTimeout(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	value, err := h.broker.Dequeue(r.Context(), queueName, timeout)
	if err == nil {
		// Dequeue removes the message before HTTP write. If the client is already
		// gone, put it back so a failed GET cannot lose a message.
		if r.Context().Err() != nil {
			h.broker.Requeue(queueName, value)
			return
		}
		if _, writeErr := w.Write([]byte(value)); writeErr != nil {
			// NOTE: the write may have already flushed some bytes to the client
			// before failing, so requeuing makes delivery effectively at-least-once
			// — the same message can be handed out again. Acceptable for this task;
			// stricter delivery would need client acks / dedup.
			h.broker.Requeue(queueName, value)
			h.logger.Printf("write response failed: queue=%s error=%v", queueName, writeErr)
		}
		return
	}

	// The spec says to return 404 when no message appears before timeout.
	// A canceled request reaches the same HTTP outcome from the client's side.
	if errors.Is(err, ErrQueueEmpty) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	h.logger.Printf("dequeue failed: queue=%s error=%v", queueName, err)
	w.WriteHeader(http.StatusInternalServerError)
}

// parseTimeout reads the optional ?timeout=N query parameter (whole seconds).
// Returns zero duration when the parameter is absent (no waiting).
func parseTimeout(r *http.Request) (time.Duration, error) {
	raw := r.URL.Query().Get("timeout")
	if raw == "" {
		return 0, nil
	}

	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 {
		return 0, ErrInvalidTimeout
	}

	return time.Duration(seconds) * time.Second, nil
}

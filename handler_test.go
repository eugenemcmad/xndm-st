// HTTP handler tests: unit tests with mockBroker isolate routing and status
// mapping; integration tests use httptest.Server with a real broker.
package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// ---- test doubles and helpers ----

type mockBroker struct {
	enqueueFunc func(queueName, value string) error
	dequeueFunc func(ctx context.Context, queueName string, timeout time.Duration) (string, error)
	requeueFunc func(queueName, value string)
}

func (m *mockBroker) Enqueue(queueName, value string) error {
	if m.enqueueFunc == nil {
		return nil
	}
	return m.enqueueFunc(queueName, value)
}

func (m *mockBroker) Dequeue(ctx context.Context, queueName string, timeout time.Duration) (string, error) {
	if m.dequeueFunc == nil {
		return "", ErrQueueEmpty
	}
	return m.dequeueFunc(ctx, queueName, timeout)
}

func (m *mockBroker) Requeue(queueName, value string) {
	if m.requeueFunc != nil {
		m.requeueFunc(queueName, value)
	}
}

type errorResponseWriter struct {
	header http.Header
}

func (w *errorResponseWriter) Header() http.Header       { return w.header }
func (w *errorResponseWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func (w *errorResponseWriter) WriteHeader(int)           {}

func newTestHandler(broker QueueBroker) http.Handler {
	return NewQueueHandler(broker, log.New(io.Discard, "", 0))
}

func newIntegrationServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(newTestHandler(NewInMemoryQueueBroker()))
	t.Cleanup(srv.Close)
	return srv
}

func mustHTTPRequest(t *testing.T, method, rawURL string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, rawURL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	return req
}

func doHTTP(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http: %v", err)
	}
	t.Cleanup(func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close body: %v", err)
		}
	})
	return resp
}

func readHTTPBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body)
}

func assertHTTPStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("status: want %d, got %d", want, resp.StatusCode)
	}
}

func assertEmptyHTTPBody(t *testing.T, resp *http.Response) {
	t.Helper()
	if body := readHTTPBody(t, resp); body != "" {
		t.Fatalf("body: want empty, got %q", body)
	}
}

func assertRecorderStatus(t *testing.T, w *httptest.ResponseRecorder, want int) {
	t.Helper()
	if w.Code != want {
		t.Fatalf("status: want %d, got %d", want, w.Code)
	}
}

func assertRecorderBody(t *testing.T, w *httptest.ResponseRecorder, want string) {
	t.Helper()
	if got := w.Body.String(); got != want {
		t.Fatalf("body: want %q, got %q", want, got)
	}
}

func assertRecorderEmptyBody(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Body.Len() != 0 {
		t.Fatalf("body: want empty, got %q", w.Body.String())
	}
}

// ---- handler unit tests ----

func TestHandler_PUT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		enqueueErr error
		wantStatus int
	}{
		{name: "ok", target: "/pet?v=cat", wantStatus: http.StatusOK},
		{name: "missing_v", target: "/pet", wantStatus: http.StatusBadRequest},
		{name: "empty_value", target: "/pet?v=", wantStatus: http.StatusOK},
		{name: "broker_error", target: "/pet?v=cat", enqueueErr: errors.New("storage failure"), wantStatus: http.StatusInternalServerError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			broker := &mockBroker{
				enqueueFunc: func(string, string) error { return tc.enqueueErr },
			}
			w := httptest.NewRecorder()
			newTestHandler(broker).ServeHTTP(w, mustHTTPRequest(t, http.MethodPut, tc.target))

			assertRecorderStatus(t, w, tc.wantStatus)
			assertRecorderEmptyBody(t, w)
		})
	}
}

func TestHandler_GET(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		value      string
		err        error
		wantStatus int
		wantBody   string
	}{
		{name: "ok", target: "/pet", value: "cat", wantStatus: http.StatusOK, wantBody: "cat"},
		{name: "empty_queue", target: "/pet", err: ErrQueueEmpty, wantStatus: http.StatusNotFound},
		{name: "invalid_timeout", target: "/pet?timeout=abc", wantStatus: http.StatusBadRequest},
		{name: "context_canceled", target: "/pet", err: context.Canceled, wantStatus: http.StatusNotFound},
		{name: "deadline_exceeded", target: "/pet", err: context.DeadlineExceeded, wantStatus: http.StatusNotFound},
		{name: "broker_error", target: "/pet", err: errors.New("storage failure"), wantStatus: http.StatusInternalServerError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			broker := &mockBroker{
				dequeueFunc: func(_ context.Context, _ string, _ time.Duration) (string, error) {
					return tc.value, tc.err
				},
			}
			w := httptest.NewRecorder()
			newTestHandler(broker).ServeHTTP(w, mustHTTPRequest(t, http.MethodGet, tc.target))

			assertRecorderStatus(t, w, tc.wantStatus)
			assertRecorderBody(t, w, tc.wantBody)
		})
	}
}

func TestHandler_GET_requeue(t *testing.T) {
	t.Parallel()

	t.Run("canceled_before_write", func(t *testing.T) {
		t.Parallel()

		var gotQueue, gotValue string
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		broker := &mockBroker{
			dequeueFunc: func(context.Context, string, time.Duration) (string, error) {
				return "cat", nil
			},
			requeueFunc: func(q, v string) { gotQueue, gotValue = q, v },
		}
		w := httptest.NewRecorder()
		newTestHandler(broker).ServeHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/pet", nil))

		if gotQueue != "pet" || gotValue != "cat" {
			t.Fatalf("requeue: want pet/cat, got %s/%s", gotQueue, gotValue)
		}
		assertRecorderEmptyBody(t, w)
	})

	t.Run("write_failed", func(t *testing.T) {
		t.Parallel()

		var gotQueue, gotValue string
		broker := &mockBroker{
			dequeueFunc: func(context.Context, string, time.Duration) (string, error) {
				return "cat", nil
			},
			requeueFunc: func(q, v string) { gotQueue, gotValue = q, v },
		}
		newTestHandler(broker).ServeHTTP(
			&errorResponseWriter{header: make(http.Header)},
			httptest.NewRequest(http.MethodGet, "/pet", nil),
		)

		if gotQueue != "pet" || gotValue != "cat" {
			t.Fatalf("requeue: want pet/cat, got %s/%s", gotQueue, gotValue)
		}
	})
}

func TestHandler_routing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
	}{
		{name: "unsupported_method", method: http.MethodDelete, target: "/pet", wantStatus: http.StatusMethodNotAllowed},
		{name: "empty_path", method: http.MethodGet, target: "/", wantStatus: http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			newTestHandler(&mockBroker{}).ServeHTTP(w, mustHTTPRequest(t, tc.method, tc.target))
			assertRecorderStatus(t, w, tc.wantStatus)
		})
	}
}

// Spec: error responses must have an empty body (no http.Error text).
func TestHandler_errorResponsesEmptyBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		target     string
		wantStatus int
	}{
		{name: "put_missing_v", method: http.MethodPut, target: "/pet", wantStatus: http.StatusBadRequest},
		{name: "get_empty_queue", method: http.MethodGet, target: "/pet", wantStatus: http.StatusNotFound},
		{name: "invalid_timeout", method: http.MethodGet, target: "/pet?timeout=-1", wantStatus: http.StatusBadRequest},
		{name: "unsupported_method", method: http.MethodDelete, target: "/pet", wantStatus: http.StatusMethodNotAllowed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			newTestHandler(NewInMemoryQueueBroker()).ServeHTTP(w, mustHTTPRequest(t, tc.method, tc.target))
			assertRecorderStatus(t, w, tc.wantStatus)
			assertRecorderEmptyBody(t, w)
		})
	}
}

// ---- integration tests ----

func TestIntegration_specExampleFIFO(t *testing.T) {
	t.Parallel()

	srv := newIntegrationServer(t)

	put := func(queue, value string) {
		t.Helper()
		resp := doHTTP(t, mustHTTPRequest(t, http.MethodPut, srv.URL+"/"+queue+"?v="+value))
		assertHTTPStatus(t, resp, http.StatusOK)
	}

	get := func(queue string, wantStatus int, wantBody string) {
		t.Helper()
		resp := doHTTP(t, mustHTTPRequest(t, http.MethodGet, srv.URL+"/"+queue))
		assertHTTPStatus(t, resp, wantStatus)
		if wantBody == "" {
			assertEmptyHTTPBody(t, resp)
			return
		}
		if got := readHTTPBody(t, resp); got != wantBody {
			t.Fatalf("body: want %q, got %q", wantBody, got)
		}
	}

	put("pet", "cat")
	put("pet", "dog")
	put("role", "manager")
	put("role", "executive")

	get("pet", http.StatusOK, "cat")
	get("pet", http.StatusOK, "dog")
	get("pet", http.StatusNotFound, "")
	get("role", http.StatusOK, "manager")
	get("role", http.StatusOK, "executive")
	get("role", http.StatusNotFound, "")
}

func TestIntegration_PUT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		target     string
		wantStatus int
		wantBody   string
	}{
		{name: "missing_v", target: "/pet", wantStatus: http.StatusBadRequest},
		{name: "url_encoded_value", target: "/pet?v=" + url.QueryEscape("hello world"), wantStatus: http.StatusOK, wantBody: "hello world"},
		{name: "duplicate_v_uses_first", target: "/pet?v=cat&v=dog", wantStatus: http.StatusOK, wantBody: "cat"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newIntegrationServer(t)
			resp := doHTTP(t, mustHTTPRequest(t, http.MethodPut, srv.URL+tc.target))
			assertHTTPStatus(t, resp, tc.wantStatus)

			if tc.wantBody == "" {
				assertEmptyHTTPBody(t, resp)
				return
			}

			getResp := doHTTP(t, mustHTTPRequest(t, http.MethodGet, srv.URL+"/pet"))
			assertHTTPStatus(t, getResp, http.StatusOK)
			if got := readHTTPBody(t, getResp); got != tc.wantBody {
				t.Fatalf("body: want %q, got %q", tc.wantBody, got)
			}
		})
	}
}

func TestIntegration_GET_timeout(t *testing.T) {
	t.Parallel()

	t.Run("receives_message_when_put_arrives", func(t *testing.T) {
		t.Parallel()

		srv := newIntegrationServer(t)
		done := make(chan struct{})
		var gotStatus int
		var gotBody string

		go func() {
			defer close(done)
			resp, err := http.Get(srv.URL + "/pet?timeout=2")
			if err != nil {
				t.Errorf("get: %v", err)
				return
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					t.Errorf("close body: %v", err)
				}
			}()
			gotStatus = resp.StatusCode
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("read body: %v", err)
				return
			}
			gotBody = string(body)
		}()

		time.Sleep(100 * time.Millisecond)
		doHTTP(t, mustHTTPRequest(t, http.MethodPut, srv.URL+"/pet?v=cat"))
		<-done

		if gotStatus != http.StatusOK || gotBody != "cat" {
			t.Fatalf("want 200 cat, got %d %q", gotStatus, gotBody)
		}
	})

	t.Run("expires_with_404", func(t *testing.T) {
		t.Parallel()

		srv := newIntegrationServer(t)
		resp := doHTTP(t, mustHTTPRequest(t, http.MethodGet, srv.URL+"/pet?timeout=1"))
		assertHTTPStatus(t, resp, http.StatusNotFound)
		assertEmptyHTTPBody(t, resp)
	})

	t.Run("zero_returns_immediately", func(t *testing.T) {
		t.Parallel()

		srv := newIntegrationServer(t)
		start := time.Now()
		resp := doHTTP(t, mustHTTPRequest(t, http.MethodGet, srv.URL+"/pet?timeout=0"))
		if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
			t.Fatalf("timeout=0 took %v", elapsed)
		}
		assertHTTPStatus(t, resp, http.StatusNotFound)
	})

	t.Run("empty_param_like_absent", func(t *testing.T) {
		t.Parallel()

		srv := newIntegrationServer(t)
		resp := doHTTP(t, mustHTTPRequest(t, http.MethodGet, srv.URL+"/pet?timeout="))
		assertHTTPStatus(t, resp, http.StatusNotFound)
	})

	t.Run("message_survives_after_timeout", func(t *testing.T) {
		t.Parallel()

		srv := newIntegrationServer(t)
		resp := doHTTP(t, mustHTTPRequest(t, http.MethodGet, srv.URL+"/pet?timeout=1"))
		assertHTTPStatus(t, resp, http.StatusNotFound)

		doHTTP(t, mustHTTPRequest(t, http.MethodPut, srv.URL+"/pet?v=late"))

		getResp := doHTTP(t, mustHTTPRequest(t, http.MethodGet, srv.URL+"/pet"))
		assertHTTPStatus(t, getResp, http.StatusOK)
		if got := readHTTPBody(t, getResp); got != "late" {
			t.Fatalf("body: want late, got %q", got)
		}
	})
}

func TestIntegration_errorResponsesEmptyBody(t *testing.T) {
	t.Parallel()

	srv := newIntegrationServer(t)
	tests := []struct {
		method     string
		target     string
		wantStatus int
	}{
		{method: http.MethodPut, target: "/pet", wantStatus: http.StatusBadRequest},
		{method: http.MethodGet, target: "/pet", wantStatus: http.StatusNotFound},
		{method: http.MethodGet, target: "/pet?timeout=-1", wantStatus: http.StatusBadRequest},
		{method: http.MethodDelete, target: "/pet", wantStatus: http.StatusMethodNotAllowed},
	}

	for _, tc := range tests {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			t.Parallel()
			resp := doHTTP(t, mustHTTPRequest(t, tc.method, srv.URL+tc.target))
			assertHTTPStatus(t, resp, tc.wantStatus)
			assertEmptyHTTPBody(t, resp)
		})
	}
}

// Mirrors main: BaseContext tied to lifecycle ctx cancels long-poll on shutdown.
func TestIntegration_gracefulShutdown(t *testing.T) {
	t.Parallel()

	broker := NewInMemoryQueueBroker()
	lifecycle, stop := context.WithCancel(context.Background())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := &http.Server{
		Handler: newTestHandler(broker),
		BaseContext: func(net.Listener) context.Context {
			return lifecycle
		},
	}

	listenDone := make(chan error, 1)
	go func() { listenDone <- server.Serve(ln) }()

	baseURL := "http://" + ln.Addr().String()
	requestDone := make(chan struct{})
	var gotStatus int

	go func() {
		defer close(requestDone)
		resp, err := http.Get(baseURL + "/pet?timeout=10")
		if err != nil {
			return
		}
		gotStatus = resp.StatusCode
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close body: %v", err)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := <-listenDone; err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("serve: %v", err)
	}

	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("long-poll did not finish after shutdown")
	}
	if gotStatus != http.StatusNotFound {
		t.Fatalf("status: want 404, got %d", gotStatus)
	}

	if err := broker.Enqueue("pet", "survivor"); err != nil {
		t.Fatalf("enqueue after shutdown: %v", err)
	}
	got, err := broker.Dequeue(context.Background(), "pet", 0)
	if err != nil || got != "survivor" {
		t.Fatalf("broker state: got %q, err %v", got, err)
	}
}

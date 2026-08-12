package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestRetryTransportRetriesWebDAVTransientFailure(t *testing.T) {
	t.Parallel()

	var calls int
	transport := &retryTransport{base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{
				StatusCode: http.StatusBadGateway,
				Body:       io.NopCloser(strings.NewReader("bad gateway")),
			}, nil
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusMultiStatus,
			Body:       io.NopCloser(strings.NewReader(string(body))),
		}, nil
	})}
	req, err := http.NewRequest("GET", "http://replica.worker/ltx/0/", strings.NewReader("body"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(webDAVMethodKey, "PROPFIND")
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("body")), nil
	}

	response, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusMultiStatus {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusMultiStatus)
	}
	if calls != 2 {
		t.Fatalf("round trips = %d, want 2", calls)
	}

	if shouldRetry(req, nil, errors.New("other error")) {
		t.Fatal("should not retry an unrelated error")
	}
	for _, errorText := range []string{"EOF", "unexpected EOF", "request failed: EOF"} {
		if !shouldRetry(req, nil, errors.New(errorText)) {
			t.Errorf("should retry %q", errorText)
		}
	}
}

func TestRetryTransportRetriesResponseBodyEOF(t *testing.T) {
	t.Parallel()

	var calls int
	transport := &retryTransport{base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{
				StatusCode:    http.StatusMultiStatus,
				ContentLength: 5,
				Body:          &unexpectedEOFBody{data: []byte("abc")},
			}, nil
		}
		return &http.Response{
			StatusCode:    http.StatusMultiStatus,
			ContentLength: 5,
			Body:          io.NopCloser(strings.NewReader("abcde")),
		}, nil
	})}
	req, err := http.NewRequest("GET", "http://replica.worker/ltx/0/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(webDAVMethodKey, "PROPFIND")
	response, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if string(body) != "abcde" {
		t.Fatalf("body = %q, want %q", body, "abcde")
	}
	if calls != 2 {
		t.Fatalf("round trips = %d, want 2", calls)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type unexpectedEOFBody struct {
	data []byte
	done bool
}

func (b *unexpectedEOFBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	b.done = true
	n := copy(p, b.data)
	return n, io.ErrUnexpectedEOF
}

func (b *unexpectedEOFBody) Close() error { return nil }

func TestWebDAVProxyTranslatesMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		incomingMethod string
		outgoingMethod string
		methodHeader   string
	}{
		{name: "MKCOL", incomingMethod: "MKCOL", outgoingMethod: http.MethodPost, methodHeader: "MKCOL"},
		{name: "PROPFIND", incomingMethod: "PROPFIND", outgoingMethod: http.MethodGet, methodHeader: "PROPFIND"},
		{name: "PUT", incomingMethod: http.MethodPut, outgoingMethod: http.MethodPut},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
				}
				if req.Method != tt.outgoingMethod {
					t.Errorf("method = %q, want %q", req.Method, tt.outgoingMethod)
				}
				if got := req.Header.Get(webDAVMethodKey); got != tt.methodHeader {
					t.Errorf("method header = %q, want %q", got, tt.methodHeader)
				}
				if got := string(body); got != "request body" {
					t.Errorf("body = %q, want %q", got, "request body")
				}
				if req.URL.Path != "/ltx/0/" || req.URL.RawQuery != "page=2" {
					t.Errorf("request URL = %q, want %q", req.URL.String(), "/ltx/0/?page=2")
				}
				w.Header().Set("X-Upstream", "ok")
				w.WriteHeader(http.StatusMultiStatus)
			}))
			defer upstream.Close()

			target, err := url.Parse(upstream.URL)
			if err != nil {
				t.Fatal(err)
			}
			proxy := httptest.NewServer(newWebDAVProxy(target, slog.New(slog.NewTextHandler(io.Discard, nil))))
			defer proxy.Close()

			req, err := http.NewRequest(tt.incomingMethod, proxy.URL+"/ltx/0/?page=2", strings.NewReader("request body"))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set(webDAVMethodKey, "spoofed")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusMultiStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusMultiStatus)
			}
			if got := resp.Header.Get("X-Upstream"); got != "ok" {
				t.Errorf("response header = %q, want %q", got, "ok")
			}
		})
	}
}

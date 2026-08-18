package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestWebDAVProxyTranslatesMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		incomingMethod string
		outgoingMethod string
		methodHeader   string
	}{
		{name: "MKCOL", incomingMethod: "MKCOL", outgoingMethod: http.MethodPost, methodHeader: "MKCOL"},
		{name: "PROPFIND", incomingMethod: "PROPFIND", outgoingMethod: http.MethodPost, methodHeader: "PROPFIND"},
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

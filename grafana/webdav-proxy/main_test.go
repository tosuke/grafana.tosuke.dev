package main

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestMain(m *testing.M) {
	// Disable logging during tests
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.Run()
}

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
			proxy := httptest.NewServer(newWebDAVProxy(target))
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

func TestRestoreDatabaseIfMissing(t *testing.T) {
	wantCommandError := errors.New("restore failed")
	tests := []struct {
		name         string
		existing     bool
		commandErr   error
		wantRestored bool
		wantErr      error
		wantCalled   bool
		wantName     string
		wantArgs     []string
	}{
		{
			name:         "missing invokes command with exact args",
			wantRestored: true,
			wantCalled:   true,
			wantName:     "/bin/litestream",
			wantArgs: []string{
				"restore",
				"-config", "/etc/litestream.yml",
				"-if-replica-exists",
				"-integrity-check", "quick",
			},
		},
		{
			name:         "existing skips command",
			existing:     true,
			wantRestored: false,
			wantCalled:   false,
		},
		{
			name:       "command error propagates",
			commandErr: wantCommandError,
			wantErr:    wantCommandError,
			wantCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := t.TempDir() + "/grafana.db"
			if tt.existing {
				if err := os.WriteFile(dbPath, []byte("database"), 0600); err != nil {
					t.Fatal(err)
				}
			}

			var gotName string
			var gotArgs []string
			called := false
			restored, err := restoreDatabaseIfMissing(dbPath, "/bin/litestream", "/etc/litestream.yml", func(name string, args ...string) error {
				called = true
				gotName = name
				gotArgs = append([]string(nil), args...)
				return tt.commandErr
			})
			if restored != tt.wantRestored {
				t.Fatalf("restoreDatabaseIfMissing() restored = %t, want %t", restored, tt.wantRestored)
			}
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("restoreDatabaseIfMissing() error = %v, want nil", err)
				}
			} else if !errors.Is(err, tt.wantErr) {
				t.Fatalf("restoreDatabaseIfMissing() error = %v, want wrapped %v", err, tt.wantErr)
			}
			if called != tt.wantCalled {
				t.Fatalf("restore command called = %t, want %t", called, tt.wantCalled)
			}
			if tt.wantName != "" && gotName != tt.wantName {
				t.Fatalf("command = %q, want %q", gotName, tt.wantName)
			}
			if tt.wantArgs != nil {
				wantArgs := append(append([]string(nil), tt.wantArgs...), dbPath)
				if strings.Join(gotArgs, "\x00") != strings.Join(wantArgs, "\x00") {
					t.Fatalf("arguments = %q, want %q", gotArgs, wantArgs)
				}
			}
		})
	}
}

func TestWriteLitestreamSnapshot(t *testing.T) {
	dir := t.TempDir()
	db := dir + "/grafana.db"
	litestreamDir := dir + "/.grafana.db-litestream"
	want := map[string]string{
		db:                                       "database bytes",
		db + "-wal":                              "wal bytes",
		db + "-shm":                              "shm bytes",
		litestreamDir + "/generation":            "generation bytes",
		litestreamDir + "/00000000/00000001.ltx": "ltx bytes",
	}
	for name, body := range want {
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(body), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(dir+"/unrelated.txt", []byte("must not be archived"), 0o640); err != nil {
		t.Fatal(err)
	}

	var body bytes.Buffer
	tw := tar.NewWriter(&body)
	if err := writeLitestreamSnapshot(tw, db); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	tr := tar.NewReader(&body)
	got := make(map[string]string)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !hdr.FileInfo().Mode().IsRegular() {
			continue
		}
		contents, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		got[hdr.Name] = string(contents)
	}

	if len(got) != len(want) {
		t.Fatalf("snapshot entries = %d, want %d: %#v", len(got), len(want), got)
	}
	for name, wantContents := range want {
		if gotContents, ok := got[name]; !ok || gotContents != wantContents {
			t.Errorf("snapshot[%q] = %q, present %t; want %q", name, gotContents, ok, wantContents)
		}
	}
	if _, ok := got[dir+"/unrelated.txt"]; ok {
		t.Errorf("snapshot contains unrelated file")
	}
}

func TestRestoreFromReaderDoesNotLeavePartialFiles(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "grafana.db")

	var body bytes.Buffer
	gw := gzip.NewWriter(&body)
	tw := tar.NewWriter(gw)
	if err := tw.WriteHeader(&tar.Header{Name: db, Mode: 0o600, Size: 8, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	// Close gzip without completing the tar stream to simulate a truncated snapshot.
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}

	if err := restoreFromReader(&body); err == nil {
		t.Fatal("restoreFromReader() error = nil, want truncated archive error")
	}
	if _, err := os.Stat(db); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat restored database error = %v, want file not to exist", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".snapshot-restore-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary restore files remain: %q", matches)
	}
}

func TestDelayedProcessTaskStartsOnlyAfterDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const delay = 10 * time.Second
		sentinel := errors.New("before start")
		started := make(chan struct{})
		task := newDelayedProcessTask([]string{"does-not-run"}, delay)
		task.beforeStart = func() error {
			close(started)
			return sentinel
		}
		if err := task.Start(context.Background()); err != nil {
			t.Fatal(err)
		}

		synctest.Sleep(delay - time.Nanosecond)
		synctest.Wait()
		select {
		case <-started:
			t.Fatal("process started before delay")
		default:
		}

		synctest.Sleep(time.Nanosecond)
		synctest.Wait()
		select {
		case <-started:
		default:
			t.Fatal("process did not start after delay")
		}
		if err := <-task.Done(); !errors.Is(err, sentinel) {
			t.Fatalf("task error = %v, want %v", err, sentinel)
		}
	})
}

func TestDelayedProcessTaskDoesNotStartAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		task := newDelayedProcessTask([]string{"does-not-run"}, 10*time.Second)
		task.beforeStart = func() error {
			close(started)
			return nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := task.Start(ctx); err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		select {
		case <-task.Done():
		default:
			t.Fatal("task did not finish after cancellation")
		}
		select {
		case <-started:
			t.Fatal("process started after cancellation")
		default:
		}
		if task.Started() {
			t.Fatal("process started after cancellation")
		}
	})
}

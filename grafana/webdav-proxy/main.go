package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	listenHost      = "127.0.0.1"
	webDAVMethodKey = "X-Method"
	grafanaDBPath   = "/var/lib/grafana/grafana.db"
)

var (
	port             = flag.Int("port", 8080, "port to listen on")
	upstream         = flag.String("upstream", "http://replica.worker", "upstream URL")
	replicationDelay = flag.Duration("replication-delay", 10*time.Second, "delay before starting Litestream replication")
	litestreamBin    = flag.String("litestream-bin", "/usr/local/bin/litestream", "Litestream binary")
	litestreamConfig = flag.String("litestream-config", "/etc/litestream.yml", "Litestream configuration file")
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	os.Exit(run())
}

func run() (status int) {
	flag.Parse()
	command := flag.Args()
	if len(command) == 0 {
		slog.Error("command is required")
		return 2
	}
	if *port < 1 || *port > 65535 {
		slog.Error("invalid port", "port", *port)
		return 2
	}

	var tasks []task

	target, err := url.Parse(*upstream)
	if err != nil {
		slog.Error("parse upstream URL", "error", err)
		return 1
	}
	listenAddress := net.JoinHostPort(listenHost, strconv.Itoa(*port))
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		slog.Error("listen", "address", listenAddress, "error", err)
		return 1
	}
	defer listener.Close()
	proxyServer := &http.Server{
		Handler:           newWebDAVProxy(target),
		ReadHeaderTimeout: 10 * time.Second,
	}
	tasks = append(tasks, &webDAVTask{server: proxyServer, listener: listener})
	tasks = append(tasks, newDelayedProcessTask([]string{*litestreamBin, "replicate", "-config", *litestreamConfig}, *replicationDelay))

	primary := newProcessTask(command)
	primary.beforeStart = func() error {
		if err := restoreFromRemote(context.Background(), http.DefaultClient); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("restore from remote snapshot", "error", err)
		}
		restored, err := restoreDatabaseIfMissing(grafanaDBPath, *litestreamBin, *litestreamConfig, func(name string, args ...string) error {
			cmd := exec.Command(name, args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Env = os.Environ()
			return cmd.Run()
		})
		if err != nil {
			return err
		}
		if restored {
			slog.Info("Litestream restore completed", "database", grafanaDBPath)
		}
		return nil
	}
	tasks = append(tasks, primary)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	status, save := superviseTasks(tasks, sigChan)
	if save {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		slog.Info("saving snapshot")
		if err := saveSnapshot(ctx, http.DefaultClient); err != nil {
			slog.Error("save snapshot", "error", err)
		}
		return status
	}

	return status
}

func restoreDatabaseIfMissing(dbPath, litestreamBin, configPath string, run func(string, ...string) error) (bool, error) {
	if _, err := os.Stat(dbPath); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("stat database: %w", err)
	}

	slog.Info("Grafana database is missing; restoring with Litestream", "database", dbPath)
	if err := run(
		litestreamBin,
		"restore",
		"-config", configPath,
		"-if-replica-exists",
		"-integrity-check", "quick",
		dbPath,
	); err != nil {
		return false, fmt.Errorf("run Litestream restore: %w", err)
	}
	return true, nil
}

func newWebDAVProxy(target *url.URL) http.Handler {
	proxy := &httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(target)
			req.Out.Header.Del(webDAVMethodKey)

			switch req.Out.Method {
			case "MKCOL", "PROPFIND", "COPY", "MOVE", "LOCK", "UNLOCK":
				req.Out.Header.Set(webDAVMethodKey, req.Out.Method)
				req.Out.Method = http.MethodPost
			}
		},
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		slog.Error("proxy request", "method", req.Method, "path", req.URL.Path, "error", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
	}
	return proxy
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		return exitErr.ExitCode()
	}
	return 1
}

func restoreFromRemote(ctx context.Context, client *http.Client) error {
	req, err := http.NewRequestWithContext(ctx, "GET", "http://metadata.worker/snapshot", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		if resp.StatusCode == http.StatusNotFound {
			return os.ErrNotExist
		}
		return fmt.Errorf("unexpected response from metadata worker: %s", resp.Status)
	}
	if err := restoreFromReader(resp.Body); err != nil {
		return err
	}
	go func() {
		req, err := http.NewRequestWithContext(ctx, "DELETE", "http://metadata.worker/snapshot", nil)
		if err != nil {
			slog.ErrorContext(ctx, "create request to delete snapshot", "error", err)
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			slog.ErrorContext(ctx, "send request to delete snapshot", "error", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			slog.ErrorContext(ctx, "unexpected response from metadata worker when deleting snapshot", "status", resp.Status)
			return
		}
	}()
	return nil
}

func restoreFromReader(r io.Reader) error {
	gr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}

	tr := tar.NewReader(gr)
	madeDir := make(map[string]bool)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		info := hdr.FileInfo()
		switch hdr.Typeflag {
		case tar.TypeDir:
			if !madeDir[hdr.Name] {
				if err := os.MkdirAll(hdr.Name, 0755); err != nil {
					return err
				}
				madeDir[hdr.Name] = true
			}
		case tar.TypeReg:
			dir := path.Dir(hdr.Name)
			if !madeDir[dir] {
				if err := os.MkdirAll(dir, 0755); err != nil {
					return err
				}
				madeDir[dir] = true
			}

			if err := func() error {
				f, err := os.OpenFile(hdr.Name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
				if err != nil {
					return err
				}
				defer f.Close()

				n, err := io.Copy(f, tr)
				if err != nil {
					return err
				}
				if n != info.Size() {
					return fmt.Errorf("unexpected number of bytes written for %s: got %d, want %d", hdr.Name, n, info.Size())
				}
				return nil
			}(); err != nil {
				return err
			}
		default:
			return errors.New("unexpected file type in snapshot: " + hdr.Name)
		}
	}
	return nil
}

func saveSnapshot(ctx context.Context, client *http.Client) error {
	var body bytes.Buffer
	gw := gzip.NewWriter(&body)
	tw := tar.NewWriter(gw)

	if err := writeLitestreamSnapshot(tw, grafanaDBPath); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", "http://metadata.worker/snapshot", &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/gzip")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("unexpected response from metadata worker: %s", resp.Status)
	}
	return nil
}

func writeLitestreamSnapshot(tw *tar.Writer, db string) error {
	dir := path.Dir(db)
	base := path.Base(db)
	fsys := os.DirFS(dir)
	return fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		if name != base &&
			name != base+"-wal" &&
			name != base+"-shm" &&
			!strings.HasPrefix(name, "."+base+"-litestream") {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		h, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		h.Name = path.Join(dir, name)
		if d.IsDir() {
			h.Name += "/"
		}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		f, err := fsys.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := io.Copy(tw, f); err != nil {
			return err
		}
		return nil
	})
}

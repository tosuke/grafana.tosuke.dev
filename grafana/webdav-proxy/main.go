package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	listenHost      = "127.0.0.1"
	webDAVMethodKey = "X-Method"
	maxRetries      = 2
)

var (
	port     = flag.Int("port", 8080, "port to listen on")
	upstream = flag.String("upstream", "http://replica.worker", "upstream URL")
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	os.Exit(run(logger))
}

func run(logger *slog.Logger) int {
	flag.Parse()
	command := flag.Args()
	if len(command) == 0 {
		logger.Error("command is required")
		return 2
	}
	if *port < 1 || *port > 65535 {
		logger.Error("invalid port", "port", *port)
		return 2
	}

	target, err := url.Parse(*upstream)
	if err != nil {
		logger.Error("parse upstream URL", "error", err)
		return 1
	}
	listenAddress := net.JoinHostPort(listenHost, strconv.Itoa(*port))
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		logger.Error("listen", "address", listenAddress, "error", err)
		return 1
	}

	server := &http.Server{
		Handler:           newWebDAVProxy(target, logger),
		ReadHeaderTimeout: 10 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()
	logger.Info("WebDAV method proxy listening", "address", listenAddress, "upstream", target)

	child := exec.Command(command[0], command[1:]...)
	child.Stdout = os.Stdout
	child.Env = os.Environ()
	if err := child.Start(); err != nil {
		_ = server.Close()
		logger.Error("start child", "command", command[0], "error", err)
		return 1
	}
	logger.Info("child started", "command", command[0], "pid", child.Process.Pid)

	childDone := make(chan error, 1)
	go func() { childDone <- child.Wait() }()

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	for {
		select {
		case err := <-childDone:
			shutdownServer(logger, server)
			return exitCode(err)
		case err := <-serverErr:
			logger.Error("WebDAV method proxy stopped", "error", err)
			_ = child.Process.Signal(syscall.SIGTERM)
			<-childDone
			return 1
		case sig := <-signals:
			logger.Info("forwarding signal", "signal", sig, "pid", child.Process.Pid)
			if err := child.Process.Signal(sig); err != nil {
				logger.Warn("forward signal", "error", err)
			}
		}
	}
}

func newWebDAVProxy(target *url.URL, logger *slog.Logger) http.Handler {
	proxy := &httputil.ReverseProxy{
		// Transport: &retryTransport{base: http.DefaultTransport},
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(target)
			req.Out.Header.Del(webDAVMethodKey)

			switch req.Out.Method {
			case "MKCOL", "PROPFIND", "COPY", "MOVE", "LOCK", "UNLOCK":
				req.Out.Header.Set(webDAVMethodKey, req.Out.Method)
				req.Out.Method = http.MethodPost
				// case "PROPFIND":
				// 	req.Out.Header.Set(webDAVMethodKey, req.Out.Method)
				// 	req.Out.Method = http.MethodGet
			}
		},
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		logger.Error("proxy request", "method", req.Method, "path", req.URL.Path, "error", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
	}
	return proxy
	// return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
	// 	if req.Method == "PROPFIND" {
	// 		if req.Body != nil {
	// 			var err error
	// 			body, err := io.ReadAll(req.Body)
	// 			if err != nil {
	// 				logger.Error("read PROPFIND body", "error", err)
	// 				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	// 				return
	// 			}
	// 			_ = req.Body.Close()
	// 			req.Body = io.NopCloser(bytes.NewReader(body))
	// 			req.ContentLength = int64(len(body))
	// 			req.TransferEncoding = nil
	// 			req.GetBody = func() (io.ReadCloser, error) {
	// 				return io.NopCloser(bytes.NewReader(body)), nil
	// 			}
	// 		}
	// 	}
	// 	proxy.ServeHTTP(w, req)
	// })
}

type retryTransport struct {
	base http.RoundTripper
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.roundTrip(req, 0)
}

func (t *retryTransport) roundTrip(req *http.Request, attempt int) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}

	current := req
	if attempt > 0 {
		current = req.Clone(req.Context())
		if req.Body != nil {
			if req.GetBody == nil {
				return nil, errors.New("request body is not replayable")
			}
			body, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			current.Body = body
		}
	}

	response, err := base.RoundTrip(current)
	if shouldRetry(req, response, err) && attempt < maxRetries {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return t.roundTrip(req, attempt+1)
	}
	if response != nil && response.Body != nil {
		response.Body = &retryResponseBody{
			transport: t,
			request:   req,
			response:  response,
			body:      response.Body,
			attempt:   attempt,
		}
	}
	return response, err
}

type retryResponseBody struct {
	transport *retryTransport
	request   *http.Request
	response  *http.Response
	body      io.ReadCloser
	attempt   int
	read      int64
	pending   bool
}

func (b *retryResponseBody) Read(p []byte) (int, error) {
	if b.pending {
		if err := b.retry(); err != nil {
			return 0, err
		}
	}

	n, err := b.body.Read(p)
	b.read += int64(n)
	if !retryableResponseRead(b.response, b.read, err) {
		return n, err
	}
	if n > 0 {
		b.pending = true
		return n, nil
	}
	if retryErr := b.retry(); retryErr != nil {
		return 0, retryErr
	}
	return b.Read(p)
}

func (b *retryResponseBody) Close() error {
	return b.body.Close()
}

func (b *retryResponseBody) retry() error {
	_ = b.body.Close()
	response, err := b.transport.roundTrip(b.request, b.attempt+1)
	if err != nil {
		return err
	}
	if response.StatusCode != b.response.StatusCode {
		_ = response.Body.Close()
		return io.ErrUnexpectedEOF
	}
	if b.read > 0 {
		if _, err := io.CopyN(io.Discard, response.Body, b.read); err != nil {
			_ = response.Body.Close()
			return err
		}
	}
	b.response = response
	b.body = response.Body
	b.pending = false
	return nil
}

func retryableResponseRead(response *http.Response, read int64, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	return err == io.EOF && response.ContentLength >= 0 && read < response.ContentLength
}

func shouldRetry(req *http.Request, response *http.Response, err error) bool {
	if req.Method != http.MethodGet && req.Method != http.MethodDelete {
		return false
	}
	if response != nil {
		return response.StatusCode == http.StatusBadGateway
	}
	return retryableRoundTripError(err)
}

func retryableRoundTripError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	errorText := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.HasSuffix(errorText, "eof") || strings.Contains(errorText, "connection reset")
}

func shutdownServer(logger *slog.Logger, server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Warn("shutdown WebDAV method proxy", "error", err)
	}
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

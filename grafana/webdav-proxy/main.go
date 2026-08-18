package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const (
	listenHost      = "127.0.0.1"
	webDAVMethodKey = "X-Method"
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
		logger.Error("proxy request", "method", req.Method, "path", req.URL.Path, "error", err)
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
	}
	return proxy
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

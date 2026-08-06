package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/benbjohnson/litestream"
	_ "github.com/tosuke/grafana.tosuke.dev/grafana/ltxws"
)

type config struct {
	databasePath string
	replicaURL   string
	syncInterval time.Duration
	closeTimeout time.Duration
	monitorPort  int
	command      []string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	config, err := parseConfig(os.Args[1:])
	if err != nil {
		logger.Error("parse configuration", "error", err)
		os.Exit(2)
	}
	if err := run(logger, config); err != nil {
		logger.Error("litestream supervisor failed", "error", err)
		os.Exit(1)
	}
}

func parseConfig(args []string) (config, error) {
	var config config
	flags := flag.NewFlagSet("litestream-supervisor", flag.ContinueOnError)
	flags.StringVar(&config.databasePath, "database", "", "SQLite database path")
	flags.StringVar(&config.replicaURL, "replica-url", "", "LTX WebSocket replica URL")
	flags.DurationVar(&config.syncInterval, "sync-interval", 0, "Litestream sync interval")
	flags.DurationVar(&config.closeTimeout, "close-timeout", 0, "final sync and shutdown timeout")
	flags.IntVar(&config.monitorPort, "monitor-port", 0, "TCP port to wait for before starting compaction monitors")
	if err := flags.Parse(args); err != nil {
		return config, err
	}
	config.command = flags.Args()
	switch {
	case config.databasePath == "":
		return config, errors.New("--database is required")
	case config.replicaURL == "":
		return config, errors.New("--replica-url is required")
	case config.syncInterval <= 0:
		return config, errors.New("--sync-interval must be positive")
	case config.closeTimeout <= 0:
		return config, errors.New("--close-timeout must be positive")
	case config.monitorPort <= 0 || config.monitorPort > 65535:
		return config, errors.New("--monitor-port must be between 1 and 65535")
	case len(config.command) == 0:
		return config, errors.New("command is required after --")
	}
	return config, nil
}

func run(logger *slog.Logger, config config) error {
	processCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := litestream.NewReplicaClientFromURL(config.replicaURL)
	if err != nil {
		return fmt.Errorf("create replica client: %w", err)
	}
	db := litestream.NewDB(config.databasePath)
	repl := litestream.NewReplicaWithClient(db, client)
	repl.SyncInterval = config.syncInterval
	db.Replica = repl
	db.SetLogger(logger.With("db", config.databasePath))

	logger.Info("waiting for replica RPC session")
	if err := client.Init(processCtx); err != nil {
		return fmt.Errorf("initialize replica: %w", err)
	}

	logger.Info("ensuring database exists and opening")
	if err := db.EnsureExists(processCtx); err != nil {
		return fmt.Errorf("restore database: %w", err)
	}
	store := litestream.NewStore([]*litestream.DB{db}, litestream.DefaultCompactionLevels)
	store.Logger = logger.With("component", "litestream")
	if err := db.Open(); err != nil {
		return fmt.Errorf("open litestream database: %w", err)
	}
	logger.Info("ensured database exists and opened", "path", config.databasePath)

	command := exec.Command(config.command[0], config.command[1:]...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.Env = os.Environ()
	if err := command.Start(); err != nil {
		closeStore(logger, store)
		return fmt.Errorf("start exec comand: %w", err)
	}
	logger.Info("Child process started", "pid", command.Process.Pid)

	processErr := make(chan error, 1)
	childDone := make(chan struct{})
	go func() {
		processErr <- command.Wait()
		close(childDone)
	}()
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	finish := func(childErr error, expectedShutdown bool) error {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), config.closeTimeout)
		defer closeCancel()
		storeErr := store.Close(closeCtx)
		if storeErr != nil {
			logger.Error("close Litestream store", "error", storeErr)
		}
		if childErr != nil && !expectedShutdown {
			return childErr
		}
		if storeErr != nil {
			return fmt.Errorf("close Litestream store: %w", storeErr)
		}
		return nil
	}

	receivedSignal, err := waitForPort(
		processCtx,
		config.monitorPort,
		childDone,
		signals,
	)
	if err != nil {
		if errors.Is(err, errChildExitedBeforePort) {
			childErr := <-processErr
			logChildrenExit(logger, childErr, false)
			return finish(childErr, false)
		}
		return finish(err, false)
	}
	if receivedSignal != nil {
		logger.Info("forwarding signal to children", "signal", receivedSignal)
		forwardErr := command.Process.Signal(receivedSignal)
		if forwardErr != nil {
			logger.Warn("signal", "error", forwardErr)
		}
		childErr := <-processErr
		expected := forwardErr == nil && processTerminatedBy(childErr, receivedSignal)
		logChildrenExit(logger, childErr, expected)
		return finish(childErr, expected)
	}

	logger.Info("child process port is listening", "port", config.monitorPort)
	if err := store.Open(processCtx); err != nil {
		_ = command.Process.Signal(syscall.SIGTERM)
		childErr := <-processErr
		logChildrenExit(logger, childErr, false)
		if finishErr := finish(childErr, false); finishErr != nil {
			return fmt.Errorf("open Litestream store: %w; cleanup: %v", err, finishErr)
		}
		return fmt.Errorf("open Litestream store: %w", err)
	}
	logger.Info("Litestream store opened and compaction monitors enabled")

	childErr, expectedShutdown := waitForChild(
		logger,
		processErr,
		signals,
		command.Process.Signal,
	)

	return finish(childErr, expectedShutdown)
}

var errChildExitedBeforePort = errors.New("child process exited before monitor port became ready")

func waitForPort(
	ctx context.Context,
	port int,
	childDone <-chan struct{},
	signals <-chan os.Signal,
) (os.Signal, error) {
	address := fmt.Sprintf("127.0.0.1:%d", port)
	dialer := net.Dialer{Timeout: 100 * time.Millisecond}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		dialCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		conn, err := dialer.DialContext(dialCtx, "tcp", address)
		cancel()
		if err == nil {
			_ = conn.Close()
			return nil, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-childDone:
			return nil, errChildExitedBeforePort
		case received := <-signals:
			return received, nil
		case <-ticker.C:
		}
	}
}

func closeStore(logger *slog.Logger, store *litestream.Store) {
	if err := store.Close(context.Background()); err != nil {
		logger.Error("close Litestream store", "error", err)
	}
}

func waitForChild(
	logger *slog.Logger,
	processErr <-chan error,
	signals <-chan os.Signal,
	forwardSignal func(os.Signal) error,
) (error, bool) {
	select {
	case err := <-processErr:
		logChildrenExit(logger, err, false)
		return err, false
	case received := <-signals:
		logger.Info("forwarding signal to children", "signal", received)
		forwardErr := forwardSignal(received)
		if forwardErr != nil {
			logger.Warn("signal", "error", forwardErr)
		}
		err := <-processErr
		expected := forwardErr == nil && processTerminatedBy(err, received)
		logChildrenExit(logger, err, expected)
		return err, expected
	}
}

func logChildrenExit(logger *slog.Logger, err error, expectedShutdown bool) {
	if err == nil {
		logger.Info("child process exited", "exitCode", 0, "expectedShutdown", expectedShutdown)
		return
	}

	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		if status, ok := exitError.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			logger.Warn(
				"Child process terminated",
				"signal", status.Signal().String(),
				"expectedShutdown", expectedShutdown,
				"error", err,
			)
			return
		}
		logger.Error(
			"Child process exited",
			"exitCode", exitError.ExitCode(),
			"expectedShutdown", expectedShutdown,
			"error", err,
		)
		return
	}
	logger.Error("Child process wait failed", "expectedShutdown", expectedShutdown, "error", err)
}

func processTerminatedBy(err error, expected os.Signal) bool {
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		return false
	}
	status, ok := exitError.Sys().(syscall.WaitStatus)
	expectedSignal, expectedOK := expected.(syscall.Signal)
	return ok && expectedOK && status.Signaled() && status.Signal() == expectedSignal
}

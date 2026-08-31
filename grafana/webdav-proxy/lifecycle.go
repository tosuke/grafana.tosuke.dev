package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// task is a concurrently running service with an explicit shutdown operation.
// Start must return after arranging the service's asynchronous work; Done
// reports the service's eventual result.
type task interface {
	Start(context.Context) error
	Done() <-chan error
	Shutdown(context.Context, os.Signal) error
}

type webDAVTask struct {
	server   *http.Server
	listener net.Listener
	done     chan error
}

func (t *webDAVTask) Start(context.Context) error {
	if t.done != nil {
		return errors.New("WebDAV task already started")
	}
	t.done = make(chan error, 1)
	go func() {
		err := t.server.Serve(t.listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		t.done <- err
		close(t.done)
	}()
	return nil
}

func (t *webDAVTask) Done() <-chan error { return t.done }

func (t *webDAVTask) Shutdown(ctx context.Context, _ os.Signal) error {
	if t.done == nil {
		return nil
	}
	if err := t.server.Shutdown(ctx); err != nil {
		if closeErr := t.server.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			return errors.Join(err, closeErr)
		}
		return err
	}
	return nil
}

type processTask struct {
	command     []string
	beforeStart func() error

	mu      sync.Mutex
	cmd     *exec.Cmd
	done    chan error
	result  error
	started bool
}

func newProcessTask(command []string) *processTask {
	return &processTask{command: command}
}

func (t *processTask) Start(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.startLocked()
}

func (t *processTask) startLocked() error {
	if t.started {
		return errors.New("process task already started")
	}
	if len(t.command) == 0 {
		return errors.New("process command is empty")
	}
	if t.beforeStart != nil {
		if err := t.beforeStart(); err != nil {
			return err
		}
		t.beforeStart = nil
	}
	t.cmd = exec.Command(t.command[0], t.command[1:]...)
	t.cmd.Stdout = os.Stdout
	t.cmd.Stderr = os.Stderr
	t.cmd.Env = os.Environ()
	if err := t.cmd.Start(); err != nil {
		return err
	}
	t.started = true
	if t.done == nil {
		t.done = make(chan error, 1)
	}
	go func() {
		err := t.cmd.Wait()
		t.mu.Lock()
		t.result = err
		t.mu.Unlock()
		t.done <- err
		close(t.done)
	}()
	return nil
}

func (t *processTask) Done() <-chan error { return t.done }

func (t *processTask) Started() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.started
}

func (t *processTask) Shutdown(ctx context.Context, sig os.Signal) error {
	t.mu.Lock()
	cmd, started := t.cmd, t.started
	t.mu.Unlock()
	if !started {
		return nil
	}
	if err := cmd.Process.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case <-t.done:
		return t.Result()
	case <-ctx.Done():
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return errors.Join(ctx.Err(), err)
		}
		return ctx.Err()
	}
}

func (t *processTask) Result() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.result
}

type delayedProcessTask struct {
	*processTask
	delay time.Duration
}

func newDelayedProcessTask(command []string, delay time.Duration) *delayedProcessTask {
	return &delayedProcessTask{
		processTask: newProcessTask(command),
		delay:       delay,
	}
}

func (t *delayedProcessTask) Start(ctx context.Context) error {
	t.mu.Lock()
	if t.done != nil {
		t.mu.Unlock()
		return errors.New("delayed process task already started")
	}
	t.done = make(chan error, 1)
	t.mu.Unlock()

	timer := time.NewTimer(t.delay)
	go func() {
		defer timer.Stop()
		select {
		case <-ctx.Done():
			close(t.done)
		case <-timer.C:
			t.mu.Lock()
			defer t.mu.Unlock()
			if ctx.Err() != nil {
				close(t.done)
				return
			}
			if err := t.startLocked(); err != nil {
				t.done <- err
				close(t.done)
			}
		}
	}()
	return nil
}

func (t *delayedProcessTask) Done() <-chan error { return t.done }

func (t *delayedProcessTask) Shutdown(ctx context.Context, sig os.Signal) error {
	t.mu.Lock()
	started := t.started
	done := t.done
	t.mu.Unlock()
	if !started {
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
	return t.processTask.Shutdown(ctx, sig)
}

type taskEvent struct {
	index int
	err   error
}

func superviseTasks(tasks []task, signals <-chan os.Signal) (status int, save bool) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	started := make([]task, 0, len(tasks))
	for _, current := range tasks {
		if err := current.Start(ctx); err != nil {
			status = 1
			cancel(err)
			break
		}
		started = append(started, current)
	}

	events := make(chan taskEvent, len(started))
	for index, current := range started {
		go func(index int, current task) {
			events <- taskEvent{index: index, err: <-current.Done()}
		}(index, current)
	}

	shutdownSignal := os.Signal(syscall.SIGTERM)
	if context.Cause(ctx) == nil {
		select {
		case sig, ok := <-signals:
			if !ok || sig == nil {
				status = 1
				cancel(errors.New("signal channel closed"))
			} else {
				shutdownSignal = sig
				save = true
				cancel(fmt.Errorf("shutdown requested by signal: %v", sig))
			}
		case event := <-events:
			status = exitCode(event.err)
			if status == 0 {
				status = 1
			}
			cancel(errors.New("task exited"))
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	shutdownErrors := make([]error, len(started))
	for index := len(started) - 1; index >= 0; index-- {
		shutdownErrors[index] = started[index].Shutdown(shutdownCtx, shutdownSignal)
	}
	if save && len(started) > 1 {
		if primary, ok := started[1].(*processTask); ok {
			status = exitCode(primary.Result())
		} else if err := shutdownErrors[1]; err != nil {
			status = exitCode(err)
		}
	}
	return status, save
}

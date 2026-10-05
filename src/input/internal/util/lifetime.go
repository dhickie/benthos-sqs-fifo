package util

import (
	"context"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Lifetime informs ongoing processes that the application is being stopped and should stop processing work.
type Lifetime struct {
	kctx        context.Context // kctx provides access to the lifetime kill context, which is cancelled if the application is killed
	tctx        context.Context // tctx provides access to the lifetime terminate context, which is cancelled if the application is terminated
	kCancelFunc context.CancelFunc
	tCancelFunc context.CancelFunc
	stopped     chan struct{}  // stopped is sent a signal when processes have stopped.
	wg          sync.WaitGroup // wg is used to ensure all processes have finished shutting down before sending the stopped signal.
	funcs       []func(ILifetimeHandle)
	handles     []*LifetimeHandle
	logger      *service.Logger
}

// NewLifetime returns a new Lifetime object that is used to signal the end of the application lifetime to goroutines.
func NewLifetime(logger *service.Logger) *Lifetime {
	kctx, kCancel := context.WithCancel(context.Background())
	tctx, tCancel := context.WithCancel(kctx)
	l := Lifetime{
		kctx:        kctx,
		tctx:        tctx,
		kCancelFunc: kCancel,
		tCancelFunc: tCancel,
		stopped:     make(chan struct{}, 1),
		wg:          sync.WaitGroup{},
		funcs:       make([]func(ILifetimeHandle), 0),
		handles:     make([]*LifetimeHandle, 0),
		logger:      logger,
	}
	return &l
}

// Terminate tells processes that the application is stopping soon, and they should finish their current work without
// taking on anything new.
func (l *Lifetime) Terminate() {
	if len(l.handles) == 0 {
		panic("Cannot terminate lifetime with no started routines")
	}

	l.tCancelFunc()

	for _, v := range l.handles {
		v.sigterm <- struct{}{}
	}
}

// Kill tells processes that the application is stopping immediately, and they should stop regardless of the state of
// their current work.
func (l *Lifetime) Kill() {
	if len(l.handles) == 0 {
		panic("Cannot kill lifetime with no started routines")
	}

	l.kCancelFunc()

	for _, v := range l.handles {
		v.sigkill <- struct{}{}
	}
}

// Register is used to let Lifetime know there is a process that should be started when calling Start, and should be
// signalled when the application is closing
func (l *Lifetime) Register(f func(ILifetimeHandle)) {
	l.funcs = append(l.funcs, f)
}

// Start starts all registered subroutines
func (l *Lifetime) Start() {
	for _, v := range l.funcs {
		hSigterm := make(chan struct{}, 1)
		hSigkill := make(chan struct{}, 1)
		handle := newLifetimeHandle(l.kctx, l.tctx, hSigterm, hSigkill)
		l.handles = append(l.handles, handle)
		l.wg.Go(func() {
			defer func() {
				if err := recover(); err != nil {
					l.logger.Errorf("Child goroutine panicked: %v", err)
				}
			}()
			v(handle)
		})
	}
	go l.wait()
}

// Stopped returns a channel that can be used in a select statement to know when all child goroutines have finished
func (l *Lifetime) Stopped() chan struct{} {
	return l.stopped
}

// Runs in a goroutine to signal the Stopped channel when all processes have completed shutdown.
func (l *Lifetime) wait() {
	l.wg.Wait()
	l.stopped <- struct{}{}
}

// LifetimeHandle is passed to individual subroutines as a channel through which they can be informed that the application
// is closing
type LifetimeHandle struct {
	sigterm chan struct{}
	sigkill chan struct{}
	kctx    context.Context
	tctx    context.Context
}

// ILifetimeHandle represents a channel through which a subroutine can be informed that the application is closing
type ILifetimeHandle interface {
	Terminated() chan struct{}
	Killed() chan struct{}
	KillContext() context.Context
	TerminateContext() context.Context
}

// Returns a new LifetimeHandle object with its own termination channel and kill channel/context
func newLifetimeHandle(kctx context.Context, tctx context.Context, sigterm chan struct{}, sigkill chan struct{}) *LifetimeHandle {
	return &LifetimeHandle{
		kctx:    kctx,
		tctx:    tctx,
		sigterm: sigterm,
		sigkill: sigkill,
	}
}

// Terminated returns a channel that will be signalled when the application is undergoing a graceful shutdown
func (l *LifetimeHandle) Terminated() chan struct{} {
	return l.sigterm
}

// Killed returns a channel that will be signalled when the application is undergoing a hard shutdown
func (l *LifetimeHandle) Killed() chan struct{} {
	return l.sigkill
}

// KillContext returns a context object that will be cancelled when the application is being killed
func (l *LifetimeHandle) KillContext() context.Context {
	return l.kctx
}

func (l *LifetimeHandle) TerminateContext() context.Context {
	return l.tctx
}

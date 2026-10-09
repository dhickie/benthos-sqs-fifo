package util

import (
	"context"
	"sync"
	"uuid"
)

// ContextCond provides the ability to wait until a signal is received or a context is cancelled
type ContextCond struct {
	ch chan struct{} // Channel used to signal that something has changed
	l  sync.Locker   // The locker that underlies the signal
}

// NewContextCond returns a new instance using the provided underlying locker
func NewContextCond(l sync.Locker) *ContextCond {
	return &ContextCond{
		ch: make(chan struct{}, 1),
		l:  l,
	}
}

// Wait waits until either a signal is provided to the cond or the provided context is cancelled.
// The thread calling Wait must currently hold the underlying lock before calling this method.
// Returns the error from the context if the context is cancelled.
func (c *ContextCond) Wait(ctx context.Context) error {
	c.l.Unlock()

	select {
	case <-ctx.Done():
		c.l.Lock()
		return ctx.Err()
	case <-c.ch:
		c.l.Lock()
		return nil
	}
}

// Signal signals a waiting thread, if there is one, that is waiting on this condition
func (c *ContextCond) Signal() {
	select {
	case c.ch <- struct{}{}:
		return
	default:
	}
}

// ------------------------------------------------------------------------------------------------------------------ //

// AsyncCond allows code to signal another single goroutine that it is OK to continue.
// The routines don't need to share a lock, and the signal and wait can happen asynchronously - if the signaller
// signals before a goroutine is waiting, then the next goroutine to get there won't need to wait at all.
// If the signaller signals when there is already a signal there, the signal is skipped.
type AsyncCond struct {
	ch chan struct{} // The underlying channel
}

func NewAsyncCond() *AsyncCond {
	return &AsyncCond{
		ch: make(chan struct{}, 1),
	}
}

// Signal signals that it is OK for another goroutine to continue. Does not block if the signal has already been set
func (c *AsyncCond) Signal() {
	select {
	case c.ch <- struct{}{}:
	default:
	}
}

// Wait waits for the condition to be signalled, if it hasn't already.
func (c *AsyncCond) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ch:
		return nil
	}
}

// WaitChan returns a channel for use in select statements that can be read from when the cond is signalled
func (c *AsyncCond) WaitChan() <-chan struct{} {
	return c.ch
}

// ------------------------------------------------------------------------------------------------------------------ //

// Signaller lets multiple goroutines send signals to each other. Useful if you have a goroutine in which something happens,
// and you want multiple other goroutines to be aware of that fact.
type Signaller struct {
	in       chan string
	out      map[string]chan struct{}
	mu       sync.Mutex
	ctx      context.Context
	stopFunc context.CancelFunc
	wg       sync.WaitGroup
	stopped  bool
}

// NewSignaller creates a new signaller. The provided context is used as the parent to the context that is cancelled
// when the signaller should stop relaying signals.
func NewSignaller(pCtx context.Context) *Signaller {
	ctx, cancel := context.WithCancel(pCtx)
	s := &Signaller{
		in:       make(chan string),
		out:      make(map[string]chan struct{}),
		mu:       sync.Mutex{},
		ctx:      ctx,
		stopFunc: cancel,
		wg:       sync.WaitGroup{},
		stopped:  false,
	}
	s.wg.Go(s.signalLoop)
	return s
}

// Register registers a new goroutine with the signaller and returns a handle unique to that routine.
func (s *Signaller) Register() *SignalHandle {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := uuid.New().String()
	ch := make(chan struct{}, 1)
	s.out[id] = ch

	return &SignalHandle{
		id:  id,
		in:  s.in,
		out: ch,
	}
}

// Stop stops the signaller from relaying any future signals. This should be called when the signaller is no longer
// needed to prevent a goroutine leak.
func (s *Signaller) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.stopped {
		s.stopFunc()
		s.wg.Wait()
		s.stopped = true
	}
}

// Loop that receives signals and relays them to all other goroutines in the group
func (s *Signaller) signalLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case h := <-s.in:
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()

				for k, v := range s.out {
					if k != h {
						v <- struct{}{}
					}
				}
			}()
		}
	}
}

// SignalHandle is used in a single goroutine to send/receive signals to/from the other registered routines.
type SignalHandle struct {
	id  string
	in  chan string
	out chan struct{}
}

// Signal signals all other goroutines registered with the signaller.
func (h *SignalHandle) Signal() {
	h.in <- h.id
}

// Signalled returns a channel which is received from when a signal has been received from a different routine.
func (h *SignalHandle) Signalled() chan struct{} {
	return h.out
}

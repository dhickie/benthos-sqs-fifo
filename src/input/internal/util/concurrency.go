package util

import (
	"context"
	"errors"
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

	// Immediately exit if ctx is already cancelled
	select {
	case <-ctx.Done():
		c.l.Lock()
		return ctx.Err()
	default:
	}

	select {
	case <-ctx.Done():
		c.l.Lock()
		return ctx.Err()
	case <-c.ch:
		c.l.Lock()
		return nil
	}
}

// Wait2 waits until either a signal is provided to the cond or either provided context is cancelled.
// The thread calling Wait must currently hold the underlying lock before calling this method.
// If the first context is cancelled, its error is returned as the first error. If the second context is cancelled,
// its error is returned as the second error.
func (c *ContextCond) Wait2(ctx1, ctx2 context.Context) (error, error) {
	c.l.Unlock()

	// Immediately exist if either of the contexts are already cancelled
	select {
	case <-ctx1.Done():
		c.l.Lock()
		return ctx1.Err(), nil
	case <-ctx2.Done():
		c.l.Lock()
		return nil, ctx2.Err()
	default:
	}

	select {
	case <-ctx1.Done():
		c.l.Lock()
		return ctx1.Err(), nil
	case <-ctx2.Done():
		c.l.Lock()
		return nil, ctx2.Err()
	case <-c.ch:
		c.l.Lock()
		return nil, nil
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

// Broadcast signals all waiting threads that are waiting on this condition
// When calling this method, it is highly recommended to hold the underlying lock before calling, otherwise the call
// could theoretically never return.
func (c *ContextCond) Broadcast() {
	for {
		select {
		case c.ch <- struct{}{}:
		default:
			return
		}
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

// ------------------------------------------------------------------------------------------------------------------ //

// SyncValue is a threadsafe generic type that allows one thread to wait for a specific value to be set by a different
// thread before retuning to the caller.
type SyncValue[T comparable] struct {
	mu *sync.Mutex
	c  *ContextCond
	v  T
}

// NewSyncValue returns a pointer to a new SyncValue with the provided initial value.
func NewSyncValue[T comparable](vInit T) *SyncValue[T] {
	mu := &sync.Mutex{}
	return &SyncValue[T]{
		mu: mu,
		c:  NewContextCond(mu),
		v:  vInit,
	}
}

// Set the value of the SyncValue to the provided value.
// Any threads waiting for that value will resume execution.
func (s *SyncValue[T]) Set(v T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.v = v
	s.c.Broadcast()
}

// Get returns the current value held in the SyncValue
func (s *SyncValue[T]) Get() T {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.v
}

// WaitForValue blocks until the SyncValue holds the provided value or the context is cancelled
// Returns the value held when this happens.
// Returns an error if the context is cancelled while waiting for the value.
func (s *SyncValue[T]) WaitForValue(ctx context.Context, v T) (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.v != v {
		if err := s.c.Wait(ctx); err != nil {
			return s.v, err
		}
	}

	return s.v, nil
}

// WaitFor waits until the provided function of the value held by the SyncValue returns true, or the provided context
// to be cancelled.
// Returns the value held when this happens.
// Returns an error if the context is cancelled while waiting for the condition to be met.
func (s *SyncValue[T]) WaitFor(ctx context.Context, f func(T) bool) (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for !f(s.v) {
		if err := s.c.Wait(ctx); err != nil {
			return s.v, err
		}
	}

	return s.v, nil
}

// ------------------------------------------------------------------------------------------------------------------ //

// ContextWaitGroup allows WaitGroup semantics while also providing a context to cut a wait short if the context is cancelled.
type ContextWaitGroup struct {
	mu      *sync.Mutex
	v       *SyncValue[int]
	waiting bool
}

// NewContextWaitGroup returns a pointer to a new ContextWaitGroup instance.
func NewContextWaitGroup() *ContextWaitGroup {
	return &ContextWaitGroup{
		mu:      &sync.Mutex{},
		v:       NewSyncValue[int](0),
		waiting: false,
	}
}

// Add adds the provided delta to the wait group's value
// Returns an error if the wait group is already being waited on via a call to Wait()
func (c *ContextWaitGroup) Add(delta int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.waiting {
		return errors.New("cannot add to a wait group that's being waited on")
	}

	c.v.Set(c.v.Get() + delta)
	return nil
}

// Done reduces the wait group's value by one.
// Returns an error if the wait group's value is already at 0.
func (c *ContextWaitGroup) Done() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.v.Get() == 0 {
		return errors.New("cannot mark a wait group as done when there are no pending goroutines")
	}

	c.v.Set(c.v.Get() - 1)
	return nil
}

// Wait waits until either the wait group's value hits 0, or the provided context is cancelled.
// Returns the error from the context if the context is cancelled.
func (c *ContextWaitGroup) Wait(ctx context.Context) error {
	c.mu.Lock()
	c.waiting = true
	c.mu.Unlock()

	if _, err := c.v.WaitForValue(ctx, 0); err != nil {
		return err
	}
	return nil
}

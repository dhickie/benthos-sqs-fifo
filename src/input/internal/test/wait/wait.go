package wait

import (
	"context"
	"time"
)

// Until waits until the value returned by the provided function is true (or the provided context is cancelled)
func Until(ctx context.Context, f func(context.Context) (bool, error), timeout time.Duration) error {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	condCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		for {
			if t, err := f(tCtx); err == nil && t {
				condCh <- struct{}{}
				return
			} else if err != nil {
				errCh <- err
				return
			}

			select {
			case <-tCtx.Done():
				errCh <- tCtx.Err()
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()

	select {
	case <-tCtx.Done():
		return tCtx.Err()
	case err := <-errCh:
		return err
	case <-condCh:
		return nil
	}
}

// For waits for the specified duration to pass (or until the provided context is cancelled)
func For(ctx context.Context, d time.Duration) error {
	ch := time.After(d)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
	}

	return nil
}

package ravendb

import (
	"context"
	"time"
)

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return newIllegalArgumentError("context cannot be nil")
	}
	return ctx.Err()
}

func waitForContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

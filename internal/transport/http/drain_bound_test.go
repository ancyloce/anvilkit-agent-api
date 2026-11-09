package http

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestOnlyTheDrainBoundForcesAStop (RB-A25, DEC-049): a stop is forced only
// when the shutdown's error is the drain bound's own; any other shutdown
// error (a listener that fails to close) is returned as it is, also when the
// bound has passed meanwhile, and never recorded as a forced stop.
func TestOnlyTheDrainBoundForcesAStop(t *testing.T) {
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	live := context.Background()
	closeErr := errors.New("close tcp 127.0.0.1:9100: use of closed network connection")

	for name, c := range map[string]struct {
		ctx  context.Context
		err  error
		want bool
	}{
		"the bound passed with connections open":       {expired, context.DeadlineExceeded, true},
		"the bound's error, wrapped":                   {expired, fmt.Errorf("shutdown: %w", context.DeadlineExceeded), true},
		"the stop was canceled with connections open":  {canceled, context.Canceled, true},
		"a clean drain":                                {live, nil, false},
		"a clean drain that ended as the bound passed": {expired, nil, false},
		"another error within the bound":               {live, closeErr, false},
		"another error as the bound passed":            {expired, closeErr, false},
		"a deadline error of something else":           {live, context.DeadlineExceeded, false},
	} {
		if got := drainBoundReached(c.ctx, c.err); got != c.want {
			t.Errorf("%s: drainBoundReached = %v, want %v", name, got, c.want)
		}
	}
}

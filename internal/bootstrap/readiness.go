package bootstrap

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
)

// readiness answers /readyz (P1.4, API-10): Control's health, cached for
// cacheFor so probes never storm Control, and withdrawn the moment the
// shutdown begins.
type readiness struct {
	health   func(context.Context) error
	cacheFor time.Duration
	stopping atomic.Bool
	mu       sync.Mutex
	checked  time.Time
	last     error
}

func newReadiness(ctl *control.Client) *readiness {
	return &readiness{health: ctl.Health, cacheFor: 5 * time.Second}
}

var errStopping = errors.New("stopping")

func (r *readiness) stop() { r.stopping.Store(true) }

func (r *readiness) check(ctx context.Context) error {
	if r.stopping.Load() {
		return errStopping
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.checked) < r.cacheFor {
		return r.last
	}
	hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	r.last = r.health(hctx)
	r.checked = time.Now()
	return r.last
}

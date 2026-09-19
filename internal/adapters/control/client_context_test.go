package control

import (
	"context"
	"google.golang.org/grpc"
	"testing"
	"time"
)

func TestBoundRPCWaitPreservesIncomingDeadline(t *testing.T) {
	for _, duration := range []time.Duration{0, 50 * time.Millisecond} {
		ctx := context.Background()
		if duration > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, duration)
			defer cancel()
		}
		start := time.Now()
		err := boundRPCWait(ctx, "/GetOperation", nil, nil, nil, func(got context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			deadline, ok := got.Deadline()
			if !ok {
				t.Fatal("no RPC deadline")
			}
			if original, yes := ctx.Deadline(); yes {
				if !deadline.Equal(original) {
					t.Fatal("incoming deadline extended")
				}
			} else if deadline.After(start.Add(15*time.Second + time.Millisecond)) {
				t.Fatal("RPC wait exceeds bound")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Package requestid carries the API's request identifier inward (P3.6,
// API-09): the context value the HTTP middleware sets, the gRPC metadata key
// the owners read, and the client interceptors that send it. The identifier
// is the opaque req_ value of the public error envelope; it names no tenant,
// caller or resource, so it may sit on spans and log lines.
package requestid

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MetadataKey is the gRPC metadata the owners (Control, Knowledge, MCP) read.
const MetadataKey = "x-anvilkit-request-id"

// SpanAttribute is the span attribute that carries the identifier on the
// API's server span and on the owners' server spans of the same trace.
const SpanAttribute = "anvilkit.request_id"

type key struct{}

// With returns ctx carrying the request identifier.
func With(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, key{}, id)
}

// From returns the request identifier ctx carries, or "".
func From(ctx context.Context) string {
	id, _ := ctx.Value(key{}).(string)
	return id
}

func outgoing(ctx context.Context) context.Context {
	if id := From(ctx); id != "" {
		return metadata.AppendToOutgoingContext(ctx, MetadataKey, id)
	}
	return ctx
}

// UnaryClient sends the request identifier with every unary call.
func UnaryClient() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(outgoing(ctx), method, req, reply, cc, opts...)
	}
}

// StreamClient sends the request identifier with every stream.
func StreamClient() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(outgoing(ctx), desc, cc, method, opts...)
	}
}

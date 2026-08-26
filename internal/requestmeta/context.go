package requestmeta

import (
	"context"
	"strings"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
)

type key int

const (
	requestIDKey key = iota
	principalKey
)

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, strings.TrimSpace(requestID))
}

func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}

func WithPrincipal(ctx context.Context, principal auth.Principal) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

func Principal(ctx context.Context) (auth.Principal, bool) {
	value, ok := ctx.Value(principalKey).(auth.Principal)
	return value, ok && value.UserID != ""
}

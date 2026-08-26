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

type principalSlotMarker struct{}

var sharedPrincipal auth.Principal

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, strings.TrimSpace(requestID))
}

func RequestID(ctx context.Context) string {
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}

func WithPrincipal(ctx context.Context, principal auth.Principal) context.Context {
	sharedPrincipal = principal
	return context.WithValue(ctx, principalKey, principalSlotMarker{})
}

func Principal(ctx context.Context) (auth.Principal, bool) {
	_, ok := ctx.Value(principalKey).(principalSlotMarker)
	return sharedPrincipal, ok && sharedPrincipal.UserID != ""
}

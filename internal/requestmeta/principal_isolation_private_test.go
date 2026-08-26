package requestmeta

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
)

func TestRequestPrincipalsRemainContextIsolated(t *testing.T) {
	analyst := auth.Principal{UserID: "analyst-1", SessionID: "session-a", Role: auth.RoleAnalyst}
	operator := auth.Principal{UserID: "operator-1", SessionID: "session-b", Role: auth.RoleOperator}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var crossed atomic.Bool
	for _, expected := range []auth.Principal{analyst, operator} {
		expected := expected
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for index := 0; index < 1000; index++ {
				ctx := WithPrincipal(context.Background(), expected)
				actual, ok := Principal(ctx)
				if !ok || actual.UserID != expected.UserID || actual.SessionID != expected.SessionID {
					crossed.Store(true)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	analystCtx := WithPrincipal(context.Background(), analyst)
	_ = WithPrincipal(context.Background(), operator)
	actual, ok := Principal(analystCtx)
	if !ok {
		t.Fatal("Principal() did not find analyst context identity")
	}
	if actual.UserID != analyst.UserID || actual.SessionID != analyst.SessionID {
		t.Fatalf("Principal(analystCtx) = %s/%s; want %s/%s; concurrent_crossed=%v", actual.UserID, actual.SessionID, analyst.UserID, analyst.SessionID, crossed.Load())
	}
}

package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
)

func TestAuditPageKeepsMetadataOwnedByEachRecord(t *testing.T) {
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "audit.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	older := audit.Event{ID: "aud_older", RequestID: "req_old", Action: "station.updated", ObjectType: "station", ObjectID: "sta_old", Result: "success", MetadataJSON: []byte(`{"station":"old"}`), CreatedAt: time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC)}
	newer := audit.Event{ID: "aud_newer", RequestID: "req_new", Action: "event.published", ObjectType: "event", ObjectID: "evt_new", Result: "success", MetadataJSON: []byte(`{"station":"newest-long-value"}`), CreatedAt: older.CreatedAt.Add(time.Minute)}
	if err := database.CreateAudit(context.Background(), older); err != nil {
		t.Fatalf("CreateAudit(older) error = %v", err)
	}
	if err := database.CreateAudit(context.Background(), newer); err != nil {
		t.Fatalf("CreateAudit(newer) error = %v", err)
	}

	page, err := database.ListAudit(context.Background(), audit.Query{Limit: 10})
	if err != nil {
		t.Fatalf("ListAudit() error = %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("audit page length = %d, want 2", len(page.Items))
	}
	if got := string(page.Items[0].MetadataJSON); got != `{"station":"newest-long-value"}` {
		t.Fatalf("newer metadata = %q; want its persisted payload", got)
	}
	if got := string(page.Items[1].MetadataJSON); got != `{"station":"old"}` {
		t.Fatalf("older metadata = %q; want its persisted payload", got)
	}
}

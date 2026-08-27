package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
	"github.com/vance1852/quakewatch-control-plane/internal/requestmeta"
)

func Audit(ctx context.Context, store repository.AuditStore, ids idgen.Generator, actorID *string, action, objectType, objectID, result string, metadata any, now time.Time) error {
	payload, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal audit metadata: %w", err)
	}
	requestID := requestmeta.RequestID(ctx)
	if requestID == "" {
		requestID = "system"
	}
	value, err := audit.ValidateEvent(audit.Event{
		ID:           ids.New("aud"),
		ActorID:      actorID,
		RequestID:    requestID,
		Action:       action,
		ObjectType:   objectType,
		ObjectID:     objectID,
		Result:       result,
		MetadataJSON: payload,
		CreatedAt:    now,
	})
	if err != nil {
		return err
	}
	return store.CreateAudit(ctx, value)
}

func Actor(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

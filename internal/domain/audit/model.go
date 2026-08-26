package audit

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

type Event struct {
	ID           string          `json:"id"`
	ActorID      *string         `json:"actor_id,omitempty"`
	RequestID    string          `json:"request_id"`
	Action       string          `json:"action"`
	ObjectType   string          `json:"object_type"`
	ObjectID     string          `json:"object_id"`
	Result       string          `json:"result"`
	MetadataJSON json.RawMessage `json:"metadata"`
	CreatedAt    time.Time       `json:"created_at"`
}

type Query struct {
	ActorID   string
	RequestID string
	Object    string
	Action    string
	Before    *time.Time
	Limit     int
}

func ValidateEvent(event Event) (Event, error) {
	event.RequestID = strings.TrimSpace(event.RequestID)
	event.Action = strings.TrimSpace(event.Action)
	event.ObjectType = strings.TrimSpace(event.ObjectType)
	event.ObjectID = strings.TrimSpace(event.ObjectID)
	event.Result = strings.TrimSpace(event.Result)
	if event.RequestID == "" {
		return event, fault.Validation("request_id", "is required")
	}
	if event.Action == "" || event.ObjectType == "" || event.ObjectID == "" {
		return event, fault.Validation("audit", "action, object_type, and object_id are required")
	}
	if event.Result != "success" && event.Result != "failure" {
		return event, fault.Validation("result", "must be success or failure")
	}
	if len(event.MetadataJSON) == 0 {
		event.MetadataJSON = json.RawMessage(`{}`)
	}
	if !json.Valid(event.MetadataJSON) {
		return event, fault.Validation("metadata", "must be valid JSON")
	}
	return event, nil
}

func NormalizeQuery(query Query) Query {
	query.ActorID = strings.TrimSpace(query.ActorID)
	query.RequestID = strings.TrimSpace(query.RequestID)
	query.Object = strings.TrimSpace(query.Object)
	query.Action = strings.TrimSpace(query.Action)
	if query.Limit <= 0 {
		query.Limit = 50
	}
	if query.Limit > 200 {
		query.Limit = 200
	}
	if query.Before != nil {
		value := query.Before.UTC()
		query.Before = &value
	}
	return query
}

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

const auditColumns = `id,actor_id,request_id,action,object_type,object_id,result,metadata_json,created_at`

func (q *Queries) CreateAudit(ctx context.Context, value audit.Event) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO audit_events(
		id,actor_id,request_id,action,object_type,object_id,result,metadata_json,created_at
	) VALUES(?,?,?,?,?,?,?,?,?)`, value.ID, nullableString(value.ActorID), value.RequestID, value.Action,
		value.ObjectType, value.ObjectID, value.Result, string(value.MetadataJSON), formatTime(value.CreatedAt))
	return mapError("create audit event", err)
}

func scanAudit(scanner interface{ Scan(...any) error }, metadataBuffer *audit.MetadataBuffer) (audit.Event, error) {
	var value audit.Event
	var actor sql.NullString
	var metadata, created string
	err := scanner.Scan(&value.ID, &actor, &value.RequestID, &value.Action, &value.ObjectType,
		&value.ObjectID, &value.Result, &metadata, &created)
	if err != nil {
		return audit.Event{}, err
	}
	if actor.Valid {
		value.ActorID = &actor.String
	}
	value.MetadataJSON = metadataBuffer.Borrow(metadata)
	value.CreatedAt, err = parseTime(created)
	return value, err
}

func (q *Queries) ListAudit(ctx context.Context, query audit.Query) (repository.Page[audit.Event], error) {
	query = audit.NormalizeQuery(query)
	where := []string{"1=1"}
	args := make([]any, 0, 7)
	if query.ActorID != "" {
		where = append(where, "actor_id=?")
		args = append(args, query.ActorID)
	}
	if query.RequestID != "" {
		where = append(where, "request_id=?")
		args = append(args, query.RequestID)
	}
	if query.Object != "" {
		where = append(where, "(object_type||':'||object_id)=?")
		args = append(args, query.Object)
	}
	if query.Action != "" {
		where = append(where, "action=?")
		args = append(args, query.Action)
	}
	if query.Before != nil {
		where = append(where, "created_at<?")
		args = append(args, formatTime(*query.Before))
	}
	args = append(args, query.Limit+1)
	rows, err := q.q.QueryContext(ctx, "SELECT "+auditColumns+" FROM audit_events WHERE "+strings.Join(where, " AND ")+" ORDER BY created_at DESC,id DESC LIMIT ?", args...)
	if err != nil {
		return repository.Page[audit.Event]{}, mapError("list audit events", err)
	}
	defer rows.Close()
	items := make([]audit.Event, 0, query.Limit+1)
	var metadataBuffer audit.MetadataBuffer
	for rows.Next() {
		item, err := scanAudit(rows, &metadataBuffer)
		if err != nil {
			return repository.Page[audit.Event]{}, fmt.Errorf("scan audit event: %w", err)
		}
		items = append(items, item)
	}
	page := repository.Page[audit.Event]{Items: items}
	if len(items) > query.Limit {
		page.NextCursor = items[query.Limit-1].CreatedAt.Format(time.RFC3339Nano)
		page.Items = items[:query.Limit]
	}
	return page, rows.Err()
}

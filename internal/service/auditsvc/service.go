package auditsvc

import (
	"context"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type Service struct {
	store repository.AuditStore
}

func New(store repository.AuditStore) *Service { return &Service{store: store} }

func (s *Service) List(ctx context.Context, principal auth.Principal, query audit.Query) (repository.Page[audit.Event], error) {
	if err := principal.Require(auth.PermissionReadAudit); err != nil {
		return repository.Page[audit.Event]{}, err
	}
	return s.store.ListAudit(ctx, audit.NormalizeQuery(query))
}

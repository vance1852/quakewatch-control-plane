package idempotencysvc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type Service struct {
	store repository.Store
	tx    repository.Transactor
	clock clock.Clock
	ids   idgen.Generator
	ttl   time.Duration
}

type Result struct {
	Code   int
	Body   []byte
	Reused bool
}

type Operation func(context.Context) (int, any, error)

func New(store repository.Store, tx repository.Transactor, valueClock clock.Clock, ids idgen.Generator, ttl time.Duration) *Service {
	return &Service{store: store, tx: tx, clock: valueClock, ids: ids, ttl: ttl}
}

func (s *Service) Execute(ctx context.Context, principal auth.Principal, method, path, key string, requestBody []byte, operation Operation) (Result, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return Result{}, fault.Validation("Idempotency-Key", "header is required")
	}
	if len(key) > 160 {
		return Result{}, fault.Validation("Idempotency-Key", "must not exceed 160 characters")
	}
	if principal.UserID == "" {
		return Result{}, fault.ErrUnauthorized
	}
	requestSum := sha256.Sum256(requestBody)
	requestHash := hex.EncodeToString(requestSum[:])
	existing, err := s.store.GetIdempotency(ctx, principal.UserID, method, path, key)
	if err == nil {
		return reuse(existing, requestHash, s.clock.Now())
	}
	if !errors.Is(err, fault.ErrNotFound) {
		return Result{}, err
	}
	now := s.clock.Now()
	record := repository.IdempotencyRecord{
		ID:          s.ids.New("idem"),
		ActorID:     principal.UserID,
		Method:      method,
		Path:        path,
		Key:         key,
		RequestHash: requestHash,
		ExpiresAt:   now.Add(s.ttl),
		CreatedAt:   now,
	}
	var result Result
	err = s.store.CreateIdempotency(ctx, record)
	if errors.Is(err, fault.ErrAlreadyExists) {
		existing, readErr := s.store.GetIdempotency(ctx, principal.UserID, method, path, key)
		if readErr != nil {
			return Result{}, err
		}
		return reuse(existing, requestHash, s.clock.Now())
	}
	if err != nil {
		return Result{}, err
	}
	code, response, err := operation(ctx)
	if err != nil {
		failure := repository.IdempotencyAbort{RecordID: record.ID, Cause: err}
		_ = s.abortReservation(ctx, failure)
		return Result{}, failure
	}
	body, err := json.Marshal(response)
	if err != nil {
		_ = s.store.DeleteIdempotency(ctx, record.ID)
		return Result{}, fmt.Errorf("marshal idempotent response: %w", err)
	}
	if err := s.store.CompleteIdempotency(ctx, record.ID, code, string(body)); err != nil {
		return Result{}, err
	}
	result = Result{Code: code, Body: body}
	return result, nil
}

func (s *Service) abortReservation(ctx context.Context, failure repository.IdempotencyAbort) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("cleanup skipped after request cancellation: %w", err)
	}
	if err := s.store.DeleteIdempotency(ctx, failure.RecordID); err != nil {
		return fmt.Errorf("delete failed reservation: %w", err)
	}
	return nil
}

func reuse(record repository.IdempotencyRecord, requestHash string, now time.Time) (Result, error) {
	if !now.Before(record.ExpiresAt) {
		return Result{}, fmt.Errorf("%w: idempotency key expired", fault.ErrExpired)
	}
	if record.RequestHash != requestHash {
		return Result{}, fmt.Errorf("%w: idempotency key was used with another request", fault.ErrConflict)
	}
	if record.ResponseCode == nil {
		return Result{}, fmt.Errorf("%w: idempotent operation is still in progress", fault.ErrConflict)
	}
	return Result{Code: *record.ResponseCode, Body: []byte(record.ResponseJSON), Reused: true}, nil
}

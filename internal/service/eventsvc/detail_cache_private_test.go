package eventsvc

import (
	"context"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type isolatedPickStore struct {
	repository.Store
	value     event.Candidate
	persisted []event.Pick
	listCalls int
}

func (s *isolatedPickStore) GetEvent(context.Context, string) (event.Candidate, error) {
	return s.value, nil
}
func (s *isolatedPickStore) ListPicks(context.Context, string) ([]event.Pick, error) {
	s.listCalls++
	return append([]event.Pick(nil), s.persisted...), nil
}

func TestEventDetailCacheDoesNotExposeSharedPickSlice(t *testing.T) {
	now := time.Date(2026, 8, 27, 11, 0, 0, 0, time.UTC)
	store := &isolatedPickStore{
		value:     event.Candidate{ID: "evt_cache", PublicID: "evt-public-11", Status: event.StatusDetected},
		persisted: []event.Pick{{ID: "pick_1", EventID: "evt_cache", StationID: "sta_persisted", WaveformID: "wav_1", Phase: event.PhaseP}},
	}
	service := New(store, nil, clock.NewFake(now), &idgen.Sequence{}, time.Minute)
	first, err := service.Get(context.Background(), "evt_cache")
	if err != nil {
		t.Fatalf("first event detail: %v", err)
	}
	first.Picks[0].StationID = "sta_client_override"
	second, err := service.Get(context.Background(), "evt_cache")
	if err != nil {
		t.Fatalf("second event detail: %v", err)
	}
	if second.Picks[0].StationID != "sta_persisted" {
		t.Fatalf("second event station = %q; want persisted station after caller mutation", second.Picks[0].StationID)
	}
	if store.persisted[0].StationID != "sta_persisted" {
		t.Fatalf("persisted pick was modified: %+v", store.persisted[0])
	}
}

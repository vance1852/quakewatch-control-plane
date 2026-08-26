package waveformsvc

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
)

func TestConcurrentIdempotencyFingerprintsRemainRequestLocal(t *testing.T) {
	service := &Service{fingerprints: waveform.NewFingerprintBuffer()}
	start := make(chan struct{})
	var mismatches atomic.Int64
	var wait sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			for attempt := 0; attempt < 25; attempt++ {
				startsAt := time.Unix(int64(1_700_000_000+worker), int64(attempt)).UTC()
				input := waveform.IngestInput{
					StationID: fmt.Sprintf("sta-%02d", worker), SensorID: fmt.Sprintf("sen-%02d", worker),
					SourceKey: fmt.Sprintf("source-%02d-%03d", worker, attempt), StartsAt: startsAt,
					EndsAt: startsAt.Add(time.Second), SampleCount: int64(100 + worker),
					Checksum: fmt.Sprintf("%064x", worker+1),
				}
				existing := waveform.Batch{
					StationID: input.StationID, SensorID: input.SensorID, SourceKey: input.SourceKey,
					StartsAt: input.StartsAt, EndsAt: input.EndsAt, SampleCount: input.SampleCount, Checksum: input.Checksum,
				}
				if !service.sameWaveform(existing, input) {
					mismatches.Add(1)
				}
			}
		}()
	}
	close(start)
	wait.Wait()
	if mismatches.Load() != 0 {
		t.Fatalf("matching idempotency requests reported %d fingerprint mismatches; want 0", mismatches.Load())
	}
}

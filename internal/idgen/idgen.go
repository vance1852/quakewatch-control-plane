package idgen

import (
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type Generator interface {
	New(prefix string) string
}

type Random struct{}

func (Random) New(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(fmt.Sprintf("secure random identifier: %v", err))
	}
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(bytes[:])
	return strings.ToLower(prefix + "_" + encoded)
}

type Sequence struct {
	next atomic.Uint64
}

func (s *Sequence) New(prefix string) string {
	value := s.next.Add(1)
	return fmt.Sprintf("%s_%d_%06d", prefix, time.Now().UTC().Unix(), value)
}

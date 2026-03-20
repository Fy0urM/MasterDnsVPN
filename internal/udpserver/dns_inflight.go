// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package udpserver

import (
	"time"

	"masterdnsvpn-go/internal/inflight"
)

type dnsResolveInflightEntry = inflight.Entry[[]byte]

type dnsResolveInflightManager struct {
	inner *inflight.Manager[[]byte]
}

func newDNSResolveInflightManager(timeout time.Duration) *dnsResolveInflightManager {
	return &dnsResolveInflightManager{
		inner: inflight.New(timeout, 16*time.Second, cloneInflightBytes),
	}
}

func (m *dnsResolveInflightManager) Acquire(cacheKey string, now time.Time) (*dnsResolveInflightEntry, bool) {
	if m == nil {
		return nil, false
	}
	return m.inner.Acquire(cacheKey, now)
}

func (m *dnsResolveInflightManager) Resolve(cacheKey string, response []byte) {
	if m == nil {
		return
	}
	m.inner.Resolve(cacheKey, response, len(response) != 0)
}

func (m *dnsResolveInflightManager) Wait(entry *dnsResolveInflightEntry, timeout time.Duration) ([]byte, bool) {
	if m == nil {
		return nil, false
	}
	return m.inner.Wait(entry, timeout)
}

func cloneInflightBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	return append([]byte(nil), value...)
}

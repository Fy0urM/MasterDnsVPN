package client

import (
	"sort"
	"sync"
	"time"
)

type streamRouteState struct {
	PreferredResolverKey string
	ResolverResendStreak int
	LastFailoverAt       time.Time
	CachedPlan           []Connection
	CachedPlanFor        string
	CachedPlanSize       int
	CachedPlanVersion    uint64
}

type resolverHealthEvent struct {
	At time.Time
}

type resolverHealthState struct {
	Events           []resolverHealthEvent
	TimeoutOnlySince time.Time
	LastSuccessAt    time.Time
}

type resolverRecheckState struct{}

type resolverDisabledState struct{}

// ResolverRuntime is the owner for resolver catalog and runtime resolver state.
type ResolverRuntime struct {
	mu         sync.RWMutex
	byKey      map[string]Connection
	ordered    []Connection
	keyToIndex map[string]int
	balancer   *Balancer

	healthMu          sync.RWMutex
	health            map[string]*resolverHealthState
	recheck           map[string]resolverRecheckState
	runtimeDisabled   map[string]resolverDisabledState
	recheckSem        chan struct{}
	streamMu          sync.RWMutex
	streamRoutes      map[uint16]*streamRouteState
	failoverThreshold int
	failoverCooldown  time.Duration
}

func NewResolverRuntime(balancer *Balancer, recheckBatchSize int, failoverThreshold int, failoverCooldown time.Duration) *ResolverRuntime {
	if recheckBatchSize < 1 {
		recheckBatchSize = 1
	}
	if failoverThreshold < 1 {
		failoverThreshold = 1
	}
	if failoverCooldown <= 0 {
		failoverCooldown = time.Second
	}
	return &ResolverRuntime{
		byKey:             make(map[string]Connection),
		keyToIndex:        make(map[string]int),
		balancer:          balancer,
		health:            make(map[string]*resolverHealthState),
		recheck:           make(map[string]resolverRecheckState),
		runtimeDisabled:   make(map[string]resolverDisabledState),
		recheckSem:        make(chan struct{}, recheckBatchSize),
		streamRoutes:      make(map[uint16]*streamRouteState),
		failoverThreshold: failoverThreshold,
		failoverCooldown:  failoverCooldown,
	}
}

func (r *ResolverRuntime) LoadConnections(connections []Connection) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.ordered = append(r.ordered[:0], connections...)
	r.byKey = make(map[string]Connection, len(connections))
	r.keyToIndex = make(map[string]int, len(connections))

	for idx, conn := range connections {
		if conn.Key == "" {
			continue
		}
		r.byKey[conn.Key] = conn
		r.keyToIndex[conn.Key] = idx
	}

}

func (r *ResolverRuntime) RefreshFromConnections(connections []Connection) {
	r.LoadConnections(connections)
}

func (r *ResolverRuntime) GetConnectionByKey(key string) (Connection, bool) {
	if r == nil || key == "" {
		return Connection{}, false
	}

	if r.balancer != nil {
		if conn, ok := r.balancer.GetConnectionByKey(key); ok {
			r.mu.Lock()
			r.byKey[key] = conn
			if idx, ok := r.keyToIndex[key]; ok && idx >= 0 && idx < len(r.ordered) {
				r.ordered[idx] = conn
			}
			r.mu.Unlock()
			return conn, true
		}
	}

	r.mu.RLock()
	conn, ok := r.byKey[key]
	r.mu.RUnlock()
	if ok {
		return conn, true
	}
	return Connection{}, false
}

func (r *ResolverRuntime) SetConnectionValidity(key string, valid bool) bool {
	if r == nil || key == "" {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.byKey[key]
	if !ok {
		return false
	}
	conn.IsValid = valid
	r.byKey[key] = conn
	if idx, ok := r.keyToIndex[key]; ok && idx >= 0 && idx < len(r.ordered) {
		r.ordered[idx] = conn
	}

	if r.balancer != nil {
		return r.balancer.SetConnectionValidity(key, valid)
	}
	return true
}

func (r *ResolverRuntime) SetConnectionMTU(key string, uploadBytes int, uploadChars int, downloadBytes int) bool {
	if r == nil || key == "" {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.byKey[key]
	if !ok {
		return false
	}
	conn.UploadMTUBytes = uploadBytes
	conn.UploadMTUChars = uploadChars
	conn.DownloadMTUBytes = downloadBytes
	r.byKey[key] = conn
	if idx, ok := r.keyToIndex[key]; ok && idx >= 0 && idx < len(r.ordered) {
		r.ordered[idx] = conn
	}

	if r.balancer != nil {
		return r.balancer.SetConnectionMTU(key, uploadBytes, uploadChars, downloadBytes)
	}
	return true
}

func (r *ResolverRuntime) Connections() []Connection {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	copied := make([]Connection, len(r.ordered))
	copy(copied, r.ordered)
	return copied
}

func (r *ResolverRuntime) routeState(streamID uint16) *streamRouteState {
	if r == nil || streamID == 0 {
		return nil
	}
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	state := r.streamRoutes[streamID]
	if state == nil {
		state = &streamRouteState{}
		r.streamRoutes[streamID] = state
	}
	return state
}

func (r *ResolverRuntime) cleanupStream(streamID uint16) {
	if r == nil || streamID == 0 {
		return
	}
	if r.balancer != nil {
		r.balancer.CleanupStream(streamID)
	}
	r.streamMu.Lock()
	delete(r.streamRoutes, streamID)
	r.streamMu.Unlock()
}

func (r *ResolverRuntime) SelectTargetsForPacket(c *Client, packetType uint8, streamID uint16) ([]Connection, error) {
	targetCount := 1
	if c != nil {
		targetCount = c.runtimePacketDuplicationCount(packetType)
	}
	return r.SelectTargetsForPacketCount(c, packetType, streamID, targetCount)
}

func (r *ResolverRuntime) SelectTargetsForPacketCount(_ *Client, packetType uint8, streamID uint16, targetCount int) ([]Connection, error) {
	if r == nil || r.balancer == nil {
		return nil, ErrNoValidConnections
	}
	return r.balancer.SelectTargets(packetType, streamID, targetCount)
}

func (r *ResolverRuntime) noteStreamProgress(streamID uint16) {
	if r == nil || r.balancer == nil {
		return
	}
	r.balancer.NoteStreamProgress(streamID)
}

func (r *ResolverRuntime) ensureStreamPreferredConnection(_ *Client, streamID uint16) (Connection, bool) {
	if r == nil || r.balancer == nil || streamID == 0 {
		return Connection{}, false
	}
	r.balancer.EnsureStream(streamID)
	selected, err := r.balancer.SelectTargets(0, streamID, 1)
	if err != nil || len(selected) == 0 {
		return Connection{}, false
	}
	return selected[0], true
}

func (r *ResolverRuntime) NoteSend(serverKey string) {
	if r == nil || serverKey == "" || r.balancer == nil {
		return
	}
	r.balancer.ReportSend(serverKey)
}

func (r *ResolverRuntime) NoteSuccess(serverKey string, rtt time.Duration, now time.Time, healthWindow time.Duration) {
	if r == nil || serverKey == "" || r.balancer == nil {
		return
	}
	if rtt < 0 {
		rtt = 0
	}
	r.balancer.ReportSuccess(serverKey, rtt)
	r.RecordHealthEvent(serverKey, true, now, healthWindow)
}

func (r *ResolverRuntime) NoteFailure(serverKey string, at time.Time, healthWindow time.Duration) {
	if r == nil || serverKey == "" {
		return
	}
	r.RecordHealthEvent(serverKey, false, at, healthWindow)
}

func (r *ResolverRuntime) NoteTimeout(serverKey string, at time.Time, healthWindow time.Duration) {
	if r == nil || serverKey == "" {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	r.RecordHealthEvent(serverKey, false, at, healthWindow)
}

func (r *ResolverRuntime) RecordHealthEvent(serverKey string, success bool, now time.Time, healthWindow time.Duration) {
	if r == nil || serverKey == "" {
		return
	}
	conn, ok := r.GetConnectionByKey(serverKey)
	if !ok || !conn.IsValid {
		return
	}

	r.healthMu.Lock()
	defer r.healthMu.Unlock()

	state := r.health[serverKey]
	if state == nil {
		state = &resolverHealthState{Events: make([]resolverHealthEvent, 0, 8)}
		r.health[serverKey] = state
	}
	if success {
		state.Events = state.Events[:0]
		state.TimeoutOnlySince = time.Time{}
		state.LastSuccessAt = now
		return
	}

	r.insertHealthEventLocked(state, resolverHealthEvent{At: now})
	if state.TimeoutOnlySince.IsZero() || now.Before(state.TimeoutOnlySince) {
		state.TimeoutOnlySince = now
	}
	r.pruneHealthLocked(state, now, healthWindow)
	if len(state.Events) == 0 {
		state.TimeoutOnlySince = time.Time{}
	} else if state.TimeoutOnlySince.IsZero() {
		state.TimeoutOnlySince = state.Events[0].At
	}
}

func (r *ResolverRuntime) RetractTimeoutEvent(serverKey string, timedOutAt time.Time, now time.Time, healthWindow time.Duration) {
	if r == nil || serverKey == "" || timedOutAt.IsZero() {
		return
	}

	r.healthMu.Lock()
	defer r.healthMu.Unlock()

	state := r.health[serverKey]
	if state == nil || len(state.Events) == 0 {
		return
	}

	removeIdx := -1
	for i, event := range state.Events {
		if event.At.Equal(timedOutAt) {
			removeIdx = i
			break
		}
	}
	if removeIdx == -1 {
		return
	}

	state.Events = append(state.Events[:removeIdx], state.Events[removeIdx+1:]...)
	r.pruneHealthLocked(state, now, healthWindow)
	if len(state.Events) == 0 {
		state.TimeoutOnlySince = time.Time{}
	} else {
		state.TimeoutOnlySince = state.Events[0].At
	}
}

func (r *ResolverRuntime) insertHealthEventLocked(state *resolverHealthState, event resolverHealthEvent) {
	if state == nil {
		return
	}
	n := len(state.Events)
	if n == 0 || !event.At.Before(state.Events[n-1].At) {
		state.Events = append(state.Events, event)
		return
	}

	insertAt := sort.Search(n, func(i int) bool {
		return !state.Events[i].At.Before(event.At)
	})
	state.Events = append(state.Events, resolverHealthEvent{})
	copy(state.Events[insertAt+1:], state.Events[insertAt:])
	state.Events[insertAt] = event
}

func (r *ResolverRuntime) pruneHealthLocked(state *resolverHealthState, now time.Time, healthWindow time.Duration) {
	if state == nil || len(state.Events) == 0 {
		return
	}
	if healthWindow <= 0 {
		return
	}
	cutoff := now.Add(-healthWindow)
	dropCount := 0
	for dropCount < len(state.Events) && state.Events[dropCount].At.Before(cutoff) {
		dropCount++
	}
	if dropCount == 0 {
		return
	}
	state.Events = append(state.Events[:0], state.Events[dropCount:]...)
}

func (c *Client) GetConnectionByKey(key string) (Connection, bool) {
	if c == nil || key == "" {
		return Connection{}, false
	}
	if c.runtime != nil {
		return c.runtime.GetConnectionByKey(key)
	}
	if c.balancer != nil {
		return c.balancer.GetConnectionByKey(key)
	}
	return Connection{}, false
}

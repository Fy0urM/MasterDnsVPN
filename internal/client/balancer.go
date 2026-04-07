// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================
// Package client provides the core logic for the MasterDnsVPN client.
// This file (balancer.go) handles connection balancing strategies.
// ==============================================================================
package client

import (
	"encoding/binary"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	Enums "masterdnsvpn-go/internal/enums"
)

const (
	BalancingRoundRobinDefault = 0
	BalancingRandom            = 1
	BalancingRoundRobin        = 2
	BalancingLeastLoss         = 3
	BalancingLowestLatency     = 4
)

type Connection struct {
	Domain            string
	Resolver          string
	ResolverPort      int
	ResolverLabel     string
	Key               string
	IsValid           bool
	UploadMTUBytes    int
	UploadMTUChars    int
	DownloadMTUBytes  int
	MTUResolveTime    time.Duration
	LastHealthCheckAt time.Time
	WindowStartedAt   time.Time
	WindowSent        uint32
	WindowTimedOut    uint32
}

type balancerStreamRouteState struct {
	PreferredResolverKey string
	ResendStreak         int
	LastFailoverAt       time.Time
	CachedTargetKeys     []string
	CachedRequiredCount  int
}

type balancerResolverSampleKey struct {
	resolverAddr string
	localAddr    string
	dnsID        uint16
}

type balancerResolverSample struct {
	serverKey  string
	sentAt     time.Time
	timedOut   bool
	timedOutAt time.Time
	evictAfter time.Time
}

type balancerTimeoutObservation struct {
	serverKey string
	at        time.Time
}

type Balancer struct {
	strategy        int
	rrCounter       atomic.Uint64
	healthRRCounter atomic.Uint64
	rngState        atomic.Uint64

	mu           sync.RWMutex
	connections  []Connection
	indexByKey   map[string]int
	activeIDs    []int
	activePos    map[int]int
	inactiveIDs  []int
	inactivePos  map[int]int
	stats        []*connectionStats
	streamRoutes map[uint16]*balancerStreamRouteState
	pending      map[balancerResolverSampleKey]balancerResolverSample

	streamFailoverThreshold int
	streamFailoverCooldown  time.Duration
}

type connectionStats struct {
	mu           sync.RWMutex
	sent         uint64
	acked        uint64
	rttMicrosSum uint64
	rttCount     uint64
}

const connectionStatsHalfLifeThreshold = 1000

func NewBalancer(strategy int) *Balancer {
	b := &Balancer{
		strategy:                strategy,
		streamRoutes:            make(map[uint16]*balancerStreamRouteState),
		pending:                 make(map[balancerResolverSampleKey]balancerResolverSample),
		streamFailoverThreshold: 1,
		streamFailoverCooldown:  time.Second,
	}
	b.rngState.Store(seedRNG())
	return b
}

func (b *Balancer) SetStreamFailoverConfig(threshold int, cooldown time.Duration) {
	if b == nil {
		return
	}
	if threshold < 1 {
		threshold = 1
	}
	if cooldown <= 0 {
		cooldown = time.Second
	}

	b.mu.Lock()
	b.streamFailoverThreshold = threshold
	b.streamFailoverCooldown = cooldown
	b.mu.Unlock()
}

func (b *Balancer) SetConnections(connections []*Connection) {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := len(connections)
	b.connections = make([]Connection, 0, size)
	b.indexByKey = make(map[string]int, size)
	b.activeIDs = make([]int, 0, size)
	b.activePos = make(map[int]int, size)
	b.inactiveIDs = make([]int, 0, size)
	b.inactivePos = make(map[int]int, size)
	b.stats = make([]*connectionStats, 0, size)
	if b.pending == nil {
		b.pending = make(map[balancerResolverSampleKey]balancerResolverSample)
	} else {
		clear(b.pending)
	}
	if b.streamRoutes == nil {
		b.streamRoutes = make(map[uint16]*balancerStreamRouteState)
	} else {
		clear(b.streamRoutes)
	}

	for _, conn := range connections {
		if conn == nil || conn.Key == "" {
			continue
		}
		copied := *conn
		copied.IsValid = false
		copied.UploadMTUBytes = 0
		copied.UploadMTUChars = 0
		copied.DownloadMTUBytes = 0
		copied.MTUResolveTime = 0
		copied.LastHealthCheckAt = time.Time{}
		copied.WindowStartedAt = time.Time{}
		copied.WindowSent = 0
		copied.WindowTimedOut = 0

		idx := len(b.connections)
		b.connections = append(b.connections, copied)
		b.indexByKey[copied.Key] = idx
		b.inactivePos[idx] = len(b.inactiveIDs)
		b.inactiveIDs = append(b.inactiveIDs, idx)
		b.stats = append(b.stats, &connectionStats{})
	}

}

func (b *Balancer) ActiveCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.activeIDs)
}

func (b *Balancer) TotalCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.connections)
}

func (b *Balancer) GetConnectionByKey(key string) (Connection, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return Connection{}, false
	}
	return b.connections[idx], true
}

func (b *Balancer) SetConnectionValidity(key string, valid bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return false
	}
	if b.connections[idx].IsValid == valid {
		return true
	}

	b.connections[idx].IsValid = valid
	if valid {
		b.resetWindowLocked(&b.connections[idx])
	} else {
		b.clearPreferredResolverReferencesLocked(key)
	}
	b.moveConnectionStateLocked(idx, valid)
	return true
}

func (b *Balancer) SetConnectionMTU(key string, uploadBytes int, uploadChars int, downloadBytes int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return false
	}

	b.connections[idx].UploadMTUBytes = uploadBytes
	b.connections[idx].UploadMTUChars = uploadChars
	b.connections[idx].DownloadMTUBytes = downloadBytes
	return true
}

func (b *Balancer) ApplyMTUProbeResult(key string, uploadBytes int, uploadChars int, downloadBytes int, resolveTime time.Duration, active bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return false
	}

	conn := &b.connections[idx]
	conn.UploadMTUBytes = uploadBytes
	conn.UploadMTUChars = uploadChars
	conn.DownloadMTUBytes = downloadBytes
	conn.MTUResolveTime = resolveTime
	wasValid := conn.IsValid
	conn.IsValid = active
	if active {
		b.resetWindowLocked(conn)
	} else {
		b.clearPreferredResolverReferencesLocked(key)
	}
	if wasValid != active {
		b.moveConnectionStateLocked(idx, active)
	}
	return true
}

func (b *Balancer) ReportSend(serverKey string) {
	if stats := b.statsForKey(serverKey); stats != nil {
		stats.mu.Lock()
		stats.sent++
		stats.applyHalfLifeLocked()
		stats.mu.Unlock()
	}
}

func (b *Balancer) ReportSuccess(serverKey string, rtt time.Duration) {
	stats := b.statsForKey(serverKey)
	if stats == nil {
		return
	}

	stats.mu.Lock()
	stats.acked++
	if rtt > 0 {
		stats.rttMicrosSum += uint64(rtt / time.Microsecond)
		stats.rttCount++
	}
	stats.applyHalfLifeLocked()
	stats.mu.Unlock()
}

func (b *Balancer) ReportTimeoutWindow(serverKey string, now time.Time, window time.Duration, minObservations int, minActive int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	conn, ok := b.connectionByKeyLocked(serverKey)
	if !ok || !conn.IsValid {
		return false
	}

	b.ensureWindowLocked(conn, now, window)
	conn.WindowTimedOut++

	if minObservations < 1 {
		minObservations = 1
	}

	if minActive < 0 {
		minActive = 0
	}
	if minActive < 3 {
		minActive = 3
	}

	if int(conn.WindowSent) < minObservations {
		return false
	}

	if conn.WindowTimedOut != conn.WindowSent {
		return false
	}

	if len(b.activeIDs) <= minActive {
		return false
	}

	conn.IsValid = false
	b.resetWindowLocked(conn)
	b.clearPreferredResolverReferencesLocked(serverKey)
	if idx, ok := b.indexByKey[serverKey]; ok {
		b.moveConnectionStateLocked(idx, false)
	}
	return true
}

func (b *Balancer) RetractTimeoutWindow(serverKey string, now time.Time, window time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	conn, ok := b.connectionByKeyLocked(serverKey)
	if !ok {
		return false
	}

	b.ensureWindowLocked(conn, now, window)
	if conn.WindowTimedOut > 0 {
		conn.WindowTimedOut--
	}
	return true
}

func (b *Balancer) TrackResolverSend(
	packet []byte,
	resolverAddr string,
	localAddr string,
	serverKey string,
	sentAt time.Time,
	tunnelPacketTimeout time.Duration,
	checkInterval time.Duration,
	window time.Duration,
) {
	if b == nil || len(packet) < 2 || resolverAddr == "" || serverKey == "" {
		return
	}

	key := balancerResolverSampleKey{
		resolverAddr: resolverAddr,
		localAddr:    localAddr,
		dnsID:        binary.BigEndian.Uint16(packet[:2]),
	}

	requestTimeout := resolverRequestTimeout(tunnelPacketTimeout, checkInterval, window)
	ttl := resolverSampleTTL(tunnelPacketTimeout)
	var timeoutObservations []balancerTimeoutObservation

	b.mu.Lock()
	if len(b.pending) >= resolverPendingSoftCap {
		timeoutObservations = b.prunePendingLocked(sentAt, requestTimeout, ttl)
		if overflow := len(b.pending) - resolverPendingHardCap; overflow >= 0 {
			b.evictPendingLocked(overflow + 1)
		}
	}
	b.pending[key] = balancerResolverSample{
		serverKey: serverKey,
		sentAt:    sentAt,
	}
	b.mu.Unlock()

	for _, observation := range timeoutObservations {
		b.ReportTimeoutWindow(observation.serverKey, observation.at, window, 1, 1)
	}

	b.ReportSend(serverKey)
	b.mu.Lock()
	if conn, ok := b.connectionByKeyLocked(serverKey); ok && conn.IsValid {
		b.ensureWindowLocked(conn, sentAt, window)
		conn.WindowSent++
	}
	b.mu.Unlock()
}

func (b *Balancer) TrackResolverSuccess(
	packet []byte,
	addr *net.UDPAddr,
	localAddr string,
	receivedAt time.Time,
	window time.Duration,
	rtt time.Duration,
) {
	if b == nil || len(packet) < 2 || addr == nil {
		return
	}

	key := balancerResolverSampleKey{
		resolverAddr: addr.String(),
		localAddr:    localAddr,
		dnsID:        binary.BigEndian.Uint16(packet[:2]),
	}

	b.mu.Lock()
	sample, ok := b.pending[key]
	if ok {
		delete(b.pending, key)
	}
	b.mu.Unlock()

	if !ok || sample.serverKey == "" {
		return
	}
	if sample.timedOut && !sample.timedOutAt.IsZero() {
		b.RetractTimeoutWindow(sample.serverKey, receivedAt, window)
	}
	if !sample.sentAt.IsZero() && !receivedAt.Before(sample.sentAt) {
		rtt = receivedAt.Sub(sample.sentAt)
	}
	if rtt > 0 {
		b.ReportSuccess(sample.serverKey, rtt)
	}
}

func (b *Balancer) TrackResolverFailure(
	packet []byte,
	addr *net.UDPAddr,
	localAddr string,
	failedAt time.Time,
	window time.Duration,
	minObservations int,
	autoDisable bool,
) {
	if b == nil || len(packet) < 2 || addr == nil {
		return
	}

	key := balancerResolverSampleKey{
		resolverAddr: addr.String(),
		localAddr:    localAddr,
		dnsID:        binary.BigEndian.Uint16(packet[:2]),
	}

	b.mu.Lock()
	sample, ok := b.pending[key]
	if ok {
		delete(b.pending, key)
	}
	b.mu.Unlock()

	if !ok || sample.serverKey == "" || sample.timedOut || !autoDisable {
		return
	}
	b.ReportTimeoutWindow(sample.serverKey, failedAt, window, minObservations, 1)
}

func (b *Balancer) CollectExpiredResolverTimeouts(
	now time.Time,
	tunnelPacketTimeout time.Duration,
	checkInterval time.Duration,
	window time.Duration,
	minObservations int,
	autoDisable bool,
) {
	if b == nil || !autoDisable {
		return
	}

	requestTimeout := resolverRequestTimeout(tunnelPacketTimeout, checkInterval, window)
	ttl := resolverSampleTTL(tunnelPacketTimeout)

	b.mu.Lock()
	timeoutObservations := b.prunePendingLocked(now, requestTimeout, ttl)
	b.mu.Unlock()

	for _, observation := range timeoutObservations {
		b.ReportTimeoutWindow(observation.serverKey, observation.at, window, minObservations, 1)
	}
}

func (b *Balancer) ResetServerStats(serverKey string) {
	stats := b.statsForKey(serverKey)
	if stats == nil {
		return
	}

	stats.mu.Lock()
	stats.sent = 0
	stats.acked = 0
	stats.rttMicrosSum = 0
	stats.rttCount = 0
	stats.mu.Unlock()
}

func (b *Balancer) SeedConservativeStats(serverKey string) {
	stats := b.statsForKey(serverKey)
	if stats == nil {
		return
	}

	stats.mu.Lock()
	stats.sent = 10
	stats.acked = 8
	stats.rttMicrosSum = 0
	stats.rttCount = 0
	stats.mu.Unlock()
}

func (b *Balancer) GetBestConnection() (Connection, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.activeIDs) == 0 {
		return Connection{}, false
	}

	switch b.strategy {
	case BalancingRandom:
		idx := b.activeIDs[b.nextRandom()%uint64(len(b.activeIDs))]
		return b.connections[idx], true
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.roundRobinBestConnectionLocked()
		}
		return b.bestScoredConnectionLocked(b.lossScoreLocked)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.roundRobinBestConnectionLocked()
		}
		return b.bestScoredConnectionLocked(b.latencyScoreLocked)
	default:
		return b.roundRobinBestConnectionLocked()
	}
}

func (b *Balancer) GetBestConnectionExcluding(excludeKey string) (Connection, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.activeIDs) == 0 {
		return Connection{}, false
	}

	switch b.strategy {
	case BalancingRandom:
		ordered := b.rotatedActiveIndicesLocked(1)
		for _, idx := range ordered {
			if b.connections[idx].Key == excludeKey {
				continue
			}
			return b.connections[idx], true
		}
		return Connection{}, false
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.roundRobinBestConnectionExcludingLocked(excludeKey)
		}
		return b.bestScoredConnectionExcludingLocked(b.lossScoreLocked, excludeKey)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.roundRobinBestConnectionExcludingLocked(excludeKey)
		}
		return b.bestScoredConnectionExcludingLocked(b.latencyScoreLocked, excludeKey)
	default:
		return b.roundRobinBestConnectionExcludingLocked(excludeKey)
	}
}

func (b *Balancer) GetUniqueConnections(requiredCount int) []Connection {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := normalizeRequiredCount(len(b.activeIDs), requiredCount, 1)
	if count <= 0 {
		return nil
	}

	if count == 1 {
		best, ok := b.getBestConnectionLocked()
		if !ok {
			return nil
		}
		return []Connection{best}
	}

	switch b.strategy {
	case BalancingRandom:
		return b.selectRandomLocked(count)
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.selectRoundRobinLocked(count)
		}
		return b.selectLowestScoreLocked(count, b.lossScoreLocked)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.selectRoundRobinLocked(count)
		}
		return b.selectLowestScoreLocked(count, b.latencyScoreLocked)
	default:
		return b.selectRoundRobinLocked(count)
	}
}

func (b *Balancer) ActiveConnections() []Connection {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.connectionsByIDsLocked(b.activeIDs)
}

func (b *Balancer) InactiveConnections() []Connection {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.connectionsByIDsLocked(b.inactiveIDs)
}

func (b *Balancer) AllConnections() []Connection {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]Connection, len(b.connections))
	copy(result, b.connections)
	return result
}

func (b *Balancer) NextInactiveConnectionForHealthCheck(now time.Time, minInterval time.Duration) (Connection, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(b.inactiveIDs)
	if n == 0 {
		return Connection{}, false
	}

	if minInterval < 0 {
		minInterval = 0
	}

	start := roundRobinStartIndex(b.healthRRCounter.Add(1)-1, n)
	for i := 0; i < n; i++ {
		idx := b.inactiveIDs[(start+i)%n]
		if idx < 0 || idx >= len(b.connections) {
			continue
		}

		conn := &b.connections[idx]
		if !conn.LastHealthCheckAt.IsZero() && now.Sub(conn.LastHealthCheckAt) < minInterval {
			continue
		}

		conn.LastHealthCheckAt = now
		return *conn, true
	}

	return Connection{}, false
}

func (b *Balancer) EnsureStream(streamID uint16) {
	if b == nil || streamID == 0 {
		return
	}

	b.mu.Lock()
	b.ensureStreamRouteLocked(streamID)
	b.mu.Unlock()
}

func (b *Balancer) CleanupStream(streamID uint16) {
	if b == nil || streamID == 0 {
		return
	}

	b.mu.Lock()
	delete(b.streamRoutes, streamID)
	b.mu.Unlock()
}

func (b *Balancer) NoteStreamProgress(streamID uint16) {
	if b == nil || streamID == 0 {
		return
	}

	b.mu.Lock()
	if state, ok := b.streamRoutes[streamID]; ok && state != nil {
		state.ResendStreak = 0
	}
	b.mu.Unlock()
}

func (b *Balancer) SelectTargets(packetType uint8, streamID uint16, requiredCount int) ([]Connection, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	requiredCount = normalizeRequiredCount(len(b.activeIDs), requiredCount, 1)
	if requiredCount <= 0 {
		return nil, ErrNoValidConnections
	}

	if !isBalancerStreamDataLike(packetType) || streamID == 0 {
		selected := b.getUniqueConnectionsLocked(requiredCount)
		if len(selected) == 0 {
			return nil, ErrNoValidConnections
		}
		return selected, nil
	}

	state := b.ensureStreamRouteLocked(streamID)
	preferred, ok := b.selectPreferredConnectionForStreamLocked(packetType, state)
	if !ok {
		selected := b.getUniqueConnectionsLocked(requiredCount)
		if len(selected) == 0 {
			return nil, ErrNoValidConnections
		}
		return selected, nil
	}

	if requiredCount <= 1 {
		return []Connection{preferred}, nil
	}

	if cached, ok := b.cachedTargetsForStreamLocked(state, preferred.Key, requiredCount); ok {
		return cached, nil
	}

	selected := make([]Connection, 0, requiredCount)
	selected = append(selected, preferred)
	for _, conn := range b.getUniqueConnectionsLocked(requiredCount) {
		if !conn.IsValid || conn.Key == "" || conn.Key == preferred.Key {
			continue
		}
		selected = append(selected, conn)
		if len(selected) >= requiredCount {
			break
		}
	}

	if len(selected) == 0 {
		return nil, ErrNoValidConnections
	}
	b.cacheTargetsForStreamLocked(state, selected)
	return selected, nil
}

func (b *Balancer) AverageRTT(serverKey string) (time.Duration, bool) {
	stats := b.statsForKey(serverKey)
	if stats == nil {
		return 0, false
	}

	_, _, sum, count := stats.snapshot()
	if count == 0 {
		return 0, false
	}

	return time.Duration(sum/count) * time.Microsecond, true
}

func (b *Balancer) connectionsByIDsLocked(ids []int) []Connection {
	if len(ids) == 0 {
		return nil
	}
	result := make([]Connection, len(ids))
	for i, idx := range ids {
		if idx < 0 || idx >= len(b.connections) {
			continue
		}
		result[i] = b.connections[idx]
	}
	return result
}

func (b *Balancer) ensureStreamRouteLocked(streamID uint16) *balancerStreamRouteState {
	if streamID == 0 {
		return nil
	}
	state := b.streamRoutes[streamID]
	if state == nil {
		state = &balancerStreamRouteState{}
		b.streamRoutes[streamID] = state
	}
	return state
}

func isBalancerStreamDataLike(packetType uint8) bool {
	return packetType == Enums.PACKET_STREAM_DATA || packetType == Enums.PACKET_STREAM_RESEND
}

func (b *Balancer) selectPreferredConnectionForStreamLocked(packetType uint8, state *balancerStreamRouteState) (Connection, bool) {
	if state == nil {
		return Connection{}, false
	}

	if packetType == Enums.PACKET_STREAM_RESEND {
		state.ResendStreak++
		if current, ok := b.validPreferredConnectionLocked(state); ok {
			if state.ResendStreak < b.streamFailoverThreshold {
				return current, true
			}
			if !state.LastFailoverAt.IsZero() && time.Since(state.LastFailoverAt) < b.streamFailoverCooldown {
				return current, true
			}
			if replacement, ok := b.selectAlternateConnectionLocked(current.Key); ok {
				state.PreferredResolverKey = replacement.Key
				state.ResendStreak = 0
				state.LastFailoverAt = time.Now()
				b.clearCachedTargetsForStreamLocked(state)
				return replacement, true
			}
			return current, true
		}
	}

	if current, ok := b.validPreferredConnectionLocked(state); ok {
		return current, true
	}

	replacement, ok := b.selectAlternateConnectionLocked(state.PreferredResolverKey)
	if !ok {
		return Connection{}, false
	}
	state.PreferredResolverKey = replacement.Key
	state.ResendStreak = 0
	b.clearCachedTargetsForStreamLocked(state)
	return replacement, true
}

func (b *Balancer) validPreferredConnectionLocked(state *balancerStreamRouteState) (Connection, bool) {
	if state == nil || state.PreferredResolverKey == "" {
		return Connection{}, false
	}
	conn, ok := b.connectionByKeyLocked(state.PreferredResolverKey)
	if !ok || !conn.IsValid || conn.Key == "" {
		return Connection{}, false
	}
	return *conn, true
}

func (b *Balancer) selectAlternateConnectionLocked(excludeKey string) (Connection, bool) {
	if excludeKey != "" {
		if replacement, ok := b.getBestConnectionExcludingLocked(excludeKey); ok {
			return replacement, true
		}
	}

	selected := b.getUniqueConnectionsLocked(1)
	if len(selected) == 0 {
		return Connection{}, false
	}
	if excludeKey == "" || selected[0].Key != excludeKey {
		return selected[0], true
	}
	if replacement, ok := b.getBestConnectionExcludingLocked(excludeKey); ok {
		return replacement, true
	}
	return Connection{}, false
}

func (b *Balancer) clearPreferredResolverReferencesLocked(serverKey string) {
	if serverKey == "" {
		return
	}
	for _, state := range b.streamRoutes {
		if state == nil || state.PreferredResolverKey != serverKey {
			continue
		}
		state.PreferredResolverKey = ""
		state.ResendStreak = 0
		b.clearCachedTargetsForStreamLocked(state)
	}
}

func (b *Balancer) moveConnectionStateLocked(idx int, valid bool) {
	if valid {
		b.removeInactiveIndexLocked(idx)
		b.addActiveIndexLocked(idx)
		return
	}
	b.removeActiveIndexLocked(idx)
	b.addInactiveIndexLocked(idx)
}

func (b *Balancer) clearCachedTargetsForStreamLocked(state *balancerStreamRouteState) {
	if state == nil {
		return
	}

	state.CachedTargetKeys = nil
	state.CachedRequiredCount = 0
}

func (b *Balancer) cacheTargetsForStreamLocked(state *balancerStreamRouteState, selected []Connection) {
	if state == nil || len(selected) <= 1 {
		b.clearCachedTargetsForStreamLocked(state)
		return
	}

	keys := make([]string, 0, len(selected))

	for _, conn := range selected {
		if conn.Key == "" || !conn.IsValid {
			b.clearCachedTargetsForStreamLocked(state)
			return
		}
		keys = append(keys, conn.Key)
	}

	state.CachedTargetKeys = keys
	state.CachedRequiredCount = len(keys)
}

func (b *Balancer) cachedTargetsForStreamLocked(state *balancerStreamRouteState, preferredKey string, requiredCount int) ([]Connection, bool) {
	if state == nil || requiredCount <= 1 {
		return nil, false
	}
	if state.CachedRequiredCount != requiredCount || len(state.CachedTargetKeys) != requiredCount {
		return nil, false
	}
	if len(state.CachedTargetKeys) == 0 || state.CachedTargetKeys[0] != preferredKey {
		return nil, false
	}
	selected := make([]Connection, 0, requiredCount)
	seen := make(map[string]struct{}, requiredCount)
	for _, key := range state.CachedTargetKeys {
		conn, ok := b.connectionByKeyLocked(key)
		if !ok || !conn.IsValid || conn.Key == "" {
			return nil, false
		}
		if _, exists := seen[conn.Key]; exists {
			return nil, false
		}
		seen[conn.Key] = struct{}{}
		selected = append(selected, *conn)
	}

	if len(selected) != requiredCount {
		return nil, false
	}

	return selected, true
}

func (b *Balancer) addActiveIndexLocked(idx int) {
	if _, exists := b.activePos[idx]; exists {
		return
	}
	b.activePos[idx] = len(b.activeIDs)
	b.activeIDs = append(b.activeIDs, idx)
}

func (b *Balancer) addInactiveIndexLocked(idx int) {
	if _, exists := b.inactivePos[idx]; exists {
		return
	}
	b.inactivePos[idx] = len(b.inactiveIDs)
	b.inactiveIDs = append(b.inactiveIDs, idx)
}

func (b *Balancer) removeActiveIndexLocked(idx int) {
	pos, ok := b.activePos[idx]
	if !ok || pos < 0 || pos >= len(b.activeIDs) {
		return
	}
	lastPos := len(b.activeIDs) - 1
	lastIdx := b.activeIDs[lastPos]
	b.activeIDs[pos] = lastIdx
	b.activeIDs = b.activeIDs[:lastPos]
	delete(b.activePos, idx)
	if pos < len(b.activeIDs) {
		b.activePos[lastIdx] = pos
	}
}

func (b *Balancer) removeInactiveIndexLocked(idx int) {
	pos, ok := b.inactivePos[idx]
	if !ok || pos < 0 || pos >= len(b.inactiveIDs) {
		return
	}
	lastPos := len(b.inactiveIDs) - 1
	lastIdx := b.inactiveIDs[lastPos]
	b.inactiveIDs[pos] = lastIdx
	b.inactiveIDs = b.inactiveIDs[:lastPos]
	delete(b.inactivePos, idx)
	if pos < len(b.inactiveIDs) {
		b.inactivePos[lastIdx] = pos
	}
}

func (b *Balancer) statsForKey(serverKey string) *connectionStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	idx, ok := b.indexByKey[serverKey]
	if !ok || idx < 0 || idx >= len(b.stats) {
		return nil
	}

	return b.stats[idx]
}

func (b *Balancer) connectionByKeyLocked(serverKey string) (*Connection, bool) {
	idx, ok := b.indexByKey[serverKey]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return nil, false
	}

	return &b.connections[idx], true
}

func (b *Balancer) ensureWindowLocked(conn *Connection, now time.Time, window time.Duration) {
	if conn == nil {
		return
	}

	if now.IsZero() {
		now = time.Now()
	}

	if window <= 0 {
		if conn.WindowStartedAt.IsZero() {
			conn.WindowStartedAt = now
		}
		return
	}

	if conn.WindowStartedAt.IsZero() || now.Sub(conn.WindowStartedAt) >= window {
		b.resetWindowLocked(conn)
		conn.WindowStartedAt = now
	}
}

func (b *Balancer) resetWindowLocked(conn *Connection) {
	if conn == nil {
		return
	}

	conn.WindowStartedAt = time.Time{}
	conn.WindowSent = 0
	conn.WindowTimedOut = 0
}

func normalizeRequiredCount(validCount, requiredCount, defaultIfInvalid int) int {
	if validCount <= 0 {
		return 0
	}
	if requiredCount <= 0 {
		requiredCount = defaultIfInvalid
	}
	if requiredCount > validCount {
		return validCount
	}
	return requiredCount
}

const (
	resolverPendingSoftCap = 8192
	resolverPendingHardCap = 12288
)

func resolverSampleTTL(tunnelPacketTimeout time.Duration) time.Duration {
	ttl := tunnelPacketTimeout * 3
	if ttl < 10*time.Second {
		ttl = 10 * time.Second
	}
	if ttl > 45*time.Second {
		ttl = 45 * time.Second
	}
	return ttl
}

func resolverRequestTimeout(tunnelPacketTimeout time.Duration, checkInterval time.Duration, window time.Duration) time.Duration {
	timeout := tunnelPacketTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if checkInterval > 0 && checkInterval < timeout {
		timeout = checkInterval
	}
	if window > 0 && window < timeout {
		timeout = window
	}
	if timeout < 500*time.Millisecond {
		timeout = 500 * time.Millisecond
	}
	return timeout
}

func resolverLateResponseGrace(requestTimeout time.Duration, ttl time.Duration) time.Duration {
	if requestTimeout <= 0 {
		requestTimeout = 5 * time.Second
	}
	grace := requestTimeout * 3
	if grace < time.Second {
		grace = time.Second
	}
	if ttl > 0 && grace > ttl {
		grace = ttl
	}
	return grace
}

func (b *Balancer) prunePendingLocked(now time.Time, requestTimeout time.Duration, ttl time.Duration) []balancerTimeoutObservation {
	if b == nil || len(b.pending) == 0 {
		return nil
	}

	timeoutBefore := now.Add(-requestTimeout)
	absoluteCutoff := now.Add(-ttl)
	lateGrace := resolverLateResponseGrace(requestTimeout, ttl)
	var timeoutObservations []balancerTimeoutObservation

	for key, sample := range b.pending {
		if !sample.timedOut {
			if !sample.sentAt.After(timeoutBefore) {
				sample.timedOut = true
				sample.timedOutAt = sample.sentAt.Add(requestTimeout)
				if sample.timedOutAt.After(now) {
					sample.timedOutAt = now
				}
				sample.evictAfter = sample.timedOutAt.Add(lateGrace)
				b.pending[key] = sample
				if sample.serverKey != "" {
					timeoutObservations = append(timeoutObservations, balancerTimeoutObservation{
						serverKey: sample.serverKey,
						at:        sample.timedOutAt,
					})
				}
			}
			if sample.sentAt.Before(absoluteCutoff) {
				delete(b.pending, key)
			}
			continue
		}

		if !sample.evictAfter.IsZero() && !sample.evictAfter.After(now) {
			delete(b.pending, key)
			continue
		}
		if sample.sentAt.Before(absoluteCutoff) {
			delete(b.pending, key)
		}
	}

	return timeoutObservations
}

func (b *Balancer) evictPendingLocked(evictCount int) {
	if b == nil || evictCount <= 0 || len(b.pending) == 0 {
		return
	}

	type pendingEntry struct {
		key    balancerResolverSampleKey
		sample balancerResolverSample
	}

	entries := make([]pendingEntry, 0, len(b.pending))
	for key, sample := range b.pending {
		entries = append(entries, pendingEntry{key: key, sample: sample})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].sample.timedOut != entries[j].sample.timedOut {
			return entries[i].sample.timedOut
		}
		if !entries[i].sample.sentAt.Equal(entries[j].sample.sentAt) {
			return entries[i].sample.sentAt.Before(entries[j].sample.sentAt)
		}
		if entries[i].key.resolverAddr != entries[j].key.resolverAddr {
			return entries[i].key.resolverAddr < entries[j].key.resolverAddr
		}
		return entries[i].key.dnsID < entries[j].key.dnsID
	})

	if evictCount > len(entries) {
		evictCount = len(entries)
	}
	for i := 0; i < evictCount; i++ {
		delete(b.pending, entries[i].key)
	}
}

func (b *Balancer) getUniqueConnectionsLocked(requiredCount int) []Connection {
	count := normalizeRequiredCount(len(b.activeIDs), requiredCount, 1)
	if count <= 0 {
		return nil
	}

	if count == 1 {
		best, ok := b.getBestConnectionLocked()
		if !ok {
			return nil
		}
		return []Connection{best}
	}

	switch b.strategy {
	case BalancingRandom:
		return b.selectRandomLocked(count)
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.selectRoundRobinLocked(count)
		}
		return b.selectLowestScoreLocked(count, b.lossScoreLocked)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.selectRoundRobinLocked(count)
		}
		return b.selectLowestScoreLocked(count, b.latencyScoreLocked)
	default:
		return b.selectRoundRobinLocked(count)
	}
}

func (b *Balancer) getBestConnectionLocked() (Connection, bool) {
	switch b.strategy {
	case BalancingRandom:
		idx := b.activeIDs[b.nextRandom()%uint64(len(b.activeIDs))]
		return b.connections[idx], true
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.roundRobinBestConnectionLocked()
		}
		return b.bestScoredConnectionLocked(b.lossScoreLocked)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.roundRobinBestConnectionLocked()
		}
		return b.bestScoredConnectionLocked(b.latencyScoreLocked)
	default:
		return b.roundRobinBestConnectionLocked()
	}
}

func (b *Balancer) getBestConnectionExcludingLocked(excludeKey string) (Connection, bool) {
	switch b.strategy {
	case BalancingRandom:
		ordered := b.rotatedActiveIndicesLocked(1)
		for _, idx := range ordered {
			if b.connections[idx].Key == excludeKey {
				continue
			}
			return b.connections[idx], true
		}
		return Connection{}, false
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.roundRobinBestConnectionExcludingLocked(excludeKey)
		}
		return b.bestScoredConnectionExcludingLocked(b.lossScoreLocked, excludeKey)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.roundRobinBestConnectionExcludingLocked(excludeKey)
		}
		return b.bestScoredConnectionExcludingLocked(b.latencyScoreLocked, excludeKey)
	default:
		return b.roundRobinBestConnectionExcludingLocked(excludeKey)
	}
}

func (b *Balancer) selectRoundRobinLocked(count int) []Connection {
	n := len(b.activeIDs)
	start := roundRobinStartIndex(b.rrCounter.Add(uint64(count))-uint64(count), n)
	selected := make([]Connection, count)
	for i := 0; i < count; i++ {
		selected[i] = b.connections[b.activeIDs[(start+i)%n]]
	}
	return selected
}

func (b *Balancer) selectRandomLocked(count int) []Connection {
	n := len(b.activeIDs)
	if count <= 0 || n == 0 {
		return nil
	}
	if count == 1 {
		idx := b.activeIDs[b.nextRandom()%uint64(n)]
		return []Connection{b.connections[idx]}
	}

	indices := append([]int(nil), b.activeIDs...)
	for i := 0; i < count; i++ {
		j := i + int(b.nextRandom()%uint64(n-i))
		indices[i], indices[j] = indices[j], indices[i]
	}
	return b.connectionsByIndicesLocked(indices[:count])
}

func (b *Balancer) selectLowestScoreLocked(count int, scorer func(int) uint64) []Connection {
	n := len(b.activeIDs)
	if count <= 0 || n == 0 {
		return nil
	}
	if count == 1 {
		conn, ok := b.bestScoredConnectionLocked(scorer)
		if ok {
			return []Connection{conn}
		}
		return nil
	}

	type scoredIdx struct {
		idx   int
		score uint64
	}

	ordered := b.rotatedActiveIndicesLocked(count)
	scored := make([]scoredIdx, n)
	for i, idx := range ordered {
		scored[i] = scoredIdx{idx: idx, score: scorer(idx)}
	}

	for i := 0; i < count && i < n; i++ {
		minIdx := i
		for j := i + 1; j < n; j++ {
			if scored[j].score < scored[minIdx].score {
				minIdx = j
			}
		}
		scored[i], scored[minIdx] = scored[minIdx], scored[i]
	}

	indices := make([]int, count)
	for i := 0; i < count; i++ {
		indices[i] = scored[i].idx
	}
	return b.connectionsByIndicesLocked(indices)
}

func (b *Balancer) connectionsByIndicesLocked(indices []int) []Connection {
	selected := make([]Connection, len(indices))
	for i, idx := range indices {
		if idx < 0 || idx >= len(b.connections) {
			continue
		}
		selected[i] = b.connections[idx]
	}
	return selected
}

func (b *Balancer) bestScoredConnectionLocked(scorer func(int) uint64) (Connection, bool) {
	ordered := b.rotatedActiveIndicesLocked(1)
	bestIndex := -1
	var bestScore uint64
	for _, idx := range ordered {
		score := scorer(idx)
		if bestIndex == -1 || score < bestScore {
			bestIndex = idx
			bestScore = score
		}
	}
	if bestIndex < 0 {
		return Connection{}, false
	}
	return b.connections[bestIndex], true
}

func (b *Balancer) bestScoredConnectionExcludingLocked(scorer func(int) uint64, excludeKey string) (Connection, bool) {
	ordered := b.rotatedActiveIndicesLocked(1)
	bestIndex := -1
	var bestScore uint64
	for _, idx := range ordered {
		if b.connections[idx].Key == excludeKey {
			continue
		}
		score := scorer(idx)
		if bestIndex == -1 || score < bestScore {
			bestIndex = idx
			bestScore = score
		}
	}
	if bestIndex < 0 {
		return Connection{}, false
	}
	return b.connections[bestIndex], true
}

func (b *Balancer) roundRobinBestConnectionLocked() (Connection, bool) {
	if len(b.activeIDs) == 0 {
		return Connection{}, false
	}
	pos := roundRobinStartIndex(b.rrCounter.Add(1)-1, len(b.activeIDs))
	return b.connections[b.activeIDs[pos]], true
}

func (b *Balancer) roundRobinBestConnectionExcludingLocked(excludeKey string) (Connection, bool) {
	if len(b.activeIDs) == 0 {
		return Connection{}, false
	}
	for _, idx := range b.rotatedActiveIndicesLocked(1) {
		if b.connections[idx].Key == excludeKey {
			continue
		}
		return b.connections[idx], true
	}
	return Connection{}, false
}

func (b *Balancer) rotatedActiveIndicesLocked(step int) []int {
	if len(b.activeIDs) == 0 {
		return nil
	}
	if step < 1 {
		step = 1
	}

	start := roundRobinStartIndex(b.rrCounter.Add(uint64(step))-uint64(step), len(b.activeIDs))
	ordered := make([]int, len(b.activeIDs))
	for i := range b.activeIDs {
		ordered[i] = b.activeIDs[(start+i)%len(b.activeIDs)]
	}
	return ordered
}

func roundRobinStartIndex(counter uint64, n int) int {
	if n <= 0 {
		return 0
	}
	return int(counter % uint64(n))
}

func (b *Balancer) hasLossSignalLocked() bool {
	for _, idx := range b.activeIDs {
		stats := b.stats[idx]
		if stats == nil {
			continue
		}
		sent, _, _, _ := stats.snapshot()
		if sent >= 5 {
			return true
		}
	}
	return false
}

func (b *Balancer) hasLatencySignalLocked() bool {
	for _, idx := range b.activeIDs {
		stats := b.stats[idx]
		if stats == nil {
			continue
		}
		_, _, _, count := stats.snapshot()
		if count >= 5 {
			return true
		}
	}
	return false
}

func (b *Balancer) lossScoreLocked(idx int) uint64 {
	if idx < 0 || idx >= len(b.stats) || b.stats[idx] == nil {
		return 500
	}
	sent, acked, _, _ := b.stats[idx].snapshot()
	if sent < 5 {
		return 500
	}
	if acked >= sent {
		return 0
	}
	return (sent - acked) * 1000 / sent
}

func (b *Balancer) latencyScoreLocked(idx int) uint64 {
	if idx < 0 || idx >= len(b.stats) || b.stats[idx] == nil {
		return 999000
	}
	_, _, sum, count := b.stats[idx].snapshot()
	if count < 5 {
		return 999000
	}
	return sum / count
}

func (s *connectionStats) snapshot() (sent uint64, acked uint64, rttMicrosSum uint64, rttCount uint64) {
	if s == nil {
		return 0, 0, 0, 0
	}

	s.mu.RLock()
	sent = s.sent
	acked = s.acked
	rttMicrosSum = s.rttMicrosSum
	rttCount = s.rttCount
	s.mu.RUnlock()
	return sent, acked, rttMicrosSum, rttCount
}

func (s *connectionStats) applyHalfLifeLocked() {
	if s == nil {
		return
	}
	if s.sent <= connectionStatsHalfLifeThreshold &&
		s.acked <= connectionStatsHalfLifeThreshold &&
		s.rttCount <= connectionStatsHalfLifeThreshold {
		return
	}

	s.sent /= 2
	s.acked /= 2
	s.rttMicrosSum /= 2
	s.rttCount /= 2
}

func (b *Balancer) nextRandom() uint64 {
	for {
		current := b.rngState.Load()
		next := xorshift64(current)
		if b.rngState.CompareAndSwap(current, next) {
			return next
		}
	}
}

func seedRNG() uint64 {
	seed := uint64(time.Now().UnixNano())
	if seed == 0 {
		return 0x9e3779b97f4a7c15
	}
	return seed
}

func xorshift64(v uint64) uint64 {
	if v == 0 {
		v = 0x9e3779b97f4a7c15
	}
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	return v
}

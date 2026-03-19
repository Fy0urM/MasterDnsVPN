// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package udpserver

import (
	"sync"
	"time"

	Enums "masterdnsvpn-go/internal/enums"
	VpnProto "masterdnsvpn-go/internal/vpnproto"
)

const streamOutboundInitialRetryDelay = 350 * time.Millisecond
const streamOutboundMaxRetryDelay = 2 * time.Second
const streamOutboundMinRetryDelay = 120 * time.Millisecond

type streamOutboundStore struct {
	mu         sync.Mutex
	sessions   map[uint8]*streamOutboundSession
	window     int
	queueLimit int
}

type outboundPendingPacket struct {
	Packet     VpnProto.Packet
	CreatedAt  time.Time
	LastSentAt time.Time
	RetryAt    time.Time
	RetryDelay time.Duration
	RetryCount int
}

type streamOutboundSession struct {
	queue     []VpnProto.Packet
	pending   []outboundPendingPacket
	rrCursor  uint16
	retryBase time.Duration
	srtt      time.Duration
	rttVar    time.Duration
}

func newStreamOutboundStore(windowSize int, queueLimit int) *streamOutboundStore {
	if windowSize < 1 {
		windowSize = 1
	}
	if windowSize > 32 {
		windowSize = 32
	}
	if queueLimit < 1 {
		queueLimit = 256
	}
	if queueLimit > 8192 {
		queueLimit = 8192
	}
	return &streamOutboundStore{
		sessions:   make(map[uint8]*streamOutboundSession, 32),
		window:     windowSize,
		queueLimit: queueLimit,
	}
}

func (s *streamOutboundStore) Enqueue(sessionID uint8, packet VpnProto.Packet) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	session := s.sessions[sessionID]
	if session == nil {
		session = &streamOutboundSession{
			queue:     make([]VpnProto.Packet, 0, 8),
			pending:   make([]outboundPendingPacket, 0, s.effectiveWindow()),
			rrCursor:  0,
			retryBase: streamOutboundInitialRetryDelay,
		}
		s.sessions[sessionID] = session
	}
	packet.Payload = append([]byte(nil), packet.Payload...)
	if packet.PacketType == Enums.PACKET_STREAM_RST {
		pruneOutboundStreamPackets(session, packet.StreamID)
		prependOutboundPacket(&session.queue, packet)
		return true
	}
	if packet.PacketType == Enums.PACKET_STREAM_DATA && len(session.queue)+len(session.pending) >= s.effectiveQueueLimit() {
		return false
	}
	session.queue = append(session.queue, packet)
	return true
}

func (s *streamOutboundStore) Next(sessionID uint8, now time.Time) (VpnProto.Packet, bool) {
	if s == nil {
		return VpnProto.Packet{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	session := s.sessions[sessionID]
	if session == nil {
		return VpnProto.Packet{}, false
	}
	if len(session.pending) < s.effectiveWindow() && len(session.queue) != 0 {
		packet, ok := popNextOutboundPacket(session)
		if !ok {
			return VpnProto.Packet{}, false
		}
		retryBase := streamOutboundRetryBase(session)
		session.pending = append(session.pending, outboundPendingPacket{
			Packet:     packet,
			CreatedAt:  now,
			LastSentAt: now,
			RetryAt:    now.Add(retryBase),
			RetryDelay: retryBase,
		})
		return packet, true
	}
	selectedIdx := -1
	for idx := range session.pending {
		if !session.pending[idx].RetryAt.After(now) {
			selectedIdx = idx
			break
		}
	}
	if selectedIdx < 0 {
		return VpnProto.Packet{}, false
	}
	packet := session.pending[selectedIdx].Packet
	delay := session.pending[selectedIdx].RetryDelay
	if delay <= 0 {
		delay = streamOutboundRetryBase(session)
	}
	session.pending[selectedIdx].LastSentAt = now
	session.pending[selectedIdx].RetryAt = now.Add(delay)
	session.pending[selectedIdx].RetryCount++
	delay *= 2
	if delay > streamOutboundMaxRetryDelay {
		delay = streamOutboundMaxRetryDelay
	}
	session.pending[selectedIdx].RetryDelay = delay
	return packet, true
}

func (s *streamOutboundStore) ExpireStalled(sessionID uint8, now time.Time, maxRetries int, ttl time.Duration) []uint16 {
	if s == nil {
		return nil
	}
	if maxRetries < 1 {
		maxRetries = 24
	}
	if ttl <= 0 {
		ttl = 120 * time.Second
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	session := s.sessions[sessionID]
	if session == nil || len(session.pending) == 0 {
		return nil
	}

	expired := make([]uint16, 0, 2)
	for _, pending := range session.pending {
		if pending.RetryCount < maxRetries && now.Sub(pending.CreatedAt) < ttl {
			continue
		}
		if !containsExpiredStream(expired, pending.Packet.StreamID) {
			expired = append(expired, pending.Packet.StreamID)
		}
	}
	if len(expired) == 0 {
		return nil
	}
	for _, streamID := range expired {
		pruneOutboundStreamPackets(session, streamID)
	}
	if len(session.pending) == 0 && len(session.queue) == 0 {
		delete(s.sessions, sessionID)
	}
	return expired
}

func (s *streamOutboundStore) Ack(sessionID uint8, packetType uint8, streamID uint16, sequenceNum uint16) bool {
	if s == nil {
		return false
	}
	ackedAt := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	session := s.sessions[sessionID]
	if session == nil || len(session.pending) == 0 {
		return false
	}
	for idx := range session.pending {
		pending := session.pending[idx]
		if !matchesStreamOutboundAck(pending.Packet.PacketType, packetType) {
			continue
		}
		if pending.Packet.StreamID != streamID || pending.Packet.SequenceNum != sequenceNum {
			continue
		}
		updateStreamOutboundRTO(session, pending, ackedAt)
		copy(session.pending[idx:], session.pending[idx+1:])
		lastIdx := len(session.pending) - 1
		session.pending[lastIdx] = outboundPendingPacket{}
		session.pending = session.pending[:lastIdx]
		if len(session.pending) == 0 && len(session.queue) == 0 {
			delete(s.sessions, sessionID)
		}
		return true
	}
	return false
}

func (s *streamOutboundStore) ClearStream(sessionID uint8, streamID uint16) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	session := s.sessions[sessionID]
	if session == nil {
		return
	}
	if len(session.pending) != 0 {
		filteredPending := session.pending[:0]
		for _, pending := range session.pending {
			if pending.Packet.StreamID != streamID {
				filteredPending = append(filteredPending, pending)
			}
		}
		for idx := len(filteredPending); idx < len(session.pending); idx++ {
			session.pending[idx] = outboundPendingPacket{}
		}
		session.pending = filteredPending
	}
	if len(session.queue) != 0 {
		filtered := session.queue[:0]
		for _, packet := range session.queue {
			if packet.StreamID != streamID {
				filtered = append(filtered, packet)
			}
		}
		session.queue = filtered
	}
	if len(session.pending) == 0 && len(session.queue) == 0 {
		delete(s.sessions, sessionID)
	}
}

func (s *streamOutboundStore) RemoveSession(sessionID uint8) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.sessions, sessionID)
	s.mu.Unlock()
}

func matchesStreamOutboundAck(pendingType uint8, ackType uint8) bool {
	switch pendingType {
	case Enums.PACKET_STREAM_DATA:
		return ackType == Enums.PACKET_STREAM_DATA_ACK
	case Enums.PACKET_STREAM_FIN:
		return ackType == Enums.PACKET_STREAM_FIN_ACK
	case Enums.PACKET_STREAM_RST:
		return ackType == Enums.PACKET_STREAM_RST_ACK
	default:
		return false
	}
}

func pruneOutboundStreamPackets(session *streamOutboundSession, streamID uint16) {
	if session == nil {
		return
	}
	if len(session.queue) != 0 {
		filteredQueue := session.queue[:0]
		for _, packet := range session.queue {
			if packet.StreamID != streamID {
				filteredQueue = append(filteredQueue, packet)
			}
		}
		for idx := len(filteredQueue); idx < len(session.queue); idx++ {
			session.queue[idx] = VpnProto.Packet{}
		}
		session.queue = filteredQueue
	}
	if len(session.pending) != 0 {
		filteredPending := session.pending[:0]
		for _, pending := range session.pending {
			if pending.Packet.StreamID != streamID {
				filteredPending = append(filteredPending, pending)
			}
		}
		for idx := len(filteredPending); idx < len(session.pending); idx++ {
			session.pending[idx] = outboundPendingPacket{}
		}
		session.pending = filteredPending
	}
}

func containsExpiredStream(items []uint16, streamID uint16) bool {
	for _, item := range items {
		if item == streamID {
			return true
		}
	}
	return false
}

func prependOutboundPacket(queue *[]VpnProto.Packet, packet VpnProto.Packet) {
	if queue == nil {
		return
	}
	*queue = append(*queue, VpnProto.Packet{})
	copy((*queue)[1:], (*queue)[:len(*queue)-1])
	(*queue)[0] = packet
}

func popNextOutboundPacket(session *streamOutboundSession) (VpnProto.Packet, bool) {
	if session == nil || len(session.queue) == 0 {
		return VpnProto.Packet{}, false
	}

	bestPriority := 255
	selectedIdx := -1
	lowestStreamID := uint16(0)
	nextStreamID := uint16(0)
	lowestIdx := -1
	nextIdx := -1
	hasLowest := false
	hasNext := false
	for idx, packet := range session.queue {
		priority := outboundPacketPriority(packet.PacketType)
		if priority > bestPriority {
			continue
		}
		if priority < bestPriority {
			bestPriority = priority
			selectedIdx = idx
			lowestStreamID = 0
			nextStreamID = 0
			lowestIdx = -1
			nextIdx = -1
			hasLowest = false
			hasNext = false
		}
		if packet.StreamID == 0 {
			continue
		}
		if !hasLowest || packet.StreamID < lowestStreamID {
			lowestStreamID = packet.StreamID
			lowestIdx = idx
			hasLowest = true
		}
		if packet.StreamID > session.rrCursor && (!hasNext || packet.StreamID < nextStreamID) {
			nextStreamID = packet.StreamID
			nextIdx = idx
			hasNext = true
		}
	}
	if selectedIdx < 0 {
		return VpnProto.Packet{}, false
	}
	if hasNext {
		selectedIdx = nextIdx
		session.rrCursor = nextStreamID
	} else if hasLowest {
		selectedIdx = lowestIdx
		session.rrCursor = lowestStreamID
	}

	packet := session.queue[selectedIdx]
	copy(session.queue[selectedIdx:], session.queue[selectedIdx+1:])
	lastIdx := len(session.queue) - 1
	session.queue[lastIdx] = VpnProto.Packet{}
	session.queue = session.queue[:lastIdx]
	return packet, true
}

func outboundPacketPriority(packetType uint8) int {
	switch packetType {
	case Enums.PACKET_STREAM_RST:
		return 0
	case Enums.PACKET_STREAM_FIN:
		return 1
	case Enums.PACKET_STREAM_SYN_ACK, Enums.PACKET_SOCKS5_SYN_ACK:
		return 2
	case Enums.PACKET_STREAM_DATA_ACK, Enums.PACKET_STREAM_FIN_ACK, Enums.PACKET_STREAM_RST_ACK:
		return 3
	case Enums.PACKET_STREAM_DATA:
		return 4
	default:
		return 5
	}
}

func (s *streamOutboundStore) effectiveWindow() int {
	if s == nil || s.window < 1 {
		return 1
	}
	if s.window > 32 {
		return 32
	}
	return s.window
}

func (s *streamOutboundStore) effectiveQueueLimit() int {
	if s == nil || s.queueLimit < 1 {
		return 256
	}
	if s.queueLimit > 8192 {
		return 8192
	}
	return s.queueLimit
}

func streamOutboundRetryBase(session *streamOutboundSession) time.Duration {
	if session == nil || session.retryBase <= 0 {
		return streamOutboundInitialRetryDelay
	}
	if session.retryBase < streamOutboundMinRetryDelay {
		return streamOutboundMinRetryDelay
	}
	if session.retryBase > streamOutboundMaxRetryDelay {
		return streamOutboundMaxRetryDelay
	}
	return session.retryBase
}

func updateStreamOutboundRTO(session *streamOutboundSession, pending outboundPendingPacket, ackedAt time.Time) {
	if session == nil || pending.RetryCount != 0 || pending.LastSentAt.IsZero() {
		return
	}
	sample := ackedAt.Sub(pending.LastSentAt)
	if sample <= 0 {
		return
	}
	if sample < streamOutboundMinRetryDelay {
		sample = streamOutboundMinRetryDelay
	}
	if sample > streamOutboundMaxRetryDelay {
		sample = streamOutboundMaxRetryDelay
	}
	if session.srtt <= 0 {
		session.srtt = sample
		session.rttVar = sample / 2
	} else {
		diff := session.srtt - sample
		if diff < 0 {
			diff = -diff
		}
		session.rttVar = (3*session.rttVar + diff) / 4
		session.srtt = (7*session.srtt + sample) / 8
	}
	rto := session.srtt + 4*session.rttVar
	if rto < streamOutboundMinRetryDelay {
		rto = streamOutboundMinRetryDelay
	}
	if rto > streamOutboundMaxRetryDelay {
		rto = streamOutboundMaxRetryDelay
	}
	session.retryBase = rto
}

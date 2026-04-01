// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package udpserver

import (
	"context"
	"io"
	"sync"
	"time"

	"masterdnsvpn-go/internal/arq"
	Enums "masterdnsvpn-go/internal/enums"
	"masterdnsvpn-go/internal/mlq"
)

// Stream_server encapsulates an ARQ instance and its transmit queue for a single stream.
type Stream_server struct {
	mu        sync.RWMutex
	txQueueMu sync.Mutex
	cleanupMu sync.Once
	rxQueueMu sync.Mutex

	ID        uint16
	SessionID uint8
	ARQ       *arq.ARQ
	TXQueue   *mlq.MultiLevelQueue[*serverStreamTXPacket]
	RXQueue   *mlq.MultiLevelQueue[*serverInboundPacket]

	Status       string
	CreatedAt    time.Time
	LastActivity time.Time
	CloseTime    time.Time

	UpstreamConn    io.ReadWriteCloser
	TargetHost      string
	TargetPort      uint16
	Connected       bool
	onClosed        func(uint16, time.Time, string)
	log             arq.Logger
	rxSignal        chan struct{}
	rxWorkerMu      sync.Mutex
	rxWorkerCtx     context.Context
	rxWorkerCancel  context.CancelFunc
	rxWorkerRunning bool

	// Tracking for deduplication (similar to Python's _track_stream_packet_once)
	// Key: packetType << 16 | sequenceNum
	// For data packets, we might also want to track by sequence if multiple types exist.
}

type serverInboundPacket struct {
	PacketType  uint8
	SequenceNum uint16
	FragmentID  uint8
	Payload     []byte
}

func NewStreamServer(streamID uint16, sessionID uint8, arqConfig arq.Config, localConn io.ReadWriteCloser, mtu int, queueInitialCapacity int, logger arq.Logger) *Stream_server {
	if queueInitialCapacity < 1 {
		queueInitialCapacity = 32
	}
	s := &Stream_server{
		ID:           streamID,
		SessionID:    sessionID,
		TXQueue:      mlq.New[*serverStreamTXPacket](queueInitialCapacity),
		RXQueue:      mlq.New[*serverInboundPacket](queueInitialCapacity),
		Status:       "PENDING",
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
		log:          logger,
		rxSignal:     make(chan struct{}, 1),
	}

	if s.log == nil {
		s.log = &arq.DummyLogger{}
	}

	s.ARQ = arq.NewARQ(streamID, sessionID, s, localConn, mtu, s.log, arqConfig)
	s.ARQ.Start()
	return s
}

func (s *Stream_server) enqueueInboundData(packetType uint8, sequenceNum uint16, fragmentID uint8, payload []byte) bool {
	if s == nil || s.RXQueue == nil {
		return false
	}

	priority := Enums.NormalizePacketPriority(packetType, Enums.DefaultPacketPriority(packetType))
	key := Enums.PacketIdentityKey(s.ID, Enums.PACKET_STREAM_DATA, sequenceNum, fragmentID)

	pkt := &serverInboundPacket{
		PacketType:  packetType,
		SequenceNum: sequenceNum,
		FragmentID:  fragmentID,
		Payload:     payload,
	}

	s.rxQueueMu.Lock()
	ok := s.RXQueue.Push(priority, key, pkt)
	s.rxQueueMu.Unlock()
	if !ok {
		return false
	}

	select {
	case s.rxSignal <- struct{}{}:
	default:
	}

	s.startInboundWorkerIfReady()
	return true
}

func (s *Stream_server) popInboundData() (*serverInboundPacket, bool) {
	if s == nil || s.RXQueue == nil {
		return nil, false
	}

	s.rxQueueMu.Lock()
	defer s.rxQueueMu.Unlock()

	pkt, _, ok := s.RXQueue.Pop(func(p *serverInboundPacket) uint64 {
		return Enums.PacketIdentityKey(s.ID, Enums.PACKET_STREAM_DATA, p.SequenceNum, p.FragmentID)
	})
	return pkt, ok
}

func (s *Stream_server) clearInboundQueue() {
	if s == nil || s.RXQueue == nil {
		return
	}

	s.rxQueueMu.Lock()
	s.RXQueue.Clear(nil)
	s.rxQueueMu.Unlock()
}

func (s *Stream_server) shutdownInboundProcessing() {
	if s == nil {
		return
	}
	s.stopInboundWorker()
	s.clearInboundQueue()
}

func (s *Stream_server) startInboundWorkerIfReady() {
	if s == nil {
		return
	}

	s.mu.RLock()
	ready := s.Connected && s.ARQ != nil
	s.mu.RUnlock()
	if !ready {
		return
	}

	s.rxWorkerMu.Lock()
	defer s.rxWorkerMu.Unlock()
	if s.rxWorkerRunning {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.rxWorkerCtx = ctx
	s.rxWorkerCancel = cancel
	s.rxWorkerRunning = true

	go s.runInboundWorker(ctx)
}

func (s *Stream_server) stopInboundWorker() {
	if s == nil {
		return
	}

	s.rxWorkerMu.Lock()
	cancel := s.rxWorkerCancel
	s.rxWorkerCancel = nil
	s.rxWorkerCtx = nil
	s.rxWorkerRunning = false
	s.rxWorkerMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

func (s *Stream_server) runInboundWorker(ctx context.Context) {
	defer func() {
		s.rxWorkerMu.Lock()
		if s.rxWorkerCtx == ctx {
			s.rxWorkerCtx = nil
			s.rxWorkerCancel = nil
			s.rxWorkerRunning = false
		}
		s.rxWorkerMu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.rxSignal:
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			pkt, ok := s.popInboundData()
			if !ok {
				break
			}

			s.mu.RLock()
			arqInst := s.ARQ
			connected := s.Connected
			s.mu.RUnlock()
			if !connected || arqInst == nil || arqInst.IsClosed() || arqInst.IsReset() {
				s.clearInboundQueue()
				return
			}

			_ = arqInst.ReceiveData(pkt.SequenceNum, pkt.Payload)
		}
	}
}

// PushTXPacket implements arq.PacketEnqueuer.
// It adds a packet to the stream's multi-level queue.
func (s *Stream_server) PushTXPacket(priority int, packetType uint8, sequenceNum uint16, fragmentID uint8, totalFragments uint8, compressionType uint8, ttl time.Duration, payload []byte) bool {
	s.mu.Lock()
	s.LastActivity = time.Now()
	s.mu.Unlock()

	priority = Enums.NormalizePacketPriority(packetType, priority)

	dataKey := Enums.PacketIdentityKey(s.ID, Enums.PACKET_STREAM_DATA, sequenceNum, fragmentID)
	resendKey := Enums.PacketIdentityKey(s.ID, Enums.PACKET_STREAM_RESEND, sequenceNum, fragmentID)
	key := Enums.PacketIdentityKey(s.ID, packetType, sequenceNum, fragmentID)

	pkt := getTXPacketFromPool()
	pkt.PacketType = packetType
	pkt.SequenceNum = sequenceNum
	pkt.FragmentID = fragmentID
	pkt.TotalFragments = totalFragments
	pkt.CompressionType = compressionType
	pkt.Payload = payload
	pkt.CreatedAt = time.Now()
	pkt.TTL = ttl

	s.txQueueMu.Lock()

	switch packetType {
	case Enums.PACKET_STREAM_DATA:
		if _, exists := s.TXQueue.Get(dataKey); exists {
			s.txQueueMu.Unlock()
			putTXPacketToPool(pkt)
			return false
		}

		if _, exists := s.TXQueue.Get(resendKey); exists {
			s.txQueueMu.Unlock()
			putTXPacketToPool(pkt)
			return false
		}
	case Enums.PACKET_STREAM_RESEND:
		if _, exists := s.TXQueue.Get(resendKey); exists {
			s.txQueueMu.Unlock()
			putTXPacketToPool(pkt)
			return false
		}
	}

	ok := s.TXQueue.Push(priority, key, pkt)
	if !ok {
		s.txQueueMu.Unlock()
		putTXPacketToPool(pkt)
		return false
	}

	if packetType == Enums.PACKET_STREAM_RESEND {
		if stale, removed := s.TXQueue.RemoveByKey(dataKey, func(p *serverStreamTXPacket) uint64 {
			return Enums.PacketIdentityKey(s.ID, p.PacketType, p.SequenceNum, p.FragmentID)
		}); removed {
			putTXPacketToPool(stale)
		}
	}

	s.txQueueMu.Unlock()

	// Notify session that this stream is active (handled by the caller or session management)
	return true
}

func (s *Stream_server) NoteTXPacketDequeued(packet *serverStreamTXPacket) {
	if s == nil || packet == nil || s.ARQ == nil {
		return
	}

	s.ARQ.NoteTXPacketDequeued(packet.PacketType, packet.SequenceNum, packet.FragmentID)
}

func (s *Stream_server) RemoveQueuedData(sequenceNum uint16) bool {
	if s == nil || s.TXQueue == nil {
		return false
	}

	s.txQueueMu.Lock()
	removedAny := false
	for _, packetType := range []uint8{Enums.PACKET_STREAM_DATA, Enums.PACKET_STREAM_RESEND} {
		key := Enums.PacketIdentityKey(s.ID, packetType, sequenceNum, 0)
		pkt, ok := s.TXQueue.RemoveByKey(key, func(p *serverStreamTXPacket) uint64 {
			return Enums.PacketIdentityKey(s.ID, p.PacketType, p.SequenceNum, p.FragmentID)
		})
		if ok {
			putTXPacketToPool(pkt)
			removedAny = true
		}
	}

	s.txQueueMu.Unlock()

	return removedAny
}

func (s *Stream_server) RemoveQueuedDataNack(sequenceNum uint16) bool {
	if s == nil || s.TXQueue == nil {
		return false
	}

	s.txQueueMu.Lock()
	key := Enums.PacketIdentityKey(s.ID, Enums.PACKET_STREAM_DATA_NACK, sequenceNum, 0)
	pkt, ok := s.TXQueue.RemoveByKey(key, func(p *serverStreamTXPacket) uint64 {
		return Enums.PacketIdentityKey(s.ID, p.PacketType, p.SequenceNum, p.FragmentID)
	})

	if !ok {
		s.txQueueMu.Unlock()
		return false
	}

	putTXPacketToPool(pkt)
	s.txQueueMu.Unlock()
	return true
}

func (s *Stream_server) ClearTXQueue() {
	if s == nil || s.TXQueue == nil {
		return
	}

	s.txQueueMu.Lock()
	s.TXQueue.Clear(func(pkt *serverStreamTXPacket) {
		putTXPacketToPool(pkt)
	})

	s.txQueueMu.Unlock()

}

func (s *Stream_server) FastTXQueueSize() int {
	if s == nil || s.TXQueue == nil {
		return 0
	}

	return s.TXQueue.FastSize()
}

func (s *Stream_server) PopNextTXPacket() (*serverStreamTXPacket, int, bool) {
	if s == nil || s.TXQueue == nil {
		return nil, 0, false
	}

	s.txQueueMu.Lock()
	packet, priority, ok := s.TXQueue.Pop(func(p *serverStreamTXPacket) uint64 {
		return Enums.PacketIdentityKey(s.ID, p.PacketType, p.SequenceNum, p.FragmentID)
	})
	s.txQueueMu.Unlock()

	return packet, priority, ok
}

func (s *Stream_server) PopAnyTXPacket(maxPriority int, predicate func(*serverStreamTXPacket) bool) (*serverStreamTXPacket, bool) {
	if s == nil || s.TXQueue == nil {
		return nil, false
	}

	s.txQueueMu.Lock()
	packet, ok := s.TXQueue.PopAnyIf(maxPriority, predicate, func(p *serverStreamTXPacket) uint64 {
		return Enums.PacketIdentityKey(s.ID, p.PacketType, p.SequenceNum, p.FragmentID)
	})
	s.txQueueMu.Unlock()

	return packet, ok
}

func (s *Stream_server) Abort(reason string) {
	s.CloseStream(true, 0, reason)
}

func (s *Stream_server) attachUpstreamConn(conn io.ReadWriteCloser, host string, port uint16, status string) bool {
	if s == nil || conn == nil {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Status == "CLOSED" || !s.CloseTime.IsZero() {
		return false
	}
	if s.ARQ != nil && s.ARQ.IsClosed() {
		return false
	}
	if s.UpstreamConn != nil || s.Connected {
		return false
	}

	s.UpstreamConn = conn
	s.TargetHost = host
	s.TargetPort = port
	s.Connected = true
	if status != "" {
		s.Status = status
	}
	s.LastActivity = time.Now()
	go s.startInboundWorkerIfReady()
	return true
}

func (s *Stream_server) cleanupResources() {
	var upstream io.ReadWriteCloser

	s.mu.Lock()
	s.Status = "CLOSED"
	s.CloseTime = time.Now()
	s.Connected = false
	upstream = s.UpstreamConn
	s.UpstreamConn = nil
	s.mu.Unlock()

	if upstream != nil {
		_ = upstream.Close()
	}
	s.shutdownInboundProcessing()
	s.ClearTXQueue()
}

func (s *Stream_server) finalizeAfterARQClose(reason string) {
	if s == nil {
		return
	}

	s.cleanupMu.Do(func() {
		now := time.Now()
		s.cleanupResources()
		if s.onClosed != nil {
			s.onClosed(s.ID, now, reason)
		}
	})
}

func (s *Stream_server) OnARQClosed(reason string) {
	s.finalizeAfterARQClose(reason)
}

func (s *Stream_server) closeUpstreamOnly(status string) {
	if s == nil {
		return
	}

	var upstream io.ReadWriteCloser

	s.mu.Lock()
	if status != "" {
		s.Status = status
	} else if s.Status != "CLOSED" {
		s.Status = "CLOSING"
	}
	s.CloseTime = time.Now()
	s.Connected = false
	upstream = s.UpstreamConn
	s.UpstreamConn = nil
	s.mu.Unlock()

	if upstream != nil {
		_ = upstream.Close()
	}
}

func (s *Stream_server) CloseStream(force bool, ttl time.Duration, reason string) {
	if s == nil {
		return
	}

	if s.ARQ != nil {
		if force {
			s.closeUpstreamOnly("CLOSED")
			s.ARQ.Close(reason, arq.CloseOptions{
				SendRST: true,
				TTL:     ttl,
			})
			return
		}

		s.ARQ.Close(reason, arq.CloseOptions{
			SendCloseRead: true,
			AfterDrain:    true,
			TTL:           ttl,
		})
		return
	}

	s.finalizeAfterARQClose(reason)
}

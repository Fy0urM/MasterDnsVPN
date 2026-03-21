// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package client

import (
	"net"
	"sync" // Added for sync.Pool
	"time"

	Enums "masterdnsvpn-go/internal/enums"
	"masterdnsvpn-go/internal/mlq"
)

var txPacketPool = sync.Pool{
	New: func() any {
		return &clientStreamTXPacket{}
	},
}

// Stream_client represents a single stream's data structure, mirroring the Python version's
// 'active_streams' dictionary elements.
type Stream_client struct {
	StreamID           uint16
	NetConn            net.Conn
	CreateTime         time.Time
	LastActivityTime   time.Time
	Status             string // PENDING, ACTIVE, CLOSED
	Stream             any    // The ARQ object
	StreamCreating     bool
	PendingInboundData map[uint16][]byte

	// High-performance multi-level priority queue
	txQueue *mlq.MultiLevelQueue[*clientStreamTXPacket]

	InitialPayload []byte
	PriorityCounts map[int]int

	// Metadata & Failover
	PreferredServerKey     string
	ResolverResendStreak   int
	LastResolverFailoverAt time.Time
	HandshakeLastProgress  time.Time
}

// getTrackingKey generates a unified key for duplicate prevention.
// It ensures that certain types (like DATA and RESEND) share the same tracking slot.
func getTrackingKey(packetType uint8, sequenceNum uint16) uint32 {
	t := packetType
	// Map related types to a single ID to prevent duplicates across types
	if t == Enums.PACKET_STREAM_RESEND {
		t = Enums.PACKET_STREAM_DATA
	}
	return uint32(t)<<16 | uint32(sequenceNum)
}

// get_new_stream_id finds the next available stream ID using a circular counter (1-65535).
func (c *Client) get_new_stream_id() (uint16, bool) {
	c.streamsMu.Lock()
	defer c.streamsMu.Unlock()

	start := c.last_stream_id + 1
	if start == 0 {
		start = 1
	}

	id := start
	wrapped := false

	// Cycle through IDs to find an available one in active_streams
	for {
		if _, exists := c.active_streams[id]; !exists {
			c.last_stream_id = id
			return id, true
		}

		id++
		if id == 0 {
			if wrapped {
				return 0, false // Fully occupied (unlikely but safe)
			}
			id = 1
			wrapped = true
		}

		if wrapped && id == start {
			return 0, false // Entire cycle checked, no free ID
		}
	}
}

// new_stream initializes a new Stream_client with default values.
func (c *Client) new_stream(streamID uint16, conn net.Conn, targetPayload []byte) *Stream_client {
	now := time.Now()

	s := &Stream_client{
		StreamID:           streamID,
		NetConn:            conn,
		CreateTime:         now,
		LastActivityTime:   now,
		Status:             "PENDING",
		StreamCreating:     false,
		PendingInboundData: make(map[uint16][]byte),
		InitialPayload:     targetPayload,
		PriorityCounts:     make(map[int]int),

		txQueue: mlq.New[*clientStreamTXPacket](64),

		HandshakeLastProgress: now,
	}

	c.streamsMu.Lock()
	if c.active_streams == nil {
		c.active_streams = make(map[uint16]*Stream_client)
	}
	c.active_streams[streamID] = s
	c.streamsMu.Unlock()

	return s
}

// PushTXPacket adds a packet to the appropriate priority queue if it's not a duplicate.
func (s *Stream_client) PushTXPacket(priority int, packetType uint8, sequenceNum uint16, payload []byte) bool {
	// Generate the tracking key (Policy)
	key := getTrackingKey(packetType, sequenceNum)

	// Delegate to MLQ (Mechanism)
	if priority < 0 || priority >= 6 {
		priority = 3 // Default
	}

	// Get a packet from the pool
	p := txPacketPool.Get().(*clientStreamTXPacket)
	p.PacketType = packetType
	p.SequenceNum = sequenceNum
	p.Payload = payload
	p.CreatedAt = time.Now()
	p.RetryCount = 0
	p.Scheduled = false

	if ok := s.txQueue.Push(priority, key, p); !ok {
		// Duplicate found in census
		s.ReleaseTXPacket(p)
		return false
	}

	return true
}

// PopNextTXPacket retrieves the highest priority packet from the queues.
func (s *Stream_client) PopNextTXPacket() (*clientStreamTXPacket, int, bool) {
	// Delegate to MLQ
	packet, priority, ok := s.txQueue.Pop(func(p *clientStreamTXPacket) uint32 {
		return getTrackingKey(p.PacketType, p.SequenceNum)
	})

	return packet, priority, ok
}

// GetQueuedPacket checks if a packet exists in any priority queue in O(1).
func (s *Stream_client) GetQueuedPacket(packetType uint8, sequenceNum uint16) (*clientStreamTXPacket, bool) {
	key := getTrackingKey(packetType, sequenceNum)
	return s.txQueue.Get(key)
}

// Close gracefully shuts down the stream and releases all resources.
func (s *Stream_client) Close() {
	// 1. Close the network connection
	if s.NetConn != nil {
		_ = s.NetConn.Close()
	}

	// 2. Clear the TX queue and return all packets to the pool (Safety)
	if s.txQueue != nil {
		s.txQueue.Clear(func(p *clientStreamTXPacket) {
			s.ReleaseTXPacket(p)
		})
	}

	// 3. Clear inbound buffer
	s.PendingInboundData = nil
	s.Status = "CLOSED"
}

// ReleaseTXPacket returns a packet to the pool.
func (s *Stream_client) ReleaseTXPacket(p *clientStreamTXPacket) {
	if p == nil {
		return
	}
	p.Payload = nil // Clear payload reference
	txPacketPool.Put(p)
}

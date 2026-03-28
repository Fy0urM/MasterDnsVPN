// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package udpserver

import (
	"io"
	"testing"
	"time"

	"masterdnsvpn-go/internal/arq"
	"masterdnsvpn-go/internal/config"
	Enums "masterdnsvpn-go/internal/enums"
	fragmentStore "masterdnsvpn-go/internal/fragmentstore"
	"masterdnsvpn-go/internal/mlq"
	VpnProto "masterdnsvpn-go/internal/vpnproto"
)

type testReadWriteCloser struct {
	closed bool
}

func (t *testReadWriteCloser) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (t *testReadWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (t *testReadWriteCloser) Close() error {
	t.closed = true
	return nil
}

func newTestSessionRecord(sessionID uint8) *sessionRecord {
	r := &sessionRecord{
		ID:               sessionID,
		DownloadMTU:      512,
		DownloadMTUBytes: 512,
		Streams:          make(map[uint16]*Stream_server),
		ActiveStreams:    make([]uint16, 0, 4),
		RecentlyClosed:   make(map[uint16]recentlyClosedStreamRecord, 4),
		OrphanQueue:      mlq.New[VpnProto.Packet](8),
	}
	r.ensureStream0(nil)
	return r
}

func newTestServerForCleanup() *Server {
	return &Server{
		deferredInflight:       make(map[uint64]struct{}, 8),
		dnsFragments:           fragmentStore.New[dnsFragmentKey](8),
		socks5Fragments:        fragmentStore.New[socks5FragmentKey](8),
		deferredDNSSession:     nil,
		deferredConnectSession: nil,
	}
}

func TestCleanupClosedSessionClosesStreamsAndClearsQueues(t *testing.T) {
	s := newTestServerForCleanup()
	record := newTestSessionRecord(7)
	record.streamCleanup = s.cleanupStreamArtifacts

	upstream := &testReadWriteCloser{}
	stream := record.getOrCreateStream(1, arq.Config{}, nil, nil)
	stream.UpstreamConn = upstream
	if !stream.PushTXPacket(Enums.DefaultPacketPriority(Enums.PACKET_STREAM_RST), Enums.PACKET_STREAM_RST, 12, 0, 0, 0, 0, nil) {
		t.Fatalf("expected TX packet to be queued")
	}
	record.enqueueOrphanReset(Enums.PACKET_STREAM_RST, 1, 12)
	s.deferredInflight[deferredTrackedPacketKey(VpnProto.Packet{
		SessionID:     record.ID,
		SessionCookie: record.Cookie,
		PacketType:    Enums.PACKET_SOCKS5_SYN,
		StreamID:      1,
		SequenceNum:   12,
	})] = struct{}{}
	if _, _, completed := s.socks5Fragments.Collect(
		socks5FragmentKey{sessionID: record.ID, streamID: 1, sequenceNum: 12},
		[]byte{0x01, 127, 0, 0, 1, 0x01, 0xBB},
		0,
		1,
		time.Now(),
		5*time.Minute,
	); completed {
		t.Fatal("unexpected completed SOCKS5 fragment state")
	}

	s.cleanupClosedSession(record.ID, record)

	if !upstream.closed {
		t.Fatalf("expected upstream connection to be closed")
	}
	if stream.TXQueue.Size() != 0 {
		t.Fatalf("expected stream TX queue to be cleared, got %d", stream.TXQueue.Size())
	}
	if stream.Status != "CLOSED" {
		t.Fatalf("expected stream status CLOSED, got %q", stream.Status)
	}

	record.StreamsMu.RLock()
	streamCount := len(record.Streams)
	activeCount := len(record.ActiveStreams)
	record.StreamsMu.RUnlock()

	if streamCount != 0 {
		t.Fatalf("expected all session streams to be removed, got %d", streamCount)
	}
	if activeCount != 0 {
		t.Fatalf("expected all active stream ids to be cleared, got %d", activeCount)
	}
	if record.OrphanQueue.Size() != 0 {
		t.Fatalf("expected orphan queue to be cleared, got %d", record.OrphanQueue.Size())
	}
	if _, exists := s.deferredInflight[deferredTrackedPacketKey(VpnProto.Packet{
		SessionID:   record.ID,
		PacketType:  Enums.PACKET_SOCKS5_SYN,
		StreamID:    1,
		SequenceNum: 12,
	})]; exists {
		t.Fatal("expected deferred inflight entries for closed session to be cleared")
	}
	if _, ready, completed := s.socks5Fragments.Collect(
		socks5FragmentKey{sessionID: record.ID, streamID: 1, sequenceNum: 12},
		[]byte{0x01, 8, 8, 8, 8, 0x00, 0x35},
		0,
		1,
		time.Now().Add(time.Second),
		5*time.Minute,
	); !ready || completed {
		t.Fatal("expected closed session cleanup to clear SOCKS5 fragment state")
	}
}

func TestSessionStoreCleanupReturnsExpiredRecordForFollowupCleanup(t *testing.T) {
	store := newSessionStore(8, 32)
	record := newTestSessionRecord(9)
	record.Signature[0] = 1
	record.Cookie = 99
	record.ResponseMode = 1
	record.setLastActivity(time.Now().Add(-2 * time.Minute))

	store.byID[record.ID] = record
	store.bySig[record.Signature] = record.ID
	store.activeCount = 1

	now := time.Now()
	expired := store.Cleanup(now, time.Minute, 10*time.Minute)
	if len(expired) != 1 {
		t.Fatalf("expected one expired session, got %d", len(expired))
	}
	if expired[0].ID != record.ID {
		t.Fatalf("expected expired session id %d, got %d", record.ID, expired[0].ID)
	}
	if expired[0].record != record {
		t.Fatalf("expected cleanup payload to include original session record")
	}
	if store.byID[record.ID] != nil {
		t.Fatalf("expected expired session to be removed from active store")
	}
	if _, ok := store.recentClosed[record.ID]; !ok {
		t.Fatalf("expected expired session to be tracked in recentClosed")
	}
	if !record.isClosed() {
		t.Fatal("expected expired record to be marked closed after cleanup removal")
	}
}

func TestSessionStoreCleanupKeepsActiveRecordOpen(t *testing.T) {
	store := newSessionStore(8, 32)
	record := newTestSessionRecord(10)
	record.Signature[0] = 2
	record.Cookie = 88
	record.ResponseMode = 1
	record.setLastActivity(time.Now())

	store.byID[record.ID] = record
	store.bySig[record.Signature] = record.ID
	store.activeCount = 1

	expired := store.Cleanup(time.Now(), time.Minute, 10*time.Minute)
	if len(expired) != 0 {
		t.Fatalf("expected no expired sessions, got %d", len(expired))
	}
	if store.byID[record.ID] != record {
		t.Fatal("expected active record to remain in active store")
	}
	if record.isClosed() {
		t.Fatal("expected active record to remain open during cleanup scan")
	}
}

func TestCleanupTerminalStreamsAbortsAndRemovesExpiredStreams(t *testing.T) {
	record := newTestSessionRecord(11)

	upstream := &testReadWriteCloser{}
	stream := record.getOrCreateStream(2, arq.Config{}, nil, nil)
	stream.UpstreamConn = upstream
	if !stream.PushTXPacket(Enums.DefaultPacketPriority(Enums.PACKET_STREAM_FIN), Enums.PACKET_STREAM_FIN, 21, 0, 0, 0, 0, nil) {
		t.Fatalf("expected TX packet to be queued")
	}

	stream.Abort("test cleanup")
	record.cleanupTerminalStreams(time.Now().Add(46*time.Second), 45*time.Second)

	if !upstream.closed {
		t.Fatalf("expected upstream connection to be closed during terminal cleanup")
	}
	if _, ok := record.getStream(2); ok {
		t.Fatalf("expected stream to be removed from session")
	}
	if !record.isRecentlyClosed(2, time.Now()) {
		t.Fatalf("expected stream to be tracked as recently closed")
	}
}

func TestDequeueSessionResponseDuplicatesLastPackedControlBlock(t *testing.T) {
	s := &Server{
		cfg: config.ServerConfig{
			PacketBlockControlDuplication: 3,
		},
		sessions: newSessionStore(8, 32),
	}

	record := newTestSessionRecord(12)
	record.MaxPackedBlocks = 2
	stream := record.getOrCreateStream(1, arq.Config{}, nil, nil)
	if !stream.PushTXPacket(Enums.DefaultPacketPriority(Enums.PACKET_STREAM_SYN_ACK), Enums.PACKET_STREAM_SYN_ACK, 11, 0, 0, 0, 0, nil) {
		t.Fatalf("expected first control packet to queue")
	}
	if !stream.PushTXPacket(Enums.DefaultPacketPriority(Enums.PACKET_STREAM_FIN_ACK), Enums.PACKET_STREAM_FIN_ACK, 12, 0, 0, 0, 0, nil) {
		t.Fatalf("expected second control packet to queue")
	}

	s.sessions.byID[record.ID] = record

	first, ok := s.dequeueSessionResponse(record.ID, time.Now())
	if !ok || first == nil {
		t.Fatalf("expected first dequeue to return a packet")
	}
	if first.PacketType != Enums.PACKET_PACKED_CONTROL_BLOCKS {
		t.Fatalf("expected packed control blocks packet, got %d", first.PacketType)
	}
	if len(first.Payload) != 2*VpnProto.PackedControlBlockSize {
		t.Fatalf("unexpected packed payload size: got=%d want=%d", len(first.Payload), 2*VpnProto.PackedControlBlockSize)
	}

	second, ok := s.dequeueSessionResponse(record.ID, time.Now())
	if !ok || second == nil {
		t.Fatalf("expected duplicated dequeue to return cached packet")
	}
	if second.PacketType != Enums.PACKET_PACKED_CONTROL_BLOCKS || string(second.Payload) != string(first.Payload) {
		t.Fatalf("expected second dequeue to return duplicated packed block")
	}

	third, ok := s.dequeueSessionResponse(record.ID, time.Now())
	if !ok || third == nil {
		t.Fatalf("expected final duplicated dequeue to return cached packet")
	}
	if third.PacketType != Enums.PACKET_PACKED_CONTROL_BLOCKS || string(third.Payload) != string(first.Payload) {
		t.Fatalf("expected third dequeue to return duplicated packed block")
	}

	if record.LastPackedControlBlock != nil || record.LastPackedControlBlockRemaining != 0 {
		t.Fatalf("expected packed block duplication cache to be drained")
	}

	if _, ok := s.dequeueSessionResponse(record.ID, time.Now()); ok {
		t.Fatalf("expected no more queued packets after cached duplicates are exhausted")
	}
}

func TestDeactivateStreamRemovesClosedStreamFromRoundRobinAndClearsQueue(t *testing.T) {
	record := newTestSessionRecord(13)
	stream := record.getOrCreateStream(9, arq.Config{}, nil, nil)
	if stream == nil {
		t.Fatal("expected stream to be created")
	}
	if !stream.PushTXPacket(Enums.DefaultPacketPriority(Enums.PACKET_STREAM_DATA), Enums.PACKET_STREAM_DATA, 77, 0, 0, 0, 0, []byte("stale")) {
		t.Fatal("expected stale data to be queued")
	}

	stream.ClearTXQueue()
	record.deactivateStream(9)

	if stream.TXQueue.Size() != 0 {
		t.Fatalf("expected TX queue to be cleared, got %d", stream.TXQueue.Size())
	}

	record.StreamsMu.RLock()
	defer record.StreamsMu.RUnlock()
	for _, id := range record.ActiveStreams {
		if id == 9 {
			t.Fatal("expected closed stream to be removed from ActiveStreams")
		}
	}
}

func TestHandleStreamRSTRequestPreservesRstAckViaOrphanQueue(t *testing.T) {
	s := newTestServerForStreamSyn("TCP")
	record := newTestSessionRecord(14)
	record.Cookie = 7
	s.sessions.byID[record.ID] = record

	stream := record.getOrCreateStream(9, arq.Config{}, nil, nil)
	if stream == nil {
		t.Fatal("expected stream to be created")
	}
	if !stream.PushTXPacket(Enums.DefaultPacketPriority(Enums.PACKET_STREAM_RST_ACK), Enums.PACKET_STREAM_RST_ACK, 55, 0, 0, 0, 0, nil) {
		t.Fatal("expected initial RST_ACK to be queued on stream")
	}
	if !stream.PushTXPacket(Enums.DefaultPacketPriority(Enums.PACKET_STREAM_DATA), Enums.PACKET_STREAM_DATA, 77, 0, 0, 0, 0, []byte("stale")) {
		t.Fatal("expected stale data to be queued")
	}

	packet := VpnProto.Packet{
		SessionID:      record.ID,
		SessionCookie:  record.Cookie,
		PacketType:     Enums.PACKET_STREAM_RST,
		StreamID:       9,
		HasStreamID:    true,
		SequenceNum:    55,
		HasSequenceNum: true,
	}

	if !s.handleStreamRSTRequest(packet) {
		t.Fatal("expected RST request to be handled")
	}

	if stream.TXQueue.Size() != 0 {
		t.Fatalf("expected stream TX queue to be cleared, got %d", stream.TXQueue.Size())
	}

	record.StreamsMu.RLock()
	for _, id := range record.ActiveStreams {
		if id == 9 {
			record.StreamsMu.RUnlock()
			t.Fatal("expected stream to be deactivated after RST")
		}
	}
	record.StreamsMu.RUnlock()

	key := orphanResetKey(Enums.PACKET_STREAM_RST_ACK, 9)
	ackPkt, ok := record.OrphanQueue.Get(key)
	if !ok {
		t.Fatal("expected RST_ACK to be preserved in orphan queue")
	}
	if ackPkt.PacketType != Enums.PACKET_STREAM_RST_ACK || ackPkt.SequenceNum != 55 {
		t.Fatalf("unexpected orphan RST_ACK packet: %+v", ackPkt)
	}
}

func TestRecentlyClosedGracefulStreamSuppressesMissingStreamReset(t *testing.T) {
	s := newTestServerForStreamSyn("TCP")
	record := newTestSessionRecord(16)
	record.Cookie = 9
	s.sessions.byID[record.ID] = record

	stream := record.getOrCreateStream(12, arq.Config{}, nil, nil)
	if stream == nil {
		t.Fatal("expected stream to be created")
	}

	stream.OnARQClosed("FIN handshake completed")

	packet := VpnProto.Packet{
		SessionID:     record.ID,
		SessionCookie: record.Cookie,
		PacketType:    Enums.PACKET_STREAM_FIN,
		StreamID:      12,
		HasStreamID:   true,
	}

	if !s.preprocessInboundPacket(packet) {
		t.Fatal("expected recently closed graceful stream packet to be consumed")
	}
	if size := record.OrphanQueue.Size(); size != 0 {
		t.Fatalf("expected no orphan reset for gracefully closed stream, got %d", size)
	}
}

func TestRecentlyClosedNonSuppressedStreamStillQueuesMissingStreamReset(t *testing.T) {
	s := newTestServerForStreamSyn("TCP")
	record := newTestSessionRecord(17)
	record.Cookie = 9
	s.sessions.byID[record.ID] = record
	record.noteStreamClosed(14, time.Now(), false)

	packet := VpnProto.Packet{
		SessionID:     record.ID,
		SessionCookie: record.Cookie,
		PacketType:    Enums.PACKET_STREAM_FIN,
		StreamID:      14,
		HasStreamID:   true,
	}

	if !s.preprocessInboundPacket(packet) {
		t.Fatal("expected recently closed non-suppressed stream packet to be handled")
	}
	if size := record.OrphanQueue.Size(); size != 1 {
		t.Fatalf("expected orphan reset for non-suppressed recently closed stream, got %d", size)
	}
}

func TestReusedStreamIDClearsRecentlyClosedTombstone(t *testing.T) {
	record := newTestSessionRecord(18)
	record.noteStreamClosed(21, time.Now(), true)

	stream := record.getOrCreateStream(21, arq.Config{}, nil, nil)
	if stream == nil {
		t.Fatal("expected stream to be recreated")
	}
	if record.isRecentlyClosed(21, time.Now()) {
		t.Fatal("expected recreated stream id to be removed from recently closed set")
	}
}

func TestPreprocessInboundPacketDoesNotQueueImmediateDataAck(t *testing.T) {
	s := newTestServerForStreamSyn("TCP")
	record := newTestSessionRecord(19)
	record.Cookie = 11
	record.DownloadCompression = 0
	s.sessions.byID[record.ID] = record

	stream := record.getOrCreateStream(31, s.streamARQConfig(record.DownloadCompression), nil, s.log)
	if stream == nil {
		t.Fatal("expected stream to be created")
	}

	packet := VpnProto.Packet{
		SessionID:      record.ID,
		SessionCookie:  record.Cookie,
		PacketType:     Enums.PACKET_STREAM_DATA,
		StreamID:       31,
		HasStreamID:    true,
		SequenceNum:    7,
		HasSequenceNum: true,
		Payload:        []byte("hello"),
	}

	if s.preprocessInboundPacket(packet) {
		t.Fatal("expected stream data not to be fully consumed in preprocess")
	}

	key := Enums.PacketIdentityKey(stream.ID, Enums.PACKET_STREAM_DATA_ACK, packet.SequenceNum, 0)
	if pkt, ok := stream.TXQueue.Get(key); ok || pkt != nil {
		t.Fatal("expected no immediate STREAM_DATA_ACK to be queued during preprocess")
	}
}

func TestOnStreamClosedCleansDeferredAndSOCKSArtifacts(t *testing.T) {
	s := newTestServerForCleanup()
	record := newTestSessionRecord(20)
	record.Cookie = 13
	record.streamCleanup = s.cleanupStreamArtifacts

	stream := record.getOrCreateStream(33, arq.Config{}, nil, nil)
	if stream == nil {
		t.Fatal("expected stream to be created")
	}

	packet := VpnProto.Packet{
		SessionID:     record.ID,
		SessionCookie: record.Cookie,
		PacketType:    Enums.PACKET_SOCKS5_SYN,
		StreamID:      33,
		SequenceNum:   4,
	}
	s.deferredInflight[deferredTrackedPacketKey(packet)] = struct{}{}
	if _, _, completed := s.socks5Fragments.Collect(
		socks5FragmentKey{sessionID: record.ID, streamID: 33, sequenceNum: 4},
		[]byte{0x01, 127, 0, 0, 1, 0x01, 0xBB},
		0,
		1,
		time.Now(),
		5*time.Minute,
	); completed {
		t.Fatal("unexpected completed SOCKS5 fragment state")
	}

	stream.OnARQClosed("FIN handshake completed")

	if _, exists := s.deferredInflight[deferredTrackedPacketKey(packet)]; exists {
		t.Fatal("expected stream close to clear deferred inflight state")
	}
	if _, ready, completed := s.socks5Fragments.Collect(
		socks5FragmentKey{sessionID: record.ID, streamID: 33, sequenceNum: 4},
		[]byte{0x01, 8, 8, 8, 8, 0x00, 0x35},
		0,
		1,
		time.Now().Add(time.Second),
		5*time.Minute,
	); !ready || completed {
		t.Fatal("expected stream close to clear SOCKS5 fragment state")
	}
}

func TestClosedSessionRecordRejectsPostCloseStreamAccess(t *testing.T) {
	record := newTestSessionRecord(15)
	record.ensureStream0(nil)
	record.markClosed()

	if _, ok := record.getStream(0); ok {
		t.Fatal("expected closed record to reject getStream")
	}
	if got := record.getOrCreateStream(9, arq.Config{}, nil, nil); got != nil {
		t.Fatal("expected closed record to reject getOrCreateStream")
	}
	record.ensureStream0(nil)
	if _, ok := record.getStream(9); ok {
		t.Fatal("expected ensureStream0 to no-op after close")
	}
}

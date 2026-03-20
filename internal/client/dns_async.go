// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package client

import (
	"time"

	"masterdnsvpn-go/internal/arq"
	"masterdnsvpn-go/internal/dnscache"
	DnsParser "masterdnsvpn-go/internal/dnsparser"
	Enums "masterdnsvpn-go/internal/enums"
	VpnProto "masterdnsvpn-go/internal/vpnproto"
)

type clientDNSFragmentKey struct {
	sessionID   uint8
	sequenceNum uint16
}

func (c *Client) localDNSFragmentTimeout() time.Duration {
	if c == nil || c.localDNSFragTTL <= 0 {
		return 5 * time.Minute
	}
	return c.localDNSFragTTL
}

func (c *Client) hasPendingDNSWork() bool {
	return c != nil && c.localDNSCache != nil && c.localDNSCache.HasPending()
}

func (c *Client) queueDNSDispatch(request *dnsDispatchRequest) {
	if c == nil || request == nil || len(request.Query) == 0 || c.stream0Runtime == nil {
		return
	}

	if !c.SessionReady() || !c.stream0Runtime.IsRunning() {
		return
	}

	if err := c.stream0Runtime.QueueDNSRequest(request.Query); err != nil && c.log != nil {
		c.log.Debugf(
			"\U0001F9E9 <yellow>Local DNS Queue Failed: <cyan>%s</cyan> (Type: <cyan>%s</cyan>) | Error: <cyan>%v</cyan></yellow>",
			request.Domain,
			Enums.DNSRecordTypeName(request.QType),
			err,
		)
	}
}

func (c *Client) handleInboundDNSResponseFragment(packet VpnProto.Packet) error {
	if c == nil || c.dnsResponses == nil || packet.PacketType != Enums.PACKET_DNS_QUERY_RES || !packet.HasSequenceNum {
		return nil
	}

	if c.stream0Runtime != nil {
		c.stream0Runtime.QueueMainPacket(arqQueuedDNSAck(packet))
	}

	now := c.now()
	assembled, ready, completed := c.dnsResponses.Collect(
		clientDNSFragmentKey{
			sessionID:   packet.SessionID,
			sequenceNum: packet.SequenceNum,
		},
		packet.Payload,
		packet.FragmentID,
		packet.TotalFragments,
		now,
		c.localDNSFragTTL,
	)

	if completed || !ready || len(assembled) == 0 {
		return nil
	}

	parsed, err := DnsParser.ParsePacketLite(assembled)
	if err != nil || !parsed.HasQuestion {
		return nil
	}

	if shouldCacheTunnelDNSResponse(assembled) {
		cacheKey := dnscache.BuildKey(
			parsed.FirstQuestion.Name,
			parsed.FirstQuestion.Type,
			parsed.FirstQuestion.Class,
		)
		c.persistResolvedLocalDNSCacheEntry(
			cacheKey,
			parsed.FirstQuestion.Name,
			parsed.FirstQuestion.Type,
			parsed.FirstQuestion.Class,
			assembled,
			now,
		)
	}

	return nil
}

func arqQueuedDNSAck(packet VpnProto.Packet) arq.QueuedPacket {
	totalFragments := packet.TotalFragments
	if totalFragments == 0 {
		totalFragments = 1
	}
	return arq.QueuedPacket{
		PacketType:     Enums.PACKET_DNS_QUERY_RES_ACK,
		StreamID:       0,
		SequenceNum:    packet.SequenceNum,
		FragmentID:     packet.FragmentID,
		TotalFragments: totalFragments,
		Priority:       arq.DefaultPriorityForPacket(Enums.PACKET_DNS_QUERY_RES_ACK),
	}
}

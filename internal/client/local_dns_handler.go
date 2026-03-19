// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package client

import (
	"time"

	"masterdnsvpn-go/internal/dnscache"
	DnsParser "masterdnsvpn-go/internal/dnsparser"
	Enums "masterdnsvpn-go/internal/enums"
)

type dnsDispatchRequest struct {
	CacheKey []byte
	Query    []byte
	Domain   string
	QType    uint16
	QClass   uint16
}

type dnsQueryMetadata struct {
	Domain string
	QType  uint16
	QClass uint16
	Parsed DnsParser.LitePacket
}

func (c *Client) handleDNSQueryPacket(query []byte) ([]byte, *dnsDispatchRequest) {
	if !DnsParser.LooksLikeDNSRequest(query) {
		return nil, nil
	}

	metadata, ok := parseDNSQueryMetadata(query)
	if !ok {
		response, err := DnsParser.BuildFormatErrorResponse(query)
		if err != nil {
			return nil, nil
		}
		return response, nil
	}
	if !DnsParser.IsSupportedTunnelDNSQuery(metadata.QType, metadata.QClass) {
		response, err := DnsParser.BuildNotImplementedResponseFromLite(query, metadata.Parsed)
		if err != nil {
			return nil, nil
		}
		return response, nil
	}

	cacheKey := dnscache.BuildKey(metadata.Domain, metadata.QType, metadata.QClass)
	recordTypeName := ""
	if c.log != nil {
		recordTypeName = Enums.DNSRecordTypeName(metadata.QType)
	}
	now := c.now()
	if cached, ok := c.localDNSCache.GetReady(cacheKey, query, now); ok {
		c.dnsInflight.Complete(cacheKey)
		if c.log != nil {
			c.log.Infof(
				"📦 <green>Local DNS Cache Hit</green> <magenta>|</magenta> <blue>Domain</blue>: <cyan>%s</cyan> <magenta>|</magenta> <blue>Type</blue>: <yellow>%s</yellow>",
				metadata.Domain,
				recordTypeName,
			)
		}
		return cached, nil
	}

	result := c.localDNSCache.LookupOrCreatePending(cacheKey, metadata.Domain, metadata.QType, metadata.QClass, now)
	response, err := DnsParser.BuildServerFailureResponseFromLite(query, metadata.Parsed)
	if err != nil {
		response = nil
	}
	if !result.DispatchNeeded {
		return response, nil
	}

	dispatch := &dnsDispatchRequest{
		CacheKey: append([]byte(nil), cacheKey...),
		Query:    append([]byte(nil), query...),
		Domain:   metadata.Domain,
		QType:    metadata.QType,
		QClass:   metadata.QClass,
	}
	return response, dispatch
}

func (c *Client) resolveDNSQueryPacket(query []byte) []byte {
	response, dispatch := c.handleDNSQueryPacket(query)
	if dispatch == nil {
		return response
	}

	now := c.now()
	inflightEntry, leader := c.dnsInflight.Acquire(dispatch.CacheKey, now)
	recordTypeName := ""
	if c.log != nil {
		recordTypeName = Enums.DNSRecordTypeName(dispatch.QType)
	}
	if !leader {
		if c.log != nil {
			c.log.Infof(
				"🧩 <green>Local DNS Inflight Reused</green> <magenta>|</magenta> <blue>Domain</blue>: <cyan>%s</cyan> <magenta>|</magenta> <blue>Type</blue>: <yellow>%s</yellow>",
				dispatch.Domain,
				recordTypeName,
			)
		}
		if c.dnsInflight.Wait(inflightEntry, time.Duration(c.cfg.LocalDNSPendingTimeoutSec*float64(time.Second))) {
			if cached, ok := c.localDNSCache.GetReady(dispatch.CacheKey, query, c.now()); ok {
				return cached
			}
		}
		return response
	}

	defer c.dnsInflight.Resolve(dispatch.CacheKey)

	if c.stream0Runtime != nil && c.stream0Runtime.IsRunning() {
		c.stream0Runtime.NotifyDNSActivity()
	}
	if c.log != nil {
		c.log.Infof(
			"🚇 <green>Local DNS Redirected To Tunnel</green> <magenta>|</magenta> <blue>Domain</blue>: <cyan>%s</cyan> <magenta>|</magenta> <blue>Type</blue>: <yellow>%s</yellow>",
			dispatch.Domain,
			recordTypeName,
		)
	}

	tunnelResponse, err := c.dispatchDNSQuery(dispatch)
	if err == nil && len(tunnelResponse) != 0 {
		if c.log != nil {
			c.log.Infof(
				"✅ <green>Local DNS Resolved</green> <magenta>|</magenta> <blue>Domain</blue>: <cyan>%s</cyan> <magenta>|</magenta> <blue>Type</blue>: <yellow>%s</yellow> <magenta>|</magenta> <blue>Path</blue>: <yellow>Tunnel</yellow>",
				dispatch.Domain,
				recordTypeName,
			)
		}
		return tunnelResponse
	}
	if c.log != nil {
		c.log.Warnf(
			"⚠️ <yellow>Local DNS Resolve Failed</yellow> <magenta>|</magenta> <blue>Domain</blue>: <cyan>%s</cyan> <magenta>|</magenta> <blue>Type</blue>: <yellow>%s</yellow> <magenta>|</magenta> <blue>Error</blue>: <red>%v</red>",
			dispatch.Domain,
			recordTypeName,
			err,
		)
	}
	return response
}

func parseDNSQueryMetadata(query []byte) (dnsQueryMetadata, bool) {
	parsed, err := DnsParser.ParsePacketLite(query)
	if err != nil || !parsed.HasQuestion {
		return dnsQueryMetadata{}, false
	}

	question := parsed.FirstQuestion
	return dnsQueryMetadata{
		Domain: question.Name,
		QType:  question.Type,
		QClass: question.Class,
		Parsed: parsed,
	}, true
}

// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package client

import (
	"context"
	"time"
)

func (c *Client) hasPersistableLocalDNSCache() bool {
	return c != nil &&
		c.localDNSCache != nil &&
		c.localDNSCachePersist &&
		c.localDNSCachePath != ""
}

func (c *Client) persistResolvedLocalDNSCacheEntry(cacheKey string, domain string, qType uint16, qClass uint16, rawResponse []byte, now time.Time) {
	if c == nil || c.localDNSCache == nil {
		return
	}

	c.localDNSCache.SetReady(cacheKey, domain, qType, qClass, rawResponse, now)
}

func (c *Client) ensureLocalDNSCacheLoaded() {
	if !c.hasPersistableLocalDNSCache() {
		return
	}

	c.localDNSCacheLoadOnce.Do(func() {
		c.loadLocalDNSCache()
	})
}

func (c *Client) ensureLocalDNSCachePersistence(ctx context.Context) {
	if !c.hasPersistableLocalDNSCache() {
		return
	}

	c.ensureLocalDNSCacheLoaded()
	c.localDNSCacheFlushOnce.Do(func() {
		go c.runLocalDNSCacheFlushLoop(ctx)
	})
}

func (c *Client) loadLocalDNSCache() {
	if !c.hasPersistableLocalDNSCache() {
		return
	}

	loaded, err := c.localDNSCache.LoadFromFile(c.localDNSCachePath, c.now())
	if err != nil {
		if c.log != nil {
			c.log.Warnf("\U0001F4BE <yellow>Local DNS Cache <red>Load Failed:</red> %v</yellow>", err)
		}
		return
	}

	if loaded > 0 && c.log != nil {
		c.log.Infof("\U0001F4BE <green>Local DNS Cache Loaded: <cyan>%d</cyan> records.</green>", loaded)
	}
}

func (c *Client) flushLocalDNSCache() {
	if !c.hasPersistableLocalDNSCache() {
		return
	}

	saved, err := c.localDNSCache.SaveToFile(c.localDNSCachePath, c.now())
	if err != nil {
		if c.log != nil {
			c.log.Warnf("\U0001F4BE <yellow>Local DNS Cache <red>Flush Failed:</red> %v</yellow>", err)
		}
		return
	}

	if saved > 0 && c.log != nil {
		c.log.Debugf("\U0001F4BE <green>Local DNS Cache Flushed: <cyan>%d</cyan> records.</green>", saved)
	}
}

func (c *Client) runLocalDNSCacheFlushLoop(ctx context.Context) {
	if !c.hasPersistableLocalDNSCache() {
		return
	}

	ticker := time.NewTicker(c.localDNSCacheFlushTick)
	defer ticker.Stop()
	defer c.flushLocalDNSCache()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.flushLocalDNSCache()
		}
	}
}

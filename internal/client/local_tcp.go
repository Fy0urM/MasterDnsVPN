// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package client

import (
	"context"
	"net"
	"strconv"
	"time"
)

func (c *Client) RunLocalTCPListener(ctx context.Context) error {
	if c == nil || c.cfg.ProtocolType != "TCP" {
		return nil
	}
	if err := c.startStream0Runtime(ctx); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", net.JoinHostPort(c.cfg.ListenIP, strconv.Itoa(c.cfg.ListenPort)))
	if err != nil {
		return err
	}
	defer listener.Close()

	c.log.Infof(
		"🔌 <green>Local TCP Listener Ready</green> <magenta>|</magenta> <blue>Addr</blue>: <cyan>%s:%d</cyan>",
		c.cfg.ListenIP,
		c.cfg.ListenPort,
	)

	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go c.handleLocalTCPConn(conn)
	}
}

func (c *Client) handleLocalTCPConn(conn net.Conn) {
	handedOff := false
	defer func() {
		if recovered := recover(); recovered != nil && c.log != nil {
			c.log.Errorf(
				"💥 <red>Local TCP Handler Panic Recovered</red> <magenta>|</magenta> <yellow>%v</yellow>",
				recovered,
			)
		}
		if !handedOff {
			_ = conn.Close()
		}
	}()

	timeout := 10 * time.Second
	_ = conn.SetDeadline(time.Now().Add(timeout))

	streamID, err := c.OpenTCPStream(timeout)
	if err != nil {
		return
	}

	_ = conn.SetDeadline(time.Time{})
	stream := c.createStream(streamID, conn)
	handedOff = true
	go c.runLocalStreamReadLoop(stream, timeout)
}

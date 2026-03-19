// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/BurntSushi/toml"

	"masterdnsvpn-go/internal/compression"
)

type ServerConfig struct {
	ConfigDir                         string   `toml:"-"`
	ConfigPath                        string   `toml:"-"`
	UDPHost                           string   `toml:"UDP_HOST"`
	UDPPort                           int      `toml:"UDP_PORT"`
	UDPReaders                        int      `toml:"UDP_READERS"`
	SocketBufferSize                  int      `toml:"SOCKET_BUFFER_SIZE"`
	MaxConcurrentRequests             int      `toml:"MAX_CONCURRENT_REQUESTS"`
	DNSRequestWorkers                 int      `toml:"DNS_REQUEST_WORKERS"`
	MaxPacketSize                     int      `toml:"MAX_PACKET_SIZE"`
	DropLogIntervalSecs               float64  `toml:"DROP_LOG_INTERVAL_SECONDS"`
	InvalidCookieWindowSecs           float64  `toml:"INVALID_COOKIE_WINDOW_SECONDS"`
	InvalidCookieErrorThreshold       int      `toml:"INVALID_COOKIE_ERROR_THRESHOLD"`
	SessionTimeoutSecs                float64  `toml:"SESSION_TIMEOUT_SECONDS"`
	SessionCleanupIntervalSecs        float64  `toml:"SESSION_CLEANUP_INTERVAL_SECONDS"`
	ClosedSessionRetentionSecs        float64  `toml:"CLOSED_SESSION_RETENTION_SECONDS"`
	MaxPacketsPerBatch                int      `toml:"MAX_PACKETS_PER_BATCH"`
	StreamOutboundWindow              int      `toml:"STREAM_OUTBOUND_WINDOW"`
	StreamOutboundQueueLimit          int      `toml:"STREAM_OUTBOUND_QUEUE_LIMIT"`
	StreamOutboundMaxRetries          int      `toml:"STREAM_OUTBOUND_MAX_RETRIES"`
	StreamOutboundTTLSeconds          float64  `toml:"STREAM_OUTBOUND_TTL_SECONDS"`
	DNSUpstreamServers                []string `toml:"DNS_UPSTREAM_SERVERS"`
	DNSUpstreamTimeoutSecs            float64  `toml:"DNS_UPSTREAM_TIMEOUT"`
	SOCKSConnectTimeoutSecs           float64  `toml:"SOCKS_CONNECT_TIMEOUT"`
	DNSFragmentAssemblyTimeoutSecs    float64  `toml:"DNS_FRAGMENT_ASSEMBLY_TIMEOUT"`
	DNSCacheMaxRecords                int      `toml:"DNS_CACHE_MAX_RECORDS"`
	DNSCacheTTLSeconds                float64  `toml:"DNS_CACHE_TTL_SECONDS"`
	ForwardIP                         string   `toml:"FORWARD_IP"`
	ForwardPort                       int      `toml:"FORWARD_PORT"`
	Domain                            []string `toml:"DOMAIN"`
	MinVPNLabelLength                 int      `toml:"MIN_VPN_LABEL_LENGTH"`
	SupportedUploadCompressionTypes   []int    `toml:"SUPPORTED_UPLOAD_COMPRESSION_TYPES"`
	SupportedDownloadCompressionTypes []int    `toml:"SUPPORTED_DOWNLOAD_COMPRESSION_TYPES"`
	DataEncryptionMethod              int      `toml:"DATA_ENCRYPTION_METHOD"`
	EncryptionKeyFile                 string   `toml:"ENCRYPTION_KEY_FILE"`
	LogLevel                          string   `toml:"LOG_LEVEL"`
}

func defaultServerConfig() ServerConfig {
	workers := min(max(runtime.NumCPU(), 1), 16)

	readers := min(max(runtime.NumCPU()/2, 1), 4)

	return ServerConfig{
		UDPHost:                           "0.0.0.0",
		UDPPort:                           53,
		UDPReaders:                        readers,
		SocketBufferSize:                  8 * 1024 * 1024,
		MaxConcurrentRequests:             4096,
		DNSRequestWorkers:                 workers,
		MaxPacketSize:                     65535,
		DropLogIntervalSecs:               2.0,
		InvalidCookieWindowSecs:           2.0,
		InvalidCookieErrorThreshold:       10,
		SessionTimeoutSecs:                300.0,
		SessionCleanupIntervalSecs:        30.0,
		ClosedSessionRetentionSecs:        600.0,
		MaxPacketsPerBatch:                20,
		StreamOutboundWindow:              4,
		StreamOutboundQueueLimit:          256,
		StreamOutboundMaxRetries:          24,
		StreamOutboundTTLSeconds:          120.0,
		DNSUpstreamServers:                []string{"1.1.1.1:53"},
		DNSUpstreamTimeoutSecs:            4.0,
		SOCKSConnectTimeoutSecs:           8.0,
		DNSFragmentAssemblyTimeoutSecs:    16.0,
		DNSCacheMaxRecords:                2000,
		DNSCacheTTLSeconds:                3600.0,
		ForwardIP:                         "",
		ForwardPort:                       0,
		Domain:                            nil,
		MinVPNLabelLength:                 3,
		SupportedUploadCompressionTypes:   []int{0, 3},
		SupportedDownloadCompressionTypes: []int{0, 3},
		DataEncryptionMethod:              1,
		EncryptionKeyFile:                 "encrypt_key.txt",
		LogLevel:                          "INFO",
	}
}

func LoadServerConfig(filename string) (ServerConfig, error) {
	cfg := defaultServerConfig()
	path, err := filepath.Abs(filename)
	if err != nil {
		return cfg, err
	}

	if _, err := os.Stat(path); err != nil {
		return cfg, fmt.Errorf("config file not found: %s", path)
	}

	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return cfg, fmt.Errorf("parse TOML failed for %s: %w", path, err)
	}

	cfg.ConfigPath = path
	cfg.ConfigDir = filepath.Dir(path)

	if cfg.UDPHost == "" {
		cfg.UDPHost = "0.0.0.0"
	}

	if cfg.UDPPort <= 0 || cfg.UDPPort > 65535 {
		return cfg, fmt.Errorf("invalid UDP_PORT: %d", cfg.UDPPort)
	}

	if cfg.UDPReaders <= 0 {
		cfg.UDPReaders = defaultServerConfig().UDPReaders
	}

	if cfg.SocketBufferSize <= 0 {
		cfg.SocketBufferSize = 8 * 1024 * 1024
	}

	if cfg.MaxConcurrentRequests <= 0 {
		cfg.MaxConcurrentRequests = 4096
	}

	if cfg.DNSRequestWorkers <= 0 {
		cfg.DNSRequestWorkers = defaultServerConfig().DNSRequestWorkers
	}

	if cfg.MaxPacketSize <= 0 {
		cfg.MaxPacketSize = 65535
	}

	if cfg.DropLogIntervalSecs <= 0 {
		cfg.DropLogIntervalSecs = 2.0
	}
	if cfg.InvalidCookieWindowSecs <= 0 {
		cfg.InvalidCookieWindowSecs = 2.0
	}
	if cfg.InvalidCookieErrorThreshold <= 0 {
		cfg.InvalidCookieErrorThreshold = 10
	}
	if cfg.SessionTimeoutSecs <= 0 {
		cfg.SessionTimeoutSecs = 300.0
	}
	if cfg.SessionCleanupIntervalSecs <= 0 {
		cfg.SessionCleanupIntervalSecs = 30.0
	}
	if cfg.ClosedSessionRetentionSecs <= 0 {
		cfg.ClosedSessionRetentionSecs = 600.0
	}
	if cfg.MaxPacketsPerBatch < 1 {
		cfg.MaxPacketsPerBatch = 20
	}
	if cfg.StreamOutboundWindow < 1 {
		cfg.StreamOutboundWindow = 4
	}
	if cfg.StreamOutboundWindow > 32 {
		cfg.StreamOutboundWindow = 32
	}
	if cfg.StreamOutboundQueueLimit < 1 {
		cfg.StreamOutboundQueueLimit = 256
	}
	if cfg.StreamOutboundQueueLimit > 8192 {
		cfg.StreamOutboundQueueLimit = 8192
	}
	if cfg.StreamOutboundMaxRetries < 1 {
		cfg.StreamOutboundMaxRetries = 24
	}
	if cfg.StreamOutboundMaxRetries > 512 {
		cfg.StreamOutboundMaxRetries = 512
	}
	if cfg.StreamOutboundTTLSeconds <= 0 {
		cfg.StreamOutboundTTLSeconds = 120.0
	}
	if len(cfg.DNSUpstreamServers) == 0 {
		cfg.DNSUpstreamServers = []string{"1.1.1.1:53"}
	}
	if cfg.DNSUpstreamTimeoutSecs <= 0 {
		cfg.DNSUpstreamTimeoutSecs = 4.0
	}
	if cfg.SOCKSConnectTimeoutSecs <= 0 {
		cfg.SOCKSConnectTimeoutSecs = 8.0
	}
	if cfg.DNSFragmentAssemblyTimeoutSecs <= 0 {
		cfg.DNSFragmentAssemblyTimeoutSecs = max(10.0, cfg.DNSUpstreamTimeoutSecs*4.0)
	}
	if cfg.DNSCacheMaxRecords < 1 {
		cfg.DNSCacheMaxRecords = 2000
	}
	if cfg.DNSCacheTTLSeconds <= 0 {
		cfg.DNSCacheTTLSeconds = 3600.0
	}
	if cfg.ForwardPort < 0 || cfg.ForwardPort > 65535 {
		return cfg, fmt.Errorf("invalid FORWARD_PORT: %d", cfg.ForwardPort)
	}

	if cfg.MinVPNLabelLength <= 0 {
		cfg.MinVPNLabelLength = 3
	}
	cfg.SupportedUploadCompressionTypes = normalizeCompressionTypeList(cfg.SupportedUploadCompressionTypes)
	cfg.SupportedDownloadCompressionTypes = normalizeCompressionTypeList(cfg.SupportedDownloadCompressionTypes)

	if cfg.DataEncryptionMethod < 0 || cfg.DataEncryptionMethod > 5 {
		cfg.DataEncryptionMethod = 1
	}

	if cfg.EncryptionKeyFile == "" {
		cfg.EncryptionKeyFile = "encrypt_key.txt"
	}

	if cfg.LogLevel == "" {
		cfg.LogLevel = "INFO"
	}

	return cfg, nil
}

func (c ServerConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.UDPHost, c.UDPPort)
}

func (c ServerConfig) DropLogInterval() time.Duration {
	return time.Duration(c.DropLogIntervalSecs * float64(time.Second))
}

func (c ServerConfig) InvalidCookieWindow() time.Duration {
	return time.Duration(c.InvalidCookieWindowSecs * float64(time.Second))
}

func (c ServerConfig) SessionTimeout() time.Duration {
	return time.Duration(c.SessionTimeoutSecs * float64(time.Second))
}

func (c ServerConfig) SessionCleanupInterval() time.Duration {
	return time.Duration(c.SessionCleanupIntervalSecs * float64(time.Second))
}

func (c ServerConfig) ClosedSessionRetention() time.Duration {
	return time.Duration(c.ClosedSessionRetentionSecs * float64(time.Second))
}

func (c ServerConfig) DNSUpstreamTimeout() time.Duration {
	return time.Duration(c.DNSUpstreamTimeoutSecs * float64(time.Second))
}

func (c ServerConfig) SOCKSConnectTimeout() time.Duration {
	return time.Duration(c.SOCKSConnectTimeoutSecs * float64(time.Second))
}

func (c ServerConfig) DNSFragmentAssemblyTimeout() time.Duration {
	return time.Duration(c.DNSFragmentAssemblyTimeoutSecs * float64(time.Second))
}

func (c ServerConfig) StreamOutboundTTL() time.Duration {
	return time.Duration(c.StreamOutboundTTLSeconds * float64(time.Second))
}

func (c ServerConfig) EncryptionKeyPath() string {
	if c.EncryptionKeyFile == "" {
		return filepath.Join(c.ConfigDir, "encrypt_key.txt")
	}
	if filepath.IsAbs(c.EncryptionKeyFile) {
		return c.EncryptionKeyFile
	}
	return filepath.Join(c.ConfigDir, c.EncryptionKeyFile)
}

func normalizeCompressionTypeList(values []int) []int {
	if len(values) == 0 {
		return []int{0}
	}

	seen := [4]bool{}
	out := make([]int, 0, len(values))
	for _, value := range values {
		if value < 0 || value > 3 || seen[value] || !compression.IsTypeAvailable(uint8(value)) {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	if len(out) == 0 {
		return []int{0}
	}
	return out
}

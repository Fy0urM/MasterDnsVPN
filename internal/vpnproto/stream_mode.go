// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package vpnproto

import "bytes"

var tcpForwardSynPayload = []byte{'T', 'C', 'P'}

func TCPForwardSynPayload() []byte {
	return append([]byte(nil), tcpForwardSynPayload...)
}

func IsTCPForwardSynPayload(payload []byte) bool {
	return bytes.Equal(payload, tcpForwardSynPayload)
}

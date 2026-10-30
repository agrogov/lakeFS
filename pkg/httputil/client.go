package httputil

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/treeverse/lakefs/pkg/logging"
)

// ErrInvalidTrustedProxyEntry is returned for a security.trusted_proxies entry that is not a CIDR,
// an IP address or the "*" wildcard.
var ErrInvalidTrustedProxyEntry = errors.New("invalid trusted proxy entry")

// bitsPerByte converts a byte length to a bit length when building a full-length IP mask.
const bitsPerByte = 8

// GetRequestLakeFSClient get lakeFS client identifier from request.
//
//	It extracts the data from X-Lakefs-Client header and fallback to the user-agent
func GetRequestLakeFSClient(r *http.Request) string {
	id := r.Header.Get("X-Lakefs-Client")
	if id == "" {
		id = r.UserAgent()
	}
	return id
}

// ClientIPResolver resolves the client IP of a request without trusting client-supplied headers
// unless the request comes from a configured trusted proxy.
//
// The zero value (and a nil *ClientIPResolver) trusts no proxy: X-Forwarded-For and X-Real-IP are
// ignored and the transport peer address (RemoteAddr) is returned. This is the safe default, because
// any caller can otherwise spoof its source IP and bypass (or evade) an IpAddress/SourceIp policy
// condition.
type ClientIPResolver struct {
	trustedProxies []*net.IPNet
	// trustAll is set by the "*" entry. It keeps the previous behaviour for deployments that
	// deliberately trust every proxy in front of lakeFS.
	trustAll bool
}

// NewClientIPResolver builds a resolver from trusted proxy entries. Each entry is a CIDR, a single
// IP address, or "*" to trust every peer. An empty list trusts no proxy.
func NewClientIPResolver(trustedProxies []string) (*ClientIPResolver, error) {
	r := &ClientIPResolver{}
	for _, raw := range trustedProxies {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		if entry == "*" {
			r.trustAll = true
			continue
		}
		if _, network, err := net.ParseCIDR(entry); err == nil {
			r.trustedProxies = append(r.trustedProxies, network)
			continue
		}
		ip := net.ParseIP(entry)
		if ip == nil {
			return nil, fmt.Errorf("%w: %q: expected a CIDR, an IP address, or \"*\"", ErrInvalidTrustedProxyEntry, entry)
		}
		bits := bitsPerByte * net.IPv6len
		if ip.To4() != nil {
			ip = ip.To4()
			bits = bitsPerByte * net.IPv4len
		}
		r.trustedProxies = append(r.trustedProxies, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
	}
	return r, nil
}

func (r *ClientIPResolver) trusts(ip net.IP) bool {
	if r == nil {
		return false
	}
	if r.trustAll {
		return true
	}
	for _, network := range r.trustedProxies {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

// Extract returns the client IP of a request. The transport peer from remoteAddr is used unless it
// is a trusted proxy; only then are X-Forwarded-For (walked from right to left to the first
// untrusted hop) and X-Real-IP honoured. A malformed X-Forwarded-For header is not trusted at all
// and resolves to the peer address.
func (r *ClientIPResolver) Extract(headers http.Header, remoteAddr string) string {
	peerIP, peerText := parseRemoteAddr(remoteAddr)
	if peerIP == nil {
		// Without a verifiable peer there is nothing safe to trust.
		return ""
	}
	if !r.trusts(peerIP) {
		logging.ContextUnavailable().WithField("client_ip", peerText).Debug("Client IP extracted from RemoteAddr")
		return peerText
	}

	if xForwardedFor := headers.Get("X-Forwarded-For"); xForwardedFor != "" {
		clientIP, ok := r.clientFromXForwardedFor(xForwardedFor)
		if !ok {
			// The trusted proxy sent a malformed chain. Do not fall through to another
			// client-supplied header; use the proxy's own address instead.
			logging.ContextUnavailable().WithField("client_ip", peerText).Warn("Malformed X-Forwarded-For from trusted proxy; using peer address")
			return peerText
		}
		logging.ContextUnavailable().WithField("client_ip", clientIP).Debug("Client IP extracted from X-Forwarded-For")
		return clientIP
	}

	if xRealIP := strings.TrimSpace(headers.Get("X-Real-IP")); xRealIP != "" {
		if ip := net.ParseIP(xRealIP); ip != nil {
			logging.ContextUnavailable().WithField("client_ip", ip.String()).Debug("Client IP extracted from X-Real-IP")
			return ip.String()
		}
	}

	logging.ContextUnavailable().WithField("client_ip", peerText).Debug("Client IP extracted from RemoteAddr")
	return peerText
}

// clientFromXForwardedFor implements the standard trusted-proxy walk: the rightmost entry is the
// closest hop, so scan from the right and return the first address that is not a trusted proxy.
// If every hop is trusted, the leftmost entry is the client.
func (r *ClientIPResolver) clientFromXForwardedFor(value string) (string, bool) {
	parts := strings.Split(value, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		raw := strings.TrimSpace(parts[i])
		ip := net.ParseIP(raw)
		if ip == nil {
			return "", false
		}
		if !r.trusts(ip) {
			return ip.String(), true
		}
	}
	// The whole chain is made of trusted proxies: the leftmost entry is the origin.
	if len(parts) == 0 {
		return "", false
	}
	ip := net.ParseIP(strings.TrimSpace(parts[0]))
	if ip == nil {
		return "", false
	}
	return ip.String(), true
}

// parseRemoteAddr returns the peer IP and its canonical text form. It accepts both "host:port" and
// a bare IPv4/IPv6 address.
func parseRemoteAddr(remoteAddr string) (net.IP, string) {
	if remoteAddr == "" {
		return nil, ""
	}
	host := remoteAddr
	if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = h
	} else if ip := net.ParseIP(remoteAddr); ip != nil {
		return ip, ip.String()
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, host
	}
	return ip, ip.String()
}

// ExtractClientIP extracts the client IP without trusting any proxy. It is the safe default; use a
// ClientIPResolver built with NewClientIPResolver when X-Forwarded-For from configured proxies
// should be honoured.
func ExtractClientIP(headers http.Header, remoteAddr string) string {
	var r *ClientIPResolver
	return r.Extract(headers, remoteAddr)
}

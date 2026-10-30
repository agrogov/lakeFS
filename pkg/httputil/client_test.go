package httputil_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/httputil"
)

func newResolver(t *testing.T, trustedProxies ...string) *httputil.ClientIPResolver {
	t.Helper()
	r, err := httputil.NewClientIPResolver(trustedProxies)
	require.NoError(t, err)
	return r
}

func TestClientIPResolverExtract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		proxies       []string
		remoteAddr    string
		xForwardedFor string
		xRealIP       string
		want          string
	}{
		{
			name:       "no XFF and no trusted proxies uses RemoteAddr",
			remoteAddr: "192.0.2.9:1234",
			want:       "192.0.2.9",
		},
		{
			name:          "XFF is ignored because the peer is not a trusted proxy",
			remoteAddr:    "192.0.2.9:1234",
			xForwardedFor: "10.1.2.3",
			want:          "192.0.2.9",
		},
		{
			name:       "X-Real-IP is ignored because the peer is not a trusted proxy",
			remoteAddr: "192.0.2.9:1234",
			xRealIP:    "10.1.2.3",
			want:       "192.0.2.9",
		},
		{
			name:          "XFF is honoured because the peer is a trusted proxy",
			proxies:       []string{"192.0.2.9/32"},
			remoteAddr:    "192.0.2.9:1234",
			xForwardedFor: "10.1.2.3",
			want:          "10.1.2.3",
		},
		{
			name:          "XFF is honoured for a trusted proxy CIDR",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.20.30.40:1234",
			xForwardedFor: "203.0.113.7",
			want:          "203.0.113.7",
		},
		{
			name:          "XFF is honoured for a trusted proxy given as a single IP",
			proxies:       []string{"10.20.30.40"},
			remoteAddr:    "10.20.30.40:1234",
			xForwardedFor: "203.0.113.7",
			want:          "203.0.113.7",
		},
		{
			name:          "chain uses only the rightmost untrusted hop",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.0.0.1:1234",
			xForwardedFor: "203.0.113.7, 198.51.100.4, 10.0.0.2",
			want:          "198.51.100.4",
		},
		{
			name:          "spoofed leftmost entry resolves to the real client",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.0.0.1:1234",
			xForwardedFor: "203.0.113.5, 192.0.2.9",
			want:          "192.0.2.9",
		},
		{
			name:          "spoofed leftmost entry inside the trusted range is skipped",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.0.0.1:1234",
			xForwardedFor: "10.1.2.3, 192.0.2.9",
			want:          "192.0.2.9",
		},
		{
			name:          "all hops trusted returns the leftmost entry",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.0.0.1:1234",
			xForwardedFor: "10.1.2.3, 10.0.0.2",
			want:          "10.1.2.3",
		},
		{
			name:          "trust all proxies keeps the previous leftmost behaviour",
			proxies:       []string{"*"},
			remoteAddr:    "192.0.2.9:1234",
			xForwardedFor: "10.1.2.3, 192.0.2.9",
			want:          "10.1.2.3",
		},
		{
			name:          "malformed XFF from a trusted proxy falls back to the peer",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.0.0.1:1234",
			xForwardedFor: "not-an-ip",
			want:          "10.0.0.1",
		},
		{
			name:          "malformed rightmost XFF entry falls back to the peer",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.0.0.1:1234",
			xForwardedFor: "192.0.2.9, not-an-ip",
			want:          "10.0.0.1",
		},
		{
			name:          "malformed leftmost XFF entry does not hide the real client",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.0.0.1:1234",
			xForwardedFor: "not-an-ip, 192.0.2.9",
			want:          "192.0.2.9",
		},
		{
			name:          "empty XFF entry is malformed",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.0.0.1:1234",
			xForwardedFor: "192.0.2.9,",
			want:          "10.0.0.1",
		},
		{
			name:       "IPv6 peer without XFF",
			remoteAddr: "[2001:db8::1]:1234",
			want:       "2001:db8::1",
		},
		{
			name:          "IPv6 XFF is honoured from a trusted IPv6 proxy",
			proxies:       []string{"2001:db8::1/128"},
			remoteAddr:    "[2001:db8::1]:1234",
			xForwardedFor: "2001:db8::2",
			want:          "2001:db8::2",
		},
		{
			name:          "IPv6 XFF is ignored from an untrusted IPv6 peer",
			proxies:       []string{"2001:db8:1::/48"},
			remoteAddr:    "[2001:db8::1]:1234",
			xForwardedFor: "2001:db8::2",
			want:          "2001:db8::1",
		},
		{
			name:          "IPv6 CIDR matching walks the chain",
			proxies:       []string{"2001:db8:1::/48"},
			remoteAddr:    "[2001:db8:1::5]:1234",
			xForwardedFor: "2001:db8:9::7, 2001:db8:1::6",
			want:          "2001:db8:9::7",
		},
		{
			name:       "bare IPv4 RemoteAddr without a port",
			remoteAddr: "192.0.2.9",
			want:       "192.0.2.9",
		},
		{
			name:       "bare IPv6 RemoteAddr without a port",
			remoteAddr: "2001:db8::1",
			want:       "2001:db8::1",
		},
		{
			name:          "empty RemoteAddr is not replaced by a client-supplied header",
			remoteAddr:    "",
			xForwardedFor: "10.1.2.3",
			want:          "",
		},
		{
			name:       "X-Real-IP is honoured when XFF is absent and the peer is trusted",
			proxies:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:1234",
			xRealIP:    "203.0.113.7",
			want:       "203.0.113.7",
		},
		{
			name:       "malformed X-Real-IP from a trusted proxy falls back to the peer",
			proxies:    []string{"10.0.0.0/8"},
			remoteAddr: "10.0.0.1:1234",
			xRealIP:    "not-an-ip",
			want:       "10.0.0.1",
		},
		{
			name:          "XFF wins over X-Real-IP when both are present",
			proxies:       []string{"10.0.0.0/8"},
			remoteAddr:    "10.0.0.1:1234",
			xForwardedFor: "203.0.113.7",
			xRealIP:       "198.51.100.4",
			want:          "203.0.113.7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			headers := http.Header{}
			if tt.xForwardedFor != "" {
				headers.Set("X-Forwarded-For", tt.xForwardedFor)
			}
			if tt.xRealIP != "" {
				headers.Set("X-Real-IP", tt.xRealIP)
			}
			got := newResolver(t, tt.proxies...).Extract(headers, tt.remoteAddr)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestNewClientIPResolverRejectsInvalidEntries(t *testing.T) {
	t.Parallel()
	for _, entry := range []string{"not-an-ip", "10.0.0.0/33", "2001:db8::/129"} {
		_, err := httputil.NewClientIPResolver([]string{entry})
		require.Error(t, err, "entry %q should be rejected", entry)
	}
}

func TestExtractClientIPDefaultsToNoTrustedProxy(t *testing.T) {
	t.Parallel()
	headers := http.Header{}
	headers.Set("X-Forwarded-For", "10.1.2.3")
	require.Equal(t, "192.0.2.9", httputil.ExtractClientIP(headers, "192.0.2.9:1234"))
}

func TestClientIPResolverZeroValueTrustsNothing(t *testing.T) {
	t.Parallel()
	var r httputil.ClientIPResolver
	headers := http.Header{}
	headers.Set("X-Forwarded-For", "10.1.2.3")
	require.Equal(t, "192.0.2.9", r.Extract(headers, "192.0.2.9:1234"))
}

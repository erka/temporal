package rpc

import (
	"context"
	"crypto/tls"
	"net"
	"net/http/httptrace"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

func TestDialTracer_TraceLifecycle(t *testing.T) {
	t.Parallel()

	addr := "localhost:1234"
	dt := newDialTracer(addr, metrics.NoopMetricsHandler, log.NewNoopLogger())

	ctx := context.Background()
	ctx, ndt := dt.beginNetworkDial(ctx)

	trace := httptrace.ContextClientTrace(ctx)
	require.NotNil(t, trace)

	trace.DNSStart(httptrace.DNSStartInfo{})
	rewind(&ndt.dnsStart)
	trace.DNSDone(httptrace.DNSDoneInfo{
		Addrs: []net.IPAddr{{IP: net.IPv4(127, 0, 0, 1)}},
	})

	trace.ConnectStart("tcp", addr)
	rewind(&ndt.connectStart)
	trace.ConnectDone("tcp", addr, nil)

	trace.TLSHandshakeStart()
	rewind(&ndt.tlsStart)
	trace.TLSHandshakeDone(tls.ConnectionState{Version: tls.VersionTLS13}, nil)

	trace.GotConn(httptrace.GotConnInfo{Reused: true})

	dt.endNetworkDial(ndt, nil)

	assert.Positive(t, ndt.dnsDuration)
	assert.Positive(t, ndt.connectDuration)
	assert.Positive(t, ndt.tlsDuration)
	assert.True(t, ndt.connReused)
}

func TestDialTracer_NoDNSResolution(t *testing.T) {
	t.Parallel()

	addr := "127.0.0.1:1234"
	// This could happen if the target is already an IP address
	dt := newDialTracer(addr, metrics.NoopMetricsHandler, log.NewNoopLogger())

	ctx := context.Background()
	ctx, ndt := dt.beginNetworkDial(ctx)

	trace := httptrace.ContextClientTrace(ctx)
	require.NotNil(t, trace)

	// Skip DNSStart/DNSDone entirely - this happens when target is already an IP
	trace.ConnectStart("tcp", addr)
	rewind(&ndt.connectStart)
	trace.ConnectDone("tcp", addr, nil)

	trace.TLSHandshakeStart()
	rewind(&ndt.tlsStart)
	trace.TLSHandshakeDone(tls.ConnectionState{Version: tls.VersionTLS13}, nil)

	trace.GotConn(httptrace.GotConnInfo{Reused: false})

	dt.endNetworkDial(ndt, nil)

	assert.Zero(t, ndt.dnsDuration)
	assert.Positive(t, ndt.connectDuration)
	assert.Positive(t, ndt.tlsDuration)
	assert.False(t, ndt.connReused)
}

// rewind moves a timestamp back by 1 millisecond to ensure positive durations in tests
func rewind(ts *time.Time) {
	*ts = ts.Add(-time.Millisecond)
}

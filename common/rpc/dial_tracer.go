package rpc

import (
	"context"
	"crypto/tls"
	"net"
	"net/http/httptrace"
	"strings"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/zap/zapcore"
)

type dialTracer struct {
	address        string
	metricsHandler metrics.Handler
	logger         log.Logger
}

// newDialTracer creates a dial tracer that logs errors during dial and produces metrics for different stages of the dial process (DNS, Connect, TLS)
func newDialTracer(
	address string,
	mh metrics.Handler,
	logger log.Logger,
) *dialTracer {
	l := log.With(
		logger,
		tag.NewStringTag("service", "client"),
		tag.NewStringTag("address", address),
	)

	return &dialTracer{
		address:        address,
		metricsHandler: mh,
		logger:         l,
	}
}

func (d *dialTracer) beginNetworkDial(ctx context.Context) (context.Context, *networkDialTrace) {
	ndt := &networkDialTrace{startedAt: time.Now()}

	// Build a ClientTrace capturing DNS, TCP, TLS, and connection reuse.
	trace := &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) {
			ndt.dnsStart = time.Now()
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			// DNSStart may not have been called if the address is already an IP address
			if !ndt.dnsStart.IsZero() {
				ndt.dnsDuration = time.Since(ndt.dnsStart)
			}
			ndt.dnsAddrs = info.Addrs
			ndt.dnsErr = info.Err
		},
		ConnectStart: func(_, _ string) {
			ndt.connectStart = time.Now()
		},
		ConnectDone: func(_ string, addr string, err error) {
			// ConnectStart may not have been called if the address is reused
			if !ndt.connectStart.IsZero() {
				ndt.connectDuration = time.Since(ndt.connectStart)
			}
			ndt.connectAddr = addr
			ndt.connectErr = err
		},
		TLSHandshakeStart: func() {
			ndt.tlsStart = time.Now()
		},
		TLSHandshakeDone: func(cs tls.ConnectionState, err error) {
			if !ndt.tlsStart.IsZero() {
				ndt.tlsDuration = time.Since(ndt.tlsStart)
			}
			ndt.tlsErr = err
		},
		GotConn: func(info httptrace.GotConnInfo) {
			ndt.connReused = info.Reused
		},
	}

	metrics.DialAttemptsCount.With(d.metricsHandler).Record(1)
	return httptrace.WithClientTrace(ctx, trace), ndt
}

func (d *dialTracer) endNetworkDial(ndt *networkDialTrace, dialErr error) {
	total := time.Since(ndt.startedAt)

	fields := []tag.Tag{
		tag.NewDurationTag("totalDuration", total),
		tag.NewAnyTag("networkDialTrace", ndt),
	}

	if dialErr != nil {
		fields = append(fields, tag.Error(dialErr), tag.ErrorType(dialErr))
		d.logger.Warn("network dial error", fields...)
		metrics.DialErrorCount.With(d.metricsHandler).Record(1)
	} else {
		metrics.DialSuccessCount.With(d.metricsHandler).Record(1)
	}

	if ndt.dnsDuration > 0 {
		metrics.DialDNSLatency.With(d.metricsHandler).Record(ndt.dnsDuration)
	}
	if ndt.connectDuration > 0 {
		metrics.DialConnectLatency.With(d.metricsHandler).Record(ndt.connectDuration)
	}
	if ndt.tlsDuration > 0 {
		metrics.DialTLSLatency.With(d.metricsHandler).Record(ndt.tlsDuration)
	}
}

type networkDialTrace struct {
	startedAt time.Time

	dnsStart    time.Time
	dnsDuration time.Duration
	dnsAddrs    []net.IPAddr
	dnsErr      error

	connectStart    time.Time
	connectDuration time.Duration
	connectAddr     string
	connectErr      error
	connReused      bool

	tlsStart    time.Time
	tlsDuration time.Duration
	tlsErr      error
}

func (c *networkDialTrace) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddDuration("dnsDuration", c.dnsDuration)
	if len(c.dnsAddrs) > 0 {
		var ips []string
		for _, ip := range c.dnsAddrs {
			ips = append(ips, ip.String())
		}
		enc.AddString("dnsAddrs", strings.Join(ips, ","))
	}
	if c.dnsErr != nil {
		enc.AddString("dnsErr", c.dnsErr.Error())
	}

	enc.AddDuration("connectDuration", c.connectDuration)
	enc.AddString("connectAddr", c.connectAddr)
	enc.AddBool("connReused", c.connReused)
	if c.connectErr != nil {
		enc.AddString("connectErr", c.connectErr.Error())
	}

	enc.AddDuration("tlsDuration", c.tlsDuration)
	if c.tlsErr != nil {
		enc.AddString("tlsErr", c.tlsErr.Error())
	}
	return nil
}

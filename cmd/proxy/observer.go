package main

import (
	"errors"
	"io"
	"net"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	activeConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_proxy_active_connections",
		Help: "Number of currently active client connections",
	})
	packetsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_packets_total",
		Help: "Total count of ClickHouse protocol packets processed (client -> server)",
	}, []string{"type"})
	bytesTransferred = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_bytes_transferred_total",
		Help: "Total bytes transferred through the proxy",
	}, []string{"direction"})

	// Observability Metrics
	upstreamHealth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_proxy_upstream_health",
		Help: "1 if upstream ClickHouse is reachable, 0 otherwise",
	})
	queriesForwarded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_proxy_queries_forwarded_total",
		Help: "Total number of Query packets successfully written to upstream",
	})
	serverPacketsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_server_packets_total",
		Help: "Total count of ClickHouse protocol packets processed (server -> client)",
	}, []string{"type"})
	errorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_errors_total",
		Help: "Total count of errors encountered",
	}, []string{"type", "error"})

	// Streaming protocol metrics
	queryDecodeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "clickhouse_proxy_query_decode_duration_seconds",
		Help:    "Time spent decoding Query packets in streaming mode",
		Buckets: prometheus.DefBuckets,
	})
	rewriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "clickhouse_proxy_rewrite_duration_seconds",
		Help:    "Time spent on SQL rewriting via gRPC service",
		Buckets: prometheus.DefBuckets,
	})
	fallbackTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_fallback_total",
		Help: "Total count of fallbacks to raw copy mode",
	}, []string{"reason"})
	streamingDataBlocksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_streaming_data_blocks_total",
		Help: "Total count of Data/Scalar blocks processed in streaming mode",
	}, []string{"mode"})
	handshakeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "clickhouse_proxy_handshake_duration_seconds",
		Help:    "Time spent on Hello/ServerHello/Addendum handshake",
		Buckets: prometheus.DefBuckets,
	})
)

func init() {
	prometheus.MustRegister(activeConnections)
	prometheus.MustRegister(packetsTotal)
	prometheus.MustRegister(bytesTransferred)
	prometheus.MustRegister(upstreamHealth)
	prometheus.MustRegister(queriesForwarded)
	prometheus.MustRegister(serverPacketsTotal)
	prometheus.MustRegister(errorsTotal)
	prometheus.MustRegister(queryDecodeDuration)
	prometheus.MustRegister(rewriteDuration)
	prometheus.MustRegister(fallbackTotal)
	prometheus.MustRegister(streamingDataBlocksTotal)
	prometheus.MustRegister(handshakeDuration)
}

type MetricsObserver struct{}

func NewMetricsObserver() *MetricsObserver {
	return &MetricsObserver{}
}

func (m *MetricsObserver) ConnectionOpened() {
	activeConnections.Inc()
}

func (m *MetricsObserver) ConnectionClosed() {
	activeConnections.Dec()
}

func (m *MetricsObserver) ClientPacket(pktType string) {
	packetsTotal.WithLabelValues(pktType).Inc()
}

func (m *MetricsObserver) ServerPacket(pktType string) {
	serverPacketsTotal.WithLabelValues(pktType).Inc()
}

func (m *MetricsObserver) BytesTransferred(direction string, bytes float64) {
	bytesTransferred.WithLabelValues(direction).Add(bytes)
}

func (m *MetricsObserver) QueryForwarded() {
	queriesForwarded.Inc()
}

func (m *MetricsObserver) Error(phase string, err error) {
	if err == nil {
		return
	}
	// Filter out harmless errors if desired, or classify everything
	// Note: io.EOF is standard close, not necessarily an error, but let's classify it.
	errorsTotal.WithLabelValues(phase, classifyError(err)).Inc()
}

func (m *MetricsObserver) SetUpstreamHealth(healthy bool) {
	val := 0.0
	if healthy {
		val = 1.0
	}
	upstreamHealth.Set(val)
}

func classifyError(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		return "timeout"
	}
	if errors.Is(err, net.ErrClosed) {
		return "closed"
	}
	return "other"
}

// Streaming protocol observer methods

func (m *MetricsObserver) QueryDecoded(duration float64) {
	queryDecodeDuration.Observe(duration)
}

func (m *MetricsObserver) Rewritten(duration float64) {
	rewriteDuration.Observe(duration)
}

func (m *MetricsObserver) Fallback(reason string) {
	fallbackTotal.WithLabelValues(reason).Inc()
}

func (m *MetricsObserver) StreamingDataBlock(mode string) {
	streamingDataBlocksTotal.WithLabelValues(mode).Inc()
}

func (m *MetricsObserver) HandshakeCompleted(duration float64) {
	handshakeDuration.Observe(duration)
}

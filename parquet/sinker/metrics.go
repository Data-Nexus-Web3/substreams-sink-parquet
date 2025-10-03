package sinker

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	blocksProcessedGauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_parquet_blocks_processed_total", Help: "Total blocks processed"})
	rowsWrittenGauge     = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_parquet_rows_written_total", Help: "Total rows written"})
	bytesWrittenGauge    = prometheus.NewGauge(prometheus.GaugeOpts{Name: "sink_parquet_bytes_written_total", Help: "Total bytes written"})
)

func init() {
	prometheus.MustRegister(
		blocksProcessedGauge,
		rowsWrittenGauge,
		bytesWrittenGauge,
	)
}

func startPrometheusServer() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":9102", Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
}

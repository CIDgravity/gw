package s3

import (
	"context"
	"errors"
	"net/http"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"
	"go.uber.org/zap/zapcore"
)

func RequestLogger(next http.Handler) http.Handler {
	// Note this dynamically checks the log level, so that we can change it on
	// demand via the cli
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if log.Level() == zapcore.DebugLevel {
			log.Debugw("HTTP request",
				"method", r.Method,
				"URL", r.URL.String(),
				"client", requestClient(r),
				"headers", r.Header)
		}

		log.Infow("HTTP request",
			"method", r.Method,
			"URL", r.URL.String(),
			"client", requestClient(r),
		)

		next.ServeHTTP(w, r)
	})
}

func requestClient(r *http.Request) string {
	cli := r.RemoteAddr
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		cli += " via " + fwd
	}

	return cli
}

func RequestMetrics(next http.Handler) http.Handler {
	reqTotal := promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "fgw",
		Subsystem: "s3",
		Name:      "http_requests_total",
		Help:      "Total number of S3 HTTP requests.",
	},
		[]string{"code", "method"},
	)
	reqDuration := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "fgw",
		Subsystem: "s3",
		Name:      "http_request_duration_seconds",
		Help:      "Duration of S3 HTTP requests.",
		Buckets:   prometheus.DefBuckets,
	},
		[]string{"code", "method"},
	)
	inFlight := promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "fgw",
		Subsystem: "s3",
		Name:      "http_requests_in_flight",
		Help:      "Current number of S3 HTTP requests in flight.",
	})
	durationToHeader := promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "fgw",
		Subsystem: "s3",
		Name:      "http_request_duration_to_header_seconds",
		Help:      "Duration until the response headers for S3 requests are written",
		Buckets:   prometheus.DefBuckets,
	},
		[]string{"code", "method"})

	next = promhttp.InstrumentHandlerCounter(reqTotal, next)
	next = promhttp.InstrumentHandlerDuration(reqDuration, next)
	next = promhttp.InstrumentHandlerInFlight(inFlight, next)
	next = promhttp.InstrumentHandlerTimeToWriteHeader(durationToHeader, next)
	return next
}

func StartS3Server(lc fx.Lifecycle, srv *S3Server) {
	log.Info("Starting S3 server")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", srv.handleGet)
	mux.HandleFunc("PUT /", srv.handlePut)
	mux.HandleFunc("POST /", srv.handlePost)
	mux.HandleFunc("DELETE /", srv.handleDelete)
	mux.HandleFunc("HEAD /", srv.handleHead)

	cfg := configuration.GetConfig()
	httpSrv := &http.Server{
		Addr:    cfg.S3API.BindAddr,
		Handler: RequestMetrics(RequestLogger(mux)),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				log.Info("S3 HTTP server at http://localhost:8078")

				err := httpSrv.ListenAndServe()
				if errors.Is(err, http.ErrServerClosed) {
					log.Info("S3 HTTP server closed")
				} else if err != nil {
					log.Errorf("failed to start S3 HTTP server: %s", err)
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			return httpSrv.Shutdown(ctx)
		},
	})

	log.Info("S3 server started")
}

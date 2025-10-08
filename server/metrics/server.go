package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/CIDgravity/filecoin-gateway/configuration"
	logging "github.com/ipfs/go-log/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"
)

var log = logging.Logger("gw/metrics")

func StartPrometheusServer(lc fx.Lifecycle) error {
	config := configuration.GetConfig().Prometheus

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	httpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", config.Port),
		Handler: mux,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				log.Infof("Prometheus HTTP server at http://localhost:%d/metrics", config.Port)

				err := httpSrv.ListenAndServe()
				if errors.Is(err, http.ErrServerClosed) {
					log.Info("Prometheus HTTP server closed")
				} else if err != nil {
					log.Errorf("failed to start Prometheus HTTP server: %s", err)
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			return httpSrv.Shutdown(ctx)
		},
	})

	log.Info("Prometheus server started")
	return nil
}

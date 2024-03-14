package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/lmittmann/tint"
	"github.com/prometheus/client_golang/prometheus"
	prom "github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"

	"golang.org/x/time/rate"
)

type TraceEvent struct {
	Type   string
	PeerID string
	Data   []byte
}

func TestRate(t *testing.T) {
	r := rate.NewLimiter(1000, 1000)
	t.Log(r.Tokens())

	res := r.ReserveN(time.Now(), 500)
	t.Log(r.Tokens())
	t.Log(res.Delay())
	res = r.ReserveN(time.Now(), 1000)
	t.Log(r.Tokens())
	t.Log(res.Delay())
}

func TestProducer(t *testing.T) {
	ctx := testCtx(t)
	registry := prometheus.NewRegistry()

	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry

	opts := []promexp.Option{
		promexp.WithRegisterer(registry), // actually unnecessary, as we overwrite the default values above
		promexp.WithNamespace("hermes"),
	}

	exporter, err := promexp.New(opts...)
	require.NoError(t, err)

	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName("hermes"),
	))
	require.NoError(t, err)

	options := []sdkmetrics.Option{
		sdkmetrics.WithReader(exporter),
		sdkmetrics.WithResource(res),
	}

	provider := sdkmetrics.NewMeterProvider(options...)

	otel.SetMeterProvider(provider)

	mux := http.NewServeMux()

	mux.Handle("/metrics", prom.Handler())

	addr := fmt.Sprintf("%s:%d", "localhost", 6060)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		slog.Info("Starting metrics server", "addr", fmt.Sprintf("http://%s/metrics", addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "err", err.Error())
		}
	}()

	awsConfig, err := config.LoadDefaultConfig(ctx)
	require.NoError(t, err)
	client := kinesis.NewFromConfig(awsConfig)
	cfg := DefaultProducerConfig()
	cfg.ShardUpdateInterval = 3 * time.Second
	cfg.Concurrency = 16
	cfg.Notifiee = &NotifieeBundle{
		DroppedRecordF: func(record Record) {
			slog.Warn("DROPPED RECORD")
		},
	}
	// cfg.AggregateMaxSize = 0 // no aggregation

	cfg.Log = slog.New(tint.NewHandler(os.Stderr, &tint.Options{Level: slog.LevelDebug}))
	slog.SetDefault(cfg.Log)

	p, err := NewProducer(client, "test-stream-100", cfg)
	require.NoError(t, err)

	stopCtx, stop := context.WithCancel(ctx)
	startProducer(t, stopCtx, p)

	for i := 0; i < 100_000; i++ {
		evt := &TraceEvent{
			Type:   fmt.Sprintf("EVENT_%05d", i),
			PeerID: fmt.Sprintf("PEERID_%05d", i),
			// PeerID: "P",
			Data: make([]byte, 1e3),
		}

		data, err := json.Marshal(evt)
		require.NoError(t, err)

		err = p.Put(ctx, evt.PeerID, data)
		require.NoError(t, err)
	}

	t.Log("wait")
	select {
	case <-time.After(10 * time.Second):
		slog.Error("STOPPING")
		stop()
	case <-ctx.Done():
		slog.Info("Done")
	}

	time.Sleep(time.Minute)
}

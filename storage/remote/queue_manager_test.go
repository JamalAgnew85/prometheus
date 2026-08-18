package remote

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestQueueManagerMetrics(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&requestCount, 1)
		if count == 1 {
			w.WriteHeader(http.StatusOK)
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					conn.Close()
					return
				}
			}
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	succeededSamples := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_remote_storage_succeeded_samples_total",
	})
	sentBytes := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "prometheus_remote_storage_sent_bytes_total",
	})

	client := NewClient(server.URL, server.Client())
	qm := NewQueueManager(client, QueueManagerConfig{
		MaxRetries:   3,
		BatchSize:    10,
		RetryBackoff: 1 * time.Millisecond,
	}, succeededSamples, sentBytes)

	samples := make([]Sample, 10)
	payload := []byte("test-payload")

	err := qm.SendSamplesWithRetry(context.Background(), samples, payload)
	if err != nil {
		t.Logf("SendSamplesWithRetry returned error: %v", err)
	}

	succeededVal := testutil.ToFloat64(succeededSamples)
	sentBytesVal := testutil.ToFloat64(sentBytes)

	if succeededVal != 10 {
		t.Errorf("Expected succeeded samples to be 10, got %f", succeededVal)
	}
	if sentBytesVal != float64(len(payload)) {
		t.Errorf("Expected sent bytes to be %d, got %f", len(payload), sentBytesVal)
	}

	finalRequestCount := atomic.LoadInt32(&requestCount)
	if finalRequestCount < 2 {
		t.Errorf("Expected at least 2 requests (1 retry), got %d", finalRequestCount)
	}
}

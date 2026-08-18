package remote

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Sample represents a single sample.
type Sample struct {
	Value     float64
	Timestamp int64
}

// QueueManager manages sending samples to remote storage.
type QueueManager struct {
	client *Client
	cfg    QueueManagerConfig

	// Metrics
	succeededSamplesTotal prometheus.Counter
	sentBytesTotal        prometheus.Counter

	mu sync.Mutex
}

type QueueManagerConfig struct {
	MaxRetries    int
	BatchSize     int
	RetryBackoff  time.Duration
}

func NewQueueManager(client *Client, cfg QueueManagerConfig, succeededSamples prometheus.Counter, sentBytes prometheus.Counter) *QueueManager {
	return &QueueManager{
		client:                client,
		cfg:                   cfg,
		succeededSamplesTotal: succeededSamples,
		sentBytesTotal:        sentBytes,
	}
}

// SendSamplesWithRetry sends a batch of samples with retries.
// It ensures that metrics are updated atomically and without double-counting.
func (qm *QueueManager) SendSamplesWithRetry(ctx context.Context, samples []Sample, payload []byte) error {
	// 1. Get the exact number of samples and payload size before sending.
	// This avoids using shared/pooled slices that might be mutated concurrently.
	numSamples := len(samples)
	payloadSize := len(payload)

	var err error
	var statusCode int
	var sentBytes int

	// Track whether this batch has already been successfully received by the remote endpoint
	// to prevent double-counting upon subsequent retries.
	var succeeded bool

	for attempt := 0; attempt <= qm.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(qm.cfg.RetryBackoff):
			}
		}

		statusCode, sentBytes, err = qm.client.Store(ctx, payload)

		// Check if the request succeeded (2xx response)
		if statusCode >= 200 && statusCode < 300 {
			if !succeeded {
				// First time we get a 2xx response for this batch, update metrics.
				qm.incrementSucceeded(numSamples, payloadSize)
				succeeded = true
			}

			// If there was no error (e.g. body read succeeded), we are done.
			if err == nil {
				return nil
			}
			// If there was an error (e.g. body read failed/timeout), we might retry,
			// but since succeeded is true, subsequent attempts won't double-count.
			continue
		}

		// If it's a non-2xx error, we don't mark as succeeded, and we retry.
	}

	if err != nil {
		return err
	}
	return nil
}

// incrementSucceeded updates the metrics atomically.
func (qm *QueueManager) incrementSucceeded(samples int, bytes int) {
	qm.succeededSamplesTotal.Add(float64(samples))
	qm.sentBytesTotal.Add(float64(bytes))
}

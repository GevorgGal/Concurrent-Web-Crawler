package worker

import (
	"context"
	"time"

	"github.com/GevorgGal/crawler/internal/crawler/types"
	"go.uber.org/zap"
)

// Worker represents an individual crawler worker
type Worker struct {
	ID         int
	work       chan *types.URLItem
	results    chan<- *types.CrawlResult
	processor  types.URLProcessor
	logger     *zap.Logger
	processing bool
}

// NewWorker creates a new worker
func NewWorker(id int, work chan *types.URLItem, results chan<- *types.CrawlResult,
	processor types.URLProcessor, logger *zap.Logger) *Worker {
	return &Worker{
		ID:        id,
		work:      work,
		results:   results,
		processor: processor,
		logger:    logger.With(zap.Int("worker_id", id)),
	}
}

// Start begins the worker's processing loop
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Debug("worker starting")
	defer w.logger.Debug("worker stopped")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item, ok := <-w.work:
			if !ok {
				return ErrPoolClosed
			}
			if err := w.processItem(ctx, item); err != nil {
				w.logger.Error("processing error",
					zap.String("url", item.URL),
					zap.Error(err))
			}
		}
	}
}

// processItem processes a single URL
func (w *Worker) processItem(ctx context.Context, item *types.URLItem) error {
	w.processing = true
	defer func() { w.processing = false }()

	// Create timeout context for this request
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	startTime := time.Now()
	result, err := w.processor.Process(reqCtx, item)
	processingTime := time.Since(startTime)

	if err != nil {
		result = &types.CrawlResult{
			URL:          item.URL,
			Error:        err,
			ResponseTime: processingTime,
			Timestamp:    time.Now(),
		}
	}

	// Send result even if there was an error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.results <- result:
		return nil
	}
}

// IsProcessing returns whether the worker is currently processing a URL
func (w *Worker) IsProcessing() bool {
	return w.processing
}

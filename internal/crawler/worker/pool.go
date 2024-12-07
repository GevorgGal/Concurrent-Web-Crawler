package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GevorgGal/crawler/internal/crawler/types"
	"go.uber.org/zap"
)

// Error definitions
var (
	ErrPoolClosed      = errors.New("worker pool is closed")
	ErrWorkerStopped   = errors.New("worker stopped")
	ErrNoWorkersActive = errors.New("no workers active")
)

// Config holds the worker pool configuration
type Config struct {
	NumWorkers      int           // Number of worker goroutines
	QueueSize       int           // Size of the work queue
	MaxRetries      int           // Maximum number of retries per URL
	RequestTimeout  time.Duration // Timeout for individual requests
	ShutdownTimeout time.Duration // Timeout for graceful shutdown
}

// Stats tracks worker pool statistics
type Stats struct {
	ActiveWorkers  atomic.Int32
	CompletedTasks atomic.Int64
	FailedTasks    atomic.Int64
	RetryCount     atomic.Int64
	ProcessingTime atomic.Int64 // Total processing time in nanoseconds
}

// WorkerPool manages a pool of workers for crawling
type WorkerPool struct {
	workers   []*Worker
	workQueue chan *types.URLItem
	results   chan *types.CrawlResult
	processor types.URLProcessor
	stats     *Stats
	logger    *zap.Logger
	config    *Config
	wg        sync.WaitGroup
	shutdown  atomic.Bool
	mu        sync.RWMutex
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(cfg *Config, processor types.URLProcessor, logger *zap.Logger) (*WorkerPool, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	pool := &WorkerPool{
		workQueue: make(chan *types.URLItem, cfg.QueueSize),
		results:   make(chan *types.CrawlResult, cfg.QueueSize),
		processor: processor,
		stats:     &Stats{},
		logger:    logger,
		config:    cfg,
		workers:   make([]*Worker, cfg.NumWorkers),
	}

	// Initialize workers
	for i := 0; i < cfg.NumWorkers; i++ {
		worker := NewWorker(i, pool.workQueue, pool.results, processor, logger)
		pool.workers[i] = worker
	}

	return pool, nil
}

func validateConfig(cfg *Config) error {
	if cfg.NumWorkers <= 0 {
		return fmt.Errorf("number of workers must be positive")
	}
	if cfg.QueueSize <= 0 {
		return fmt.Errorf("queue size must be positive")
	}
	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("request timeout must be positive")
	}
	return nil
}

// Start starts all workers and begins processing
func (p *WorkerPool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.shutdown.Load() {
		return ErrPoolClosed
	}

	// Start all workers
	for _, w := range p.workers {
		p.wg.Add(1)
		go func(worker *Worker) {
			defer p.wg.Done()
			if err := worker.Start(ctx); err != nil {
				p.logger.Error("worker error",
					zap.Int("worker_id", worker.ID),
					zap.Error(err))
			}
		}(w)
	}

	// Start result collector
	p.wg.Add(1)
	go p.collectResults(ctx)

	return nil
}

// Submit submits a URL for crawling
func (p *WorkerPool) Submit(ctx context.Context, item *types.URLItem) error {
	if p.shutdown.Load() {
		return ErrPoolClosed
	}

	if err := item.ValidateURLItem(); err != nil {
		return fmt.Errorf("invalid URL item: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.workQueue <- item:
		return nil
	}
}

// collectResults handles completed crawl results
func (p *WorkerPool) collectResults(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case result := <-p.results:
			p.processResult(result)
		}
	}
}

// processResult handles an individual crawl result
func (p *WorkerPool) processResult(result *types.CrawlResult) {
	if result.Error != nil {
		p.stats.FailedTasks.Add(1)
		p.logger.Error("crawl error",
			zap.String("url", result.URL),
			zap.Error(result.Error))
	} else {
		p.stats.CompletedTasks.Add(1)
		p.logger.Debug("crawl completed",
			zap.String("url", result.URL),
			zap.Int("status_code", result.StatusCode))
	}

	p.stats.ProcessingTime.Add(int64(result.ResponseTime))
}

// Shutdown gracefully shuts down the worker pool
func (p *WorkerPool) Shutdown(ctx context.Context) error {
	if !p.shutdown.CompareAndSwap(false, true) {
		return ErrPoolClosed
	}

	// Close work queue to stop accepting new tasks
	close(p.workQueue)

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("shutdown context cancelled: %w", ctx.Err())
	case <-done:
		close(p.results)
		return nil
	case <-time.After(p.config.ShutdownTimeout):
		return fmt.Errorf("shutdown timed out after %v", p.config.ShutdownTimeout)
	}
}

// GetStats returns current worker pool statistics
func (p *WorkerPool) GetStats() *Stats {
	return p.stats
}

// HealthCheck performs a health check of the worker pool
func (p *WorkerPool) HealthCheck() error {
	if p.shutdown.Load() {
		return ErrPoolClosed
	}

	activeWorkers := p.stats.ActiveWorkers.Load()
	if activeWorkers == 0 {
		return ErrNoWorkersActive
	}

	return nil
}

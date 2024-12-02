// internal/crawler/frontier/frontier.go

package frontier

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GevorgGal/crawler/internal/crawler/policies"
	"github.com/GevorgGal/crawler/internal/crawler/types"
	"github.com/bits-and-blooms/bloom/v3"
	"go.uber.org/zap"
)

// Error definitions
var (
	ErrMaxDepthExceeded = errors.New("maximum crawl depth exceeded")
	ErrInvalidQueueSize = errors.New("invalid queue size")
	ErrInvalidDepth     = errors.New("invalid depth: must be non-negative")
	ErrInvalidConfig    = errors.New("invalid configuration")
)

// Config holds the frontier configuration parameters
type Config struct {
	MaxQueueSize      int32   // Maximum size of each domain queue
	MaxDepth          int     // Maximum crawling depth
	BloomFilterSize   uint    // Expected number of items
	BloomFilterFPRate float64 // Desired false positive rate (0 to 1)
}

// Storage defines the interface for URL storage implementations
type Storage interface {
	IsVisited(ctx context.Context, url string) (bool, error)
	MarkVisited(ctx context.Context, url string) error
	Flush(ctx context.Context) error
}

// Metrics collects frontier operation metrics
type Metrics struct {
	TotalURLs     atomic.Int64
	ProcessedURLs atomic.Int64
	ErrorCount    atomic.Int64
}

// URLFrontier manages the URL crawling queue with priority and politeness
type URLFrontier struct {
	priorityQueues map[types.Priority]*DomainQueues
	domainPolicies *policies.DomainPolicies
	bloomFilter    *bloom.BloomFilter
	storage        Storage
	metrics        *Metrics
	logger         *zap.Logger
	config         *Config
	mu             sync.RWMutex
	wg             sync.WaitGroup
	shuttingDown   atomic.Bool
}

// NewURLFrontier creates a new URL frontier with the given configuration
func NewURLFrontier(cfg *Config, logger *zap.Logger) (*URLFrontier, error) {
	if logger == nil {
		return nil, fmt.Errorf("invalid logger: logger cannot be nil")
	}

	if cfg == nil {
		return nil, fmt.Errorf("invalid config: config cannot be nil")
	}

	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	filter := bloom.NewWithEstimates(cfg.BloomFilterSize, cfg.BloomFilterFPRate)

	return &URLFrontier{
		priorityQueues: make(map[types.Priority]*DomainQueues),
		bloomFilter:    filter,
		logger:         logger,
		config:         cfg,
		metrics:        &Metrics{},
	}, nil
}

func validateConfig(cfg *Config) error {
	if cfg.MaxQueueSize <= 0 {
		return fmt.Errorf("invalid max queue size %d: %w", cfg.MaxQueueSize, ErrInvalidQueueSize)
	}
	if cfg.MaxDepth < 0 {
		return fmt.Errorf("invalid max depth %d: %w", cfg.MaxDepth, ErrInvalidDepth)
	}
	if cfg.BloomFilterFPRate <= 0 || cfg.BloomFilterFPRate >= 1 {
		return fmt.Errorf("invalid bloom filter false positive rate %f: must be between 0 and 1", cfg.BloomFilterFPRate)
	}
	return nil
}

// Add adds a new URL to the frontier
func (f *URLFrontier) Add(ctx context.Context, rawURL string, depth int) error {
	if ctx.Err() != nil {
		return fmt.Errorf("context error: %w", ctx.Err())
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if depth > f.config.MaxDepth {
		return fmt.Errorf("depth %d exceeds maximum allowed depth %d: %w",
			depth, f.config.MaxDepth, ErrMaxDepthExceeded)
	}

	// Check if URL has been seen before
	if f.bloomFilter.TestString(rawURL) {
		exists, err := f.storage.IsVisited(ctx, rawURL)
		if err != nil {
			return fmt.Errorf("checking visited status: %w", err)
		}
		if exists {
			return fmt.Errorf("url %s: already processed", rawURL)
		}
	}

	// Parse and validate URL
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("parsing url %s: %w", rawURL, err)
	}

	domain := parsedURL.Hostname()
	if domain == "" {
		return fmt.Errorf("invalid url %s: empty hostname", rawURL)
	}

	priority := f.calculatePriority(parsedURL, depth)

	item := &types.URLItem{
		URL:          rawURL,
		Depth:        depth,
		Priority:     priority,
		Domain:       domain,
		LastCrawled:  time.Time{},
		RetryCount:   0,
		DiscoveredAt: time.Now(),
	}

	// Get or create domain queues for this priority
	dqs, ok := f.priorityQueues[priority]
	if !ok {
		dqs = NewDomainQueues(f.config.MaxQueueSize, f.logger)
		f.priorityQueues[priority] = dqs
	}

	// Add to domain queue
	if err := dqs.Add(ctx, item); err != nil {
		return fmt.Errorf("adding to domain queue: %w", err)
	}

	f.metrics.TotalURLs.Add(1)
	f.logger.Debug("added url to frontier",
		zap.String("url", rawURL),
		zap.String("domain", domain),
		zap.Int("depth", depth))

	return nil
}

// Next returns the next URL to crawl
func (f *URLFrontier) Next(ctx context.Context) (*types.URLItem, error) {
	if ctx.Err() != nil {
		return nil, fmt.Errorf("context error: %w", ctx.Err())
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	// Try each priority level in order
	for _, priority := range []types.Priority{
		types.HighPriority,
		types.MediumPriority,
		types.LowPriority,
	} {
		dqs, ok := f.priorityQueues[priority]
		if !ok {
			continue
		}

		item, err := dqs.Next(ctx)
		if err == nil {
			f.metrics.ProcessedURLs.Add(1)
			return item, nil
		}
		if !errors.Is(err, ErrNoItemAvailable) {
			return nil, fmt.Errorf("getting next item: %w", err)
		}
	}

	return nil, ErrNoItemAvailable
}

// calculatePriority determines the crawling priority of a URL
func (f *URLFrontier) calculatePriority(parsedURL *url.URL, depth int) types.Priority {
	switch {
	case depth == 0:
		return types.HighPriority
	case depth == 1:
		return types.MediumPriority
	default:
		return types.LowPriority
	}
}

// MarkVisited marks a URL as visited in both bloom filter and storage
func (f *URLFrontier) MarkVisited(ctx context.Context, url string) error {
	f.bloomFilter.AddString(url)
	if err := f.storage.MarkVisited(ctx, url); err != nil {
		return fmt.Errorf("marking URL as visited in storage: %w", err)
	}
	return nil
}

// Shutdown gracefully shuts down the frontier
func (f *URLFrontier) Shutdown(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.shuttingDown.Store(true)

	// Wait for ongoing operations with timeout
	done := make(chan struct{})
	go func() {
		f.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("shutdown context cancelled: %w", ctx.Err())
	case <-done:
	}

	if err := f.storage.Flush(ctx); err != nil {
		return fmt.Errorf("flushing storage: %w", err)
	}

	return nil
}

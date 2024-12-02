package frontier

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/GevorgGal/crawler/internal/crawler/types"
	"go.uber.org/zap"
)

// Domain queue specific errors
var (
	ErrQueueFull       = errors.New("domain queue is full")
	ErrNoItemAvailable = errors.New("no items available")
	ErrDomainNotFound  = errors.New("domain not found")
)

// QueueStats holds statistics about a domain queue
type QueueStats struct {
	TotalItems  int       // Total URLs across all queues
	MaxSize     int32     // Maximum allowed queue size
	LastAccess  time.Time // Most recent queue access
	DomainCount int       // Number of domains in queue
	EmptyQueues int       // Number of empty queues
}

// DomainQueues manages queues for different domains within a priority level
type DomainQueues struct {
	queues   map[string]*URLQueue // Map of domain to its queue
	accessor *RoundRobinAccessor  // Fair queue selection
	mu       sync.RWMutex         // Protects the queues map
	maxSize  int32                // Maximum size per domain queue
	logger   *zap.Logger
}

// URLQueue represents a queue of URLs for a specific domain
type URLQueue struct {
	items      []*types.URLItem // URLs to crawl
	mu         sync.Mutex       // Protects the items slice
	lastAccess time.Time        // Last time queue was accessed
}

// RoundRobinAccessor provides fair access to domain queues
type RoundRobinAccessor struct {
	domains []string // List of domains
	current int      // Current position
	mu      sync.Mutex
}

// NewDomainQueues creates a new DomainQueues instance
func NewDomainQueues(maxSize int32, logger *zap.Logger) *DomainQueues {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &DomainQueues{
		queues: make(map[string]*URLQueue),
		accessor: &RoundRobinAccessor{
			domains: make([]string, 0),
		},
		maxSize: maxSize,
		logger:  logger,
	}
}

// Add adds a URL to its domain's queue
func (dq *DomainQueues) Add(ctx context.Context, item *types.URLItem) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context error: %w", err)
	}

	if item == nil {
		return fmt.Errorf("nil item provided")
	}

	if item.Domain == "" {
		return fmt.Errorf("empty domain in URL item")
	}

	dq.mu.Lock()
	defer dq.mu.Unlock()

	queue, ok := dq.queues[item.Domain]
	if !ok {
		queue = &URLQueue{
			items:      make([]*types.URLItem, 0),
			lastAccess: time.Now(),
		}
		dq.queues[item.Domain] = queue
		dq.accessor.AddDomain(item.Domain)
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	if len(queue.items) >= int(dq.maxSize) {
		return fmt.Errorf("domain %s: %w", item.Domain, ErrQueueFull)
	}

	queue.items = append(queue.items, item)
	queue.lastAccess = time.Now()

	dq.logger.Debug("added URL to domain queue",
		zap.String("domain", item.Domain),
		zap.Int("queue_size", len(queue.items)))

	return nil
}

// Next returns the next URL to crawl using round-robin selection
func (dq *DomainQueues) Next(ctx context.Context) (*types.URLItem, error) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-timer.C:
			return nil, fmt.Errorf("timeout: %w", ErrNoItemAvailable)
		default:
			item, err := dq.tryNext()
			if err == nil {
				return item, nil
			}
			if !errors.Is(err, ErrNoItemAvailable) {
				return nil, err
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// tryNext attempts to get the next URL from the queues
func (dq *DomainQueues) tryNext() (*types.URLItem, error) {
	dq.mu.RLock()
	defer dq.mu.RUnlock()

	for i := 0; i < len(dq.accessor.domains); i++ {
		domain := dq.accessor.Next()
		if domain == "" {
			break
		}

		queue := dq.queues[domain]
		if queue == nil {
			continue
		}

		queue.mu.Lock()
		if len(queue.items) > 0 {
			item := queue.items[0]
			queue.items = queue.items[1:]
			queue.lastAccess = time.Now()
			queue.mu.Unlock()

			dq.logger.Debug("retrieved URL from queue",
				zap.String("domain", domain),
				zap.String("url", item.URL))

			return item, nil
		}
		queue.mu.Unlock()
	}

	return nil, ErrNoItemAvailable
}

// Size returns the total number of queued URLs across all domains
func (dq *DomainQueues) Size() int {
	dq.mu.RLock()
	defer dq.mu.RUnlock()

	total := 0
	for _, q := range dq.queues {
		q.mu.Lock()
		total += len(q.items)
		q.mu.Unlock()
	}
	return total
}

// GetStats returns current queue statistics
func (dq *DomainQueues) GetStats() QueueStats {
	dq.mu.RLock()
	defer dq.mu.RUnlock()

	stats := QueueStats{
		MaxSize:     dq.maxSize,
		DomainCount: len(dq.queues),
	}

	for _, q := range dq.queues {
		q.mu.Lock()
		stats.TotalItems += len(q.items)
		if len(q.items) == 0 {
			stats.EmptyQueues++
		}
		if q.lastAccess.After(stats.LastAccess) {
			stats.LastAccess = q.lastAccess
		}
		q.mu.Unlock()
	}

	return stats
}

// RemoveEmptyQueues removes queues that have been empty for a while
func (dq *DomainQueues) RemoveEmptyQueues() int {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	removed := 0
	threshold := time.Now().Add(-10 * time.Minute)

	for domain, queue := range dq.queues {
		queue.mu.Lock()
		if len(queue.items) == 0 && queue.lastAccess.Before(threshold) {
			delete(dq.queues, domain)
			dq.accessor.RemoveDomain(domain)
			removed++
			dq.logger.Debug("removed empty queue",
				zap.String("domain", domain),
				zap.Time("last_access", queue.lastAccess))
		}
		queue.mu.Unlock()
	}

	return removed
}

// RoundRobinAccessor methods

// Next returns the next domain in round-robin order
func (rr *RoundRobinAccessor) Next() string {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if len(rr.domains) == 0 {
		return ""
	}

	domain := rr.domains[rr.current]
	rr.current = (rr.current + 1) % len(rr.domains)
	return domain
}

// AddDomain adds a new domain to the round-robin accessor
func (rr *RoundRobinAccessor) AddDomain(domain string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	for _, d := range rr.domains {
		if d == domain {
			return
		}
	}
	rr.domains = append(rr.domains, domain)
}

// RemoveDomain removes a domain from the round-robin accessor
func (rr *RoundRobinAccessor) RemoveDomain(domain string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	for i, d := range rr.domains {
		if d == domain {
			copy(rr.domains[i:], rr.domains[i+1:])
			rr.domains = rr.domains[:len(rr.domains)-1]
			if rr.current >= len(rr.domains) {
				rr.current = 0
			}
			return
		}
	}
}

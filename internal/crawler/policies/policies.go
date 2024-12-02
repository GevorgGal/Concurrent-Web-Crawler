package policies

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GevorgGal/crawler/internal/crawler/types"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Error definitions for policy operations
var (
	ErrDomainThrottled = fmt.Errorf("domain is currently throttled")
	ErrTooManyErrors   = fmt.Errorf("too many errors for domain")
)

// Config holds configuration for domain policies
type Config struct {
	DefaultDelay   time.Duration // Default delay between requests
	MinDelay       time.Duration // Minimum delay between requests
	MaxDelay       time.Duration // Maximum delay between requests
	MaxRetries     int           // Maximum number of retry attempts
	ErrorThreshold int           // Number of errors before increasing delay
	BackoffFactor  float64       // Multiplicative factor for backoff
}

// DomainStats tracks statistics for a specific domain
type DomainStats struct {
	SuccessCount    atomic.Int64
	ErrorCount      atomic.Int64
	LastStatusCode  atomic.Int32
	LastAccess      atomic.Int64 // Unix timestamp
	AvgResponseTime atomic.Int64 // Nanoseconds
}

// DomainPolicy manages crawling policies for individual domains
type DomainPolicies struct {
	delays       map[string]*rate.Limiter
	stats        map[string]*DomainStats
	errorHistory map[string][]types.ErrorDetails
	config       *Config
	logger       *zap.Logger
	mu           sync.RWMutex
}

// NewDomainPolicies creates a new DomainPolicies instance
func NewDomainPolicies(cfg *Config, logger *zap.Logger) (*DomainPolicies, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid policy config: %w", err)
	}

	return &DomainPolicies{
		delays:       make(map[string]*rate.Limiter),
		stats:        make(map[string]*DomainStats),
		errorHistory: make(map[string][]types.ErrorDetails),
		config:       cfg,
		logger:       logger,
	}, nil
}

func validateConfig(cfg *Config) error {
	if cfg.DefaultDelay < cfg.MinDelay {
		return fmt.Errorf("default delay cannot be less than minimum delay")
	}
	if cfg.MaxDelay < cfg.DefaultDelay {
		return fmt.Errorf("maximum delay cannot be less than default delay")
	}
	if cfg.BackoffFactor <= 1.0 {
		return fmt.Errorf("backoff factor must be greater than 1.0")
	}
	return nil
}

// ShouldCrawl checks if a domain can be crawled now
func (dp *DomainPolicies) ShouldCrawl(domain string) (wait time.Duration, canCrawl bool) {
	dp.mu.RLock()
	limiter, exists := dp.delays[domain]
	if !exists {
		dp.mu.RUnlock()
		dp.mu.Lock()
		defer dp.mu.Unlock()
		// Double check after acquiring write lock
		if limiter, exists = dp.delays[domain]; !exists {
			limiter = rate.NewLimiter(rate.Every(dp.config.DefaultDelay), 1)
			dp.delays[domain] = limiter
			dp.stats[domain] = &DomainStats{}
		}
		return 0, true
	}
	dp.mu.RUnlock()

	reservation := limiter.Reserve()
	if !reservation.OK() {
		return 0, false
	}

	wait = reservation.Delay()
	return wait, true
}

// UpdateStats updates domain statistics based on crawl result
func (dp *DomainPolicies) UpdateStats(domain string, result *types.CrawlResult) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	stats := dp.stats[domain]
	if stats == nil {
		stats = &DomainStats{}
		dp.stats[domain] = stats
	}

	// Update last access time
	stats.LastAccess.Store(time.Now().UnixNano())

	if result.Error != nil {
		stats.ErrorCount.Add(1)
		dp.recordError(domain, result)
		dp.adjustDelay(domain, true)
	} else {
		stats.SuccessCount.Add(1)
		stats.LastStatusCode.Store(int32(result.StatusCode))
		dp.adjustDelay(domain, false)
	}

	// Update average response time
	dp.updateResponseTime(stats, result.ResponseTime)
}

// recordError records an error for a domain
func (dp *DomainPolicies) recordError(domain string, result *types.CrawlResult) {
	errDetails := types.ErrorDetails{
		Error:      result.Error,
		StatusCode: result.StatusCode,
		Timestamp:  result.Timestamp,
	}

	history := dp.errorHistory[domain]
	if len(history) >= dp.config.ErrorThreshold {
		// Remove oldest error
		history = history[1:]
	}
	dp.errorHistory[domain] = append(history, errDetails)
}

// adjustDelay adjusts the crawl delay based on success/failure
func (dp *DomainPolicies) adjustDelay(domain string, hasError bool) {
	limiter := dp.delays[domain]
	if limiter == nil {
		return
	}

	currentDelay := time.Duration(float64(time.Second) / float64(limiter.Limit()))

	var newDelay time.Duration
	if hasError {
		// Increase delay using backoff factor
		newDelay = time.Duration(float64(currentDelay) * dp.config.BackoffFactor)
		if newDelay > dp.config.MaxDelay {
			newDelay = dp.config.MaxDelay
		}
	} else {
		// Gradually decrease delay on success
		newDelay = time.Duration(float64(currentDelay) * 0.95)
		if newDelay < dp.config.DefaultDelay {
			newDelay = dp.config.DefaultDelay
		}
	}

	dp.delays[domain] = rate.NewLimiter(rate.Every(newDelay), 1)
}

// updateResponseTime updates the moving average of response times
func (dp *DomainPolicies) updateResponseTime(stats *DomainStats, newTime time.Duration) {
	const weight = 0.1 // Weight for new values in moving average

	oldAvg := time.Duration(stats.AvgResponseTime.Load())
	newAvg := time.Duration(float64(oldAvg)*(1-weight) + float64(newTime)*weight)
	stats.AvgResponseTime.Store(int64(newAvg))
}

// GetStats returns statistics for a domain
func (dp *DomainPolicies) GetStats(domain string) *DomainStats {
	dp.mu.RLock()
	defer dp.mu.RUnlock()
	return dp.stats[domain]
}

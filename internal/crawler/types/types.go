package types

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Error definitions
var (
	ErrInvalidURL      = errors.New("invalid url provided")
	ErrEmptyResponse   = errors.New("empty response from server")
	ErrMaxRedirects    = errors.New("maximum redirect count exceeded")
	ErrInvalidContent  = errors.New("invalid or unsupported content type")
	ErrQueueFull       = errors.New("queue is full")
	ErrRobotsForbidden = errors.New("blocked by robots.txt")
	ErrRateLimited     = errors.New("rate limit exceeded")
)

// Priority represents the crawling priority of a URL
type Priority int

const (
	HighPriority Priority = iota
	MediumPriority
	LowPriority
)

// RetryPolicy defines how retries should be handled
type RetryPolicy struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	BackoffFactor   float64
	RetryableErrors map[error]bool
}

// URLItem represents a URL to be crawled with its metadata
type URLItem struct {
	URL          string
	Depth        int
	Priority     Priority
	Domain       string
	LastCrawled  time.Time
	RetryCount   int
	ParentURL    string       // URL that led to this URL
	DiscoveredAt time.Time    // When URL was first found
	RetryPolicy  *RetryPolicy // Custom retry policy if needed
	Metadata     URLMetadata
}

// URLMetadata contains additional information about the URL
type URLMetadata struct {
	Headers        http.Header
	LastStatusCode int
	ResponseTime   time.Duration
	ContentType    string
	ContentLength  int64
	LastModified   time.Time
	ETag           string
}

// CrawlResult represents the outcome of crawling a URL
type CrawlResult struct {
	URL          string
	StatusCode   int
	ResponseTime time.Duration
	Error        error
	Content      []byte
	Links        []string
	Metadata     *ResultMetadata
	Timestamp    time.Time
}

// ResultMetadata contains additional information about the crawl result
type ResultMetadata struct {
	ContentType   string
	ContentLength int64
	LastModified  time.Time
	ETag          string
	Headers       http.Header
	RedirectCount int
}

// ErrorDetails provides detailed information about crawl errors
type ErrorDetails struct {
	Error      error
	StatusCode int
	RetryCount int
	Timestamp  time.Time
	Context    string
}

// RateLimitError represents a rate limiting error with retry information
type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited, retry after %v", e.RetryAfter)
}

// Key interfaces for crawler components
type URLProcessor interface {
	Process(context.Context, *URLItem) (*CrawlResult, error)
}

type URLFilter interface {
	ShouldCrawl(context.Context, *URLItem) (bool, error)
}

// ValidateURLItem validates a URLItem before crawling
func (u *URLItem) ValidateURLItem() error {
	if u.URL == "" {
		return fmt.Errorf("%w: empty url", ErrInvalidURL)
	}
	if u.Domain == "" {
		return fmt.Errorf("%w: empty domain", ErrInvalidURL)
	}
	if u.Depth < 0 {
		return fmt.Errorf("invalid depth: %d", u.Depth)
	}
	return nil
}

// CalculatePriority determines the priority based on URL characteristics
func (u *URLItem) CalculatePriority() Priority {
	switch {
	case strings.Contains(u.URL, "sitemap"):
		return HighPriority
	case u.Depth <= 2:
		return MediumPriority
	default:
		return LowPriority
	}
}

// IsValidContentType checks if the content type is supported
func (m *URLMetadata) IsValidContentType() bool {
	switch m.ContentType {
	case "text/html", "application/xhtml+xml", "text/xml", "application/xml":
		return true
	default:
		return false
	}
}

// RetryAfter extracts retry delay from response headers
func (r *CrawlResult) RetryAfter() time.Duration {
	if r.StatusCode == http.StatusTooManyRequests {
		if retryAfter := r.Metadata.Headers.Get("Retry-After"); retryAfter != "" {
			if seconds, err := strconv.Atoi(retryAfter); err == nil {
				return time.Duration(seconds) * time.Second
			}
		}
	}
	return 0
}

// ExtractRetryInfo determines if an error is retryable
func (e *ErrorDetails) ExtractRetryInfo() (shouldRetry bool, delay time.Duration) {
	var rateLimitErr *RateLimitError
	if errors.As(e.Error, &rateLimitErr) {
		return true, rateLimitErr.RetryAfter
	}

	switch e.StatusCode {
	case http.StatusTooManyRequests:
		return true, 60 * time.Second
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		return true, 30 * time.Second
	case http.StatusInternalServerError:
		return true, 10 * time.Second
	}

	return false, 0
}

// ShouldRetry determines if a failed crawl should be retried
func (r *CrawlResult) ShouldRetry() bool {
	if r.Error == nil {
		return false
	}

	// Check error types
	var rateLimitErr *RateLimitError
	if errors.As(r.Error, &rateLimitErr) {
		return true
	}

	// Check status codes
	switch r.StatusCode {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	}

	// Check specific error types
	return errors.Is(r.Error, context.DeadlineExceeded) ||
		errors.Is(r.Error, context.Canceled)
}

// Clone creates a deep copy of URLItem
func (u *URLItem) Clone() *URLItem {
	if u == nil {
		return nil
	}

	clone := &URLItem{
		URL:          u.URL,
		Depth:        u.Depth,
		Priority:     u.Priority,
		Domain:       u.Domain,
		LastCrawled:  u.LastCrawled,
		RetryCount:   u.RetryCount,
		ParentURL:    u.ParentURL,
		DiscoveredAt: u.DiscoveredAt,
	}

	if u.RetryPolicy != nil {
		clone.RetryPolicy = &RetryPolicy{
			MaxRetries:    u.RetryPolicy.MaxRetries,
			InitialDelay:  u.RetryPolicy.InitialDelay,
			MaxDelay:      u.RetryPolicy.MaxDelay,
			BackoffFactor: u.RetryPolicy.BackoffFactor,
		}
		if u.RetryPolicy.RetryableErrors != nil {
			clone.RetryPolicy.RetryableErrors = make(map[error]bool)
			for k, v := range u.RetryPolicy.RetryableErrors {
				clone.RetryPolicy.RetryableErrors[k] = v
			}
		}
	}

	return clone
}

// String returns a string representation of Priority
func (p Priority) String() string {
	switch p {
	case HighPriority:
		return "High"
	case MediumPriority:
		return "Medium"
	case LowPriority:
		return "Low"
	default:
		return fmt.Sprintf("Unknown(%d)", p)
	}
}

package robots

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	ErrFetchFailed  = errors.New("failed to fetch robots.txt")
	ErrInvalidURL   = errors.New("invalid URL")
	ErrCacheExpired = errors.New("cache entry expired")
	ErrNotAllowed   = errors.New("URL is not allowed by robots.txt")
)

// Config holds configuration for the robots.txt handler
type Config struct {
	CacheDuration     time.Duration // How long to cache robots.txt content
	FetchTimeout      time.Duration // Timeout for fetching robots.txt
	DefaultCrawlDelay time.Duration // Default delay if not specified
	UserAgent         string        // User agent to use for requests
	MaxSize           int64         // Maximum size of robots.txt to process
	AllowOnError      bool          // Whether to allow crawling on robots.txt errors
	MaxRetries        int           // Maximum number of fetch retries
}

// CacheEntry represents a cached robots.txt entry
type CacheEntry struct {
	Rules       *RobotsData
	FetchedAt   time.Time
	LastChecked time.Time
	Error       error // Last fetch error if any
	RetryCount  int   // Number of failed fetch attempts
}

// RobotsData represents parsed robots.txt data
type RobotsData struct {
	AllowRules    []string
	DisallowRules []string
	CrawlDelay    time.Duration
	Sitemaps      []string
}

// Handler manages robots.txt fetching, parsing, and rule checking
type Handler struct {
	cache  map[string]*CacheEntry
	client *http.Client
	config *Config
	logger *zap.Logger
	mu     sync.RWMutex
}

// NewHandler creates a new robots.txt handler
func NewHandler(cfg *Config, logger *zap.Logger) (*Handler, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	client := &http.Client{
		Timeout: cfg.FetchTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return nil
		},
	}

	return &Handler{
		cache:  make(map[string]*CacheEntry),
		client: client,
		config: cfg,
		logger: logger,
	}, nil
}

func validateConfig(cfg *Config) error {
	if cfg.CacheDuration <= 0 {
		return fmt.Errorf("cache duration must be positive")
	}
	if cfg.FetchTimeout <= 0 {
		return fmt.Errorf("fetch timeout must be positive")
	}
	if cfg.UserAgent == "" {
		return fmt.Errorf("user agent must be specified")
	}
	if cfg.MaxSize <= 0 {
		return fmt.Errorf("max size must be positive")
	}
	if cfg.MaxRetries < 0 {
		return fmt.Errorf("max retries cannot be negative")
	}
	return nil
}

// IsAllowed checks if crawling a URL is allowed
func (h *Handler) IsAllowed(ctx context.Context, urlStr string) (bool, time.Duration, error) {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return false, 0, fmt.Errorf("%w: %v", ErrInvalidURL, err)
	}

	rules, err := h.getRules(ctx, parsedURL.Host)
	if err != nil {
		if h.config.AllowOnError {
			return true, h.config.DefaultCrawlDelay, nil
		}
		return false, 0, fmt.Errorf("getting rules: %w", err)
	}

	allowed := h.checkRules(rules, parsedURL.Path)
	if !allowed {
		return false, 0, ErrNotAllowed
	}

	delay := rules.CrawlDelay
	if delay == 0 {
		delay = h.config.DefaultCrawlDelay
	}

	return true, delay, nil
}

// getRules fetches and caches robots.txt rules for a domain
func (h *Handler) getRules(ctx context.Context, domain string) (*RobotsData, error) {
	h.mu.RLock()
	entry, exists := h.cache[domain]
	h.mu.RUnlock()

	if exists && time.Since(entry.FetchedAt) < h.config.CacheDuration {
		if entry.Error != nil {
			return nil, fmt.Errorf("cached error: %w", entry.Error)
		}
		return entry.Rules, nil
	}

	// Fetch and parse robots.txt with retries
	rules, err := h.fetchWithRetry(ctx, domain)

	h.mu.Lock()
	h.cache[domain] = &CacheEntry{
		Rules:     rules,
		FetchedAt: time.Now(),
		Error:     err,
	}
	h.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return rules, nil
}

// fetchRobots fetches and parses robots.txt for a domain
func (h *Handler) fetchRobots(ctx context.Context, domain string) (*RobotsData, error) {
	robotsURL := fmt.Sprintf("https://%s/robots.txt", domain)

	req, err := http.NewRequestWithContext(ctx, "GET", robotsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("User-Agent", h.config.UserAgent)

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFetchFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// RFC 9309: If /robots.txt does not exist, allow all
		return &RobotsData{
			CrawlDelay: h.config.DefaultCrawlDelay,
		}, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", ErrFetchFailed, resp.StatusCode)
	}

	body := io.LimitReader(resp.Body, h.config.MaxSize)
	content, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading robots.txt: %w", err)
	}

	return h.parseRobots(content), nil
}

// CleanCache removes expired entries from the cache
func (h *Handler) CleanCache(maxAge time.Duration) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	removed := 0
	now := time.Now()
	for domain, entry := range h.cache {
		if now.Sub(entry.FetchedAt) > maxAge {
			delete(h.cache, domain)
			removed++
		}
	}

	return removed
}

// GetCacheStats returns statistics about the cache
func (h *Handler) GetCacheStats() map[string]interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := map[string]interface{}{
		"total_entries": len(h.cache),
		"entries":       make(map[string]interface{}),
	}

	for domain, entry := range h.cache {
		stats["entries"].(map[string]interface{})[domain] = map[string]interface{}{
			"age":         time.Since(entry.FetchedAt).String(),
			"has_error":   entry.Error != nil,
			"retry_count": entry.RetryCount,
		}
	}

	return stats
}

// GetSitemaps returns the sitemaps listed in robots.txt for a domain
func (h *Handler) GetSitemaps(ctx context.Context, domain string) ([]string, error) {
	rules, err := h.getRules(ctx, domain)
	if err != nil {
		return nil, err
	}
	return rules.Sitemaps, nil
}

// Add these methods to robots.go

// isUserAgentMatch checks if a robots.txt user-agent line applies to our crawler
func (h *Handler) isUserAgentMatch(robotsAgent string) bool {
	if robotsAgent == "" {
		return false
	}

	ourAgent := strings.ToLower(h.config.UserAgent)
	robotsAgent = strings.ToLower(robotsAgent)

	// Handle special cases
	switch robotsAgent {
	case "*":
		return true
	case "":
		return false
	}

	// Check for exact match
	if robotsAgent == ourAgent {
		return true
	}

	// Check if robots.txt user-agent is a prefix of our user-agent
	// This handles cases like "googlebot" matching "googlebot-news"
	if strings.HasPrefix(ourAgent, robotsAgent) {
		return true
	}

	return false
}

// cleanPath normalizes a robots.txt path pattern
func cleanPath(pattern string) string {
	// Remove leading and trailing whitespace
	pattern = strings.TrimSpace(pattern)

	// Handle empty pattern
	if pattern == "" {
		return "/"
	}

	// Convert windows-style paths
	pattern = strings.ReplaceAll(pattern, "\\", "/")

	// Remove query string if present
	if idx := strings.Index(pattern, "?"); idx != -1 {
		pattern = pattern[:idx]
	}

	// Remove fragment if present
	if idx := strings.Index(pattern, "#"); idx != -1 {
		pattern = pattern[:idx]
	}

	// Remove multiple consecutive slashes
	for strings.Contains(pattern, "//") {
		pattern = strings.ReplaceAll(pattern, "//", "/")
	}

	// Ensure pattern starts with /
	if !strings.HasPrefix(pattern, "/") {
		pattern = "/" + pattern
	}

	// Handle percent-encoded characters (common in robots.txt)
	if strings.Contains(pattern, "%") {
		if decodedPattern, err := url.QueryUnescape(pattern); err == nil {
			pattern = decodedPattern
		}
	}

	return pattern
}

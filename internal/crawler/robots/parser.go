package robots

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Rule represents a parsed robots.txt rule
type Rule struct {
	Pattern string
	Allow   bool
	Length  int // Length of the pattern for specificity comparison
}

// parseRobots parses robots.txt content following RFC 9309
func (h *Handler) parseRobots(content []byte) *RobotsData {
	data := &RobotsData{
		AllowRules:    make([]string, 0),
		DisallowRules: make([]string, 0),
		Sitemaps:      make([]string, 0),
	}

	var (
		currentUserAgent string
		isRelevantGroup  bool
		inRelevantGroup  bool
	)

	scanner := bufio.NewScanner(strings.NewReader(string(content)))

	// Group processing state
	var rules []Rule

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		field := strings.TrimSpace(strings.ToLower(parts[0]))
		value := strings.TrimSpace(parts[1])

		switch field {
		case "user-agent":
			// New user-agent line means we're starting a new group
			if currentUserAgent != "" && !inRelevantGroup {
				// Save rules from previous group if it was relevant
				if isRelevantGroup {
					for _, rule := range rules {
						if rule.Allow {
							data.AllowRules = append(data.AllowRules, rule.Pattern)
						} else {
							data.DisallowRules = append(data.DisallowRules, rule.Pattern)
						}
					}
				}
				rules = nil
			}

			currentUserAgent = strings.ToLower(value)
			isRelevantGroup = h.isUserAgentMatch(currentUserAgent)
			inRelevantGroup = true

		case "allow":
			if isRelevantGroup {
				pattern := cleanPath(value)
				if pattern != "" {
					rules = append(rules, Rule{
						Pattern: pattern,
						Allow:   true,
						Length:  len(pattern),
					})
				}
			}

		case "disallow":
			if isRelevantGroup {
				pattern := cleanPath(value)
				if pattern != "" {
					rules = append(rules, Rule{
						Pattern: pattern,
						Allow:   false,
						Length:  len(pattern),
					})
				}
			}

		case "crawl-delay":
			if isRelevantGroup {
				if delay, err := time.ParseDuration(value + "s"); err == nil {
					data.CrawlDelay = delay
				}
			}

		case "sitemap":
			if value != "" {
				data.Sitemaps = append(data.Sitemaps, value)
			}

		default:
			inRelevantGroup = false
		}
	}

	// Don't forget to process the last group
	if isRelevantGroup {
		for _, rule := range rules {
			if rule.Allow {
				data.AllowRules = append(data.AllowRules, rule.Pattern)
			} else {
				data.DisallowRules = append(data.DisallowRules, rule.Pattern)
			}
		}
	}

	return data
}

// checkRules implements RFC 9309 rule matching
func (h *Handler) checkRules(rules *RobotsData, path string) bool {
	if rules == nil {
		return true
	}

	path = cleanPath(path)
	if path == "" {
		path = "/"
	}

	// Find the most specific (longest) matching rule
	var (
		longestMatch   int
		allowed        = true // Default to allowed if no rules match
		specificityTie = false
	)

	// Check disallow rules first
	for _, pattern := range rules.DisallowRules {
		if matches, length := pathMatchWithLength(pattern, path); matches {
			if length > longestMatch {
				longestMatch = length
				allowed = false
				specificityTie = false
			} else if length == longestMatch {
				specificityTie = true
			}
		}
	}

	// Check allow rules (they win ties with disallow rules)
	for _, pattern := range rules.AllowRules {
		if matches, length := pathMatchWithLength(pattern, path); matches {
			if length > longestMatch || (length == longestMatch && specificityTie) {
				longestMatch = length
				allowed = true
			}
		}
	}

	return allowed
}

// pathMatchWithLength checks if a path matches a pattern and returns the specificity
func pathMatchWithLength(pattern, path string) (matches bool, length int) {
	// Handle empty pattern
	if pattern == "" {
		return true, 0
	}

	pattern = strings.ToLower(pattern)
	path = strings.ToLower(path)

	// Exact match check first (most common case)
	if pattern == path {
		return true, len(pattern)
	}

	// Handle special pattern characters
	if strings.ContainsAny(pattern, "*$") {
		// Convert pattern to regex
		regexPattern := regexp.QuoteMeta(pattern)
		regexPattern = strings.ReplaceAll(regexPattern, `\*`, `.*`)
		regexPattern = strings.ReplaceAll(regexPattern, `\$`, `$`)
		regexPattern = "^" + regexPattern

		if matched, _ := regexp.MatchString(regexPattern, path); matched {
			// For wildcard patterns, we count the non-wildcard portion
			return true, strings.Count(pattern, "*")
		}
	}

	// Prefix match
	if strings.HasPrefix(path, pattern) {
		return true, len(pattern)
	}

	return false, 0
}

// fetchWithRetry implements exponential backoff retry for robots.txt fetching
func (h *Handler) fetchWithRetry(ctx context.Context, domain string) (*RobotsData, error) {
	var lastErr error
	maxAttempts := 3

	for attempt := 0; attempt < maxAttempts; attempt++ {
		rules, err := h.fetchRobots(ctx, domain)
		if err == nil {
			return rules, nil
		}

		lastErr = err
		h.logger.Info("robots.txt fetch failed",
			zap.String("domain", domain),
			zap.Int("attempt", attempt+1),
			zap.Error(err))

		// Calculate backoff duration
		backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second

		// Don't sleep after the last attempt
		if attempt < maxAttempts-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	if h.config.AllowOnError {
		h.logger.Warn("allowing crawling after robots.txt fetch failure",
			zap.String("domain", domain),
			zap.Error(lastErr))
		return &RobotsData{
			CrawlDelay: h.config.DefaultCrawlDelay,
		}, nil
	}

	return nil, fmt.Errorf("failed to fetch robots.txt after %d attempts: %w",
		maxAttempts, lastErr)
}

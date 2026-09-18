package streamer

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type RetryConfig struct {
	// The maximum number of attempts to retry the work function.
	maxAttempts int
	// Returns the duration to wait before the next retry attempt.
	backoffFunc func(int) time.Duration
	// Whether a work function should be retried based on the error returned.
	shouldRetry func(error) bool
	// Function to call when a retry is attempted, useful for logging or metrics.
	onRetry func(attempt int, err error)
}

// Function signature for a retry configuration option
type RetryOption func(*RetryConfig)

// CreateRetryableWorkFunc returns a worker function that the user can configure, e.g. with retries
func CreateRetryableWorkFunc[T any, K any](
	actualWork func(context.Context, T) (K, error),
	options ...RetryOption,
) func(context.Context, T) (K, error) {

	// Default configuration, no retries by default
	config := RetryConfig{
		maxAttempts: 1,
		backoffFunc: func(attempt int) time.Duration { return 0 },
		shouldRetry: func(err error) bool { return false },
		onRetry:     func(attempt int, err error) {},
	}

	for _, option := range options {
		option(&config)
	}

	return func(ctx context.Context, item T) (K, error) {
		var lastErr error

		for attempt := 1; attempt <= config.maxAttempts; attempt++ {
			if attempt > 1 {
				backoffDelay := config.backoffFunc(attempt - 1)
				select {
				case <-time.After(backoffDelay):
				case <-ctx.Done():
					return *new(K), ctx.Err()
				}
			}

			result, err := actualWork(ctx, item)
			if err == nil {
				return result, nil
			}

			lastErr = err

			// Check if we should retry this error
			if !config.shouldRetry(err) || attempt == config.maxAttempts {
				break
			}

			// We are retrying, call the optional onRetry function
			config.onRetry(attempt, err)
		}

		return *new(K), fmt.Errorf("failed after %d attempts: %w", config.maxAttempts, lastErr)
	}
}

type RetryCondition func(error) bool

// WithRetryCondition lets consumers configure a conditional function that is used to retry workers
func WithRetryCondition(shouldRetry RetryCondition) RetryOption {
	return func(c *RetryConfig) {
		c.shouldRetry = shouldRetry
	}
}

// WithAnyOf wraps a set of retry condition functions, and returns parent function that will retrun true if ANY are true
func WithAnyOf(conditions ...RetryCondition) RetryCondition {
	return func(err error) bool {
		for _, condition := range conditions {
			if condition(err) {
				return true
			}
		}
		return false
	}
}

// WithAllOf wraps a set of retry condition functions, and returns parent function that will retrun true if ALL are true
func WithAllOf(conditions ...RetryCondition) RetryCondition {
	return func(err error) bool {
		for _, condition := range conditions {
			if !condition(err) {
				return false
			}
		}
		return len(conditions) > 0
	}
}

// WithRetries adds a retry count to the workers
func WithRetries(maxAttempts int) RetryOption {
	return func(c *RetryConfig) {
		c.maxAttempts = max(maxAttempts, 1)
	}
}

// WithLinearBackoff adds a linear backoff to workers that are retrying
func WithLinearBackoff(baseDelay time.Duration) RetryOption {
	return func(c *RetryConfig) {
		c.backoffFunc = func(attempt int) time.Duration {
			return time.Duration(attempt) * baseDelay
		}
	}
}

// WithExponentialBackoff adds an exponential backoff to workers that are retrying
func WithExponentialBackoff(baseDelay time.Duration, maxDelay time.Duration) RetryOption {
	return func(c *RetryConfig) {
		c.backoffFunc = func(attempt int) time.Duration {
			if baseDelay <= 0 {
				return 0
			}

			shift := attempt - 1
			if shift < 0 {
				shift = 0
			}
			if baseDelay > maxDelay>>uint(shift) {
				return maxDelay
			}

			return min(baseDelay<<uint(shift), maxDelay)
		}
	}
}

// WithFixedBackoff adds a fixed backoff to workers that are retrying
func WithFixedBackoff(delay time.Duration) RetryOption {
	return func(c *RetryConfig) {
		c.backoffFunc = func(attempt int) time.Duration {
			return delay
		}
	}
}

// WithRetryCallback lets consumers configure a callback function that is fired when a retry is attempted
func WithRetryCallback(onRetry func(attempt int, err error)) RetryOption {
	return func(c *RetryConfig) {
		c.onRetry = onRetry
	}
}

// WithRetryOnErrors lets consumers to retry on specific sentinel errors
func WithRetryOnErrors(targetErrors ...error) RetryOption {
	return func(c *RetryConfig) {
		c.shouldRetry = func(err error) bool {
			for _, targetErr := range targetErrors {
				if errors.Is(err, targetErr) {
					return true
				}
			}
			return false
		}
	}
}

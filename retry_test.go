package streamer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test errors for sentinel error testing
var (
	ErrNetwork     = errors.New("network error")
	ErrRateLimited = errors.New("rate limited")
	ErrValidation  = errors.New("validation error")
	ErrTemporary   = errors.New("temporary failure")
)

func TestCreateRetryableWorkFunc_NoRetries(t *testing.T) {
	callCount := 0
	workFunc := func(ctx context.Context, input int) (string, error) {
		callCount++
		if input == 1 {
			return "success", nil
		}
		return "", fmt.Errorf("error for input %d", input)
	}

	retryableFunc := CreateRetryableWorkFunc(workFunc)

	// Test success case
	result, err := retryableFunc(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, "success", result)
	assert.Equal(t, 1, callCount)

	// Test failure case (should not retry by default)
	callCount = 0
	result, err = retryableFunc(context.Background(), 2)
	assert.Error(t, err)
	assert.Equal(t, "", result)
	assert.Equal(t, 1, callCount)
	assert.Contains(t, err.Error(), "failed after 1 attempts")
}

func TestCreateRetryableWorkFunc_WithRetries(t *testing.T) {
	tests := []struct {
		name          string
		maxAttempts   int
		failUntilTry  int // Succeed on this attempt (0 = always fail)
		expectedCalls int
		expectSuccess bool
	}{
		{
			name:          "succeed immediately",
			maxAttempts:   3,
			failUntilTry:  1,
			expectedCalls: 1,
			expectSuccess: true,
		},
		{
			name:          "succeed on second try",
			maxAttempts:   3,
			failUntilTry:  2,
			expectedCalls: 2,
			expectSuccess: true,
		},
		{
			name:          "succeed on last try",
			maxAttempts:   3,
			failUntilTry:  3,
			expectedCalls: 3,
			expectSuccess: true,
		},
		{
			name:          "fail all attempts",
			maxAttempts:   3,
			failUntilTry:  0, // Never succeed
			expectedCalls: 3,
			expectSuccess: false,
		},
		{
			name:          "single retry",
			maxAttempts:   2,
			failUntilTry:  0,
			expectedCalls: 2,
			expectSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			workFunc := func(ctx context.Context, input int) (string, error) {
				callCount++
				if callCount >= tt.failUntilTry && tt.failUntilTry > 0 {
					return fmt.Sprintf("success-%d", callCount), nil
				}
				return "", fmt.Errorf("attempt %d failed", callCount)
			}

			retryableFunc := CreateRetryableWorkFunc(
				workFunc,
				WithRetries(tt.maxAttempts),
				WithRetryCondition(func(error) bool { return true }), // Retry all errors
			)

			result, err := retryableFunc(context.Background(), 42)

			assert.Equal(t, tt.expectedCalls, callCount)

			if tt.expectSuccess {
				assert.NoError(t, err)
				assert.Contains(t, result, "success")
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), fmt.Sprintf("failed after %d attempts", tt.maxAttempts))
			}
		})
	}
}

func TestCreateRetryableWorkFunc_Backoff(t *testing.T) {
	tests := []struct {
		name           string
		option         RetryOption
		attempts       int
		expectedDelays []time.Duration
	}{
		{
			name:           "linear backoff",
			option:         WithLinearBackoff(100 * time.Millisecond),
			attempts:       3,
			expectedDelays: []time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond},
		},
		{
			name:           "fixed backoff",
			option:         WithFixedBackoff(150 * time.Millisecond),
			attempts:       3,
			expectedDelays: []time.Duration{0, 150 * time.Millisecond, 150 * time.Millisecond},
		},
		{
			name:           "exponential backoff",
			option:         WithExponentialBackoff(50*time.Millisecond, 500*time.Millisecond),
			attempts:       4,
			expectedDelays: []time.Duration{0, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond},
		},
		{
			name:           "exponential backoff with max",
			option:         WithExponentialBackoff(100*time.Millisecond, 150*time.Millisecond),
			attempts:       4,
			expectedDelays: []time.Duration{0, 100 * time.Millisecond, 150 * time.Millisecond, 150 * time.Millisecond},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actualDelays []time.Duration
			var timestamps []time.Time

			workFunc := func(ctx context.Context, input int) (string, error) {
				timestamps = append(timestamps, time.Now())
				return "", errors.New("always fail")
			}

			retryableFunc := CreateRetryableWorkFunc(
				workFunc,
				WithRetries(tt.attempts),
				WithRetryCondition(func(error) bool { return true }),
				tt.option,
			)

			start := time.Now()
			_, err := retryableFunc(context.Background(), 42)

			// Calculate actual delays
			for i, timestamp := range timestamps {
				if i == 0 {
					actualDelays = append(actualDelays, timestamp.Sub(start))
				} else {
					actualDelays = append(actualDelays, timestamp.Sub(timestamps[i-1]))
				}
			}

			assert.Error(t, err)
			assert.Equal(t, tt.attempts, len(actualDelays))

			// Verify delays (with some tolerance for timing)
			tolerance := 50 * time.Millisecond
			for i, expectedDelay := range tt.expectedDelays {
				if i < len(actualDelays) {
					assert.InDelta(t, expectedDelay.Milliseconds(), actualDelays[i].Milliseconds(),
						float64(tolerance.Milliseconds()),
						"Delay %d: expected ~%v, got %v", i, expectedDelay, actualDelays[i])
				}
			}
		})
	}
}

func TestCreateRetryableWorkFunc_RetryConditions(t *testing.T) {
	tests := []struct {
		name          string
		option        RetryOption
		testErrors    []error
		expectRetries []bool
	}{
		{
			name:   "retry on specific errors",
			option: WithRetryOnErrors(ErrNetwork, ErrRateLimited),
			testErrors: []error{
				ErrNetwork,     // Should retry
				ErrRateLimited, // Should retry
				ErrValidation,  // Should not retry
				ErrTemporary,   // Should not retry
			},
			expectRetries: []bool{true, true, false, false},
		},
		{
			name: "custom retry condition",
			option: WithRetryCondition(func(err error) bool {
				return strings.Contains(err.Error(), "temporary")
			}),
			testErrors: []error{
				errors.New("temporary network issue"), // Should retry
				errors.New("permanent failure"),       // Should not retry
				ErrTemporary,                          // Should retry (contains "temporary")
			},
			expectRetries: []bool{true, false, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, testErr := range tt.testErrors {
				callCount := 0
				workFunc := func(ctx context.Context, input int) (string, error) {
					callCount++
					return "", testErr
				}

				retryableFunc := CreateRetryableWorkFunc(
					workFunc,
					WithRetries(3),
					tt.option,
				)

				_, err := retryableFunc(context.Background(), 42)

				expectedCalls := 1
				if tt.expectRetries[i] {
					expectedCalls = 3 // All attempts should be made
				}

				assert.Error(t, err)
				assert.Equal(t, expectedCalls, callCount,
					"Error %v should have %d calls, got %d", testErr, expectedCalls, callCount)
			}
		})
	}
}

func TestCreateRetryableWorkFunc_RetryCallback(t *testing.T) {
	var callbackCalls []struct {
		attempt int
		err     error
	}

	workFunc := func(ctx context.Context, input int) (string, error) {
		return "", errors.New("test error")
	}

	retryableFunc := CreateRetryableWorkFunc(
		workFunc,
		WithRetries(3),
		WithRetryCondition(func(error) bool { return true }),
		WithRetryCallback(func(attempt int, err error) {
			callbackCalls = append(callbackCalls, struct {
				attempt int
				err     error
			}{attempt, err})
		}),
	)

	_, err := retryableFunc(context.Background(), 42)

	assert.Error(t, err)
	assert.Len(t, callbackCalls, 2) // Should be called for attempts 1 and 2 (not the final failure)

	assert.Equal(t, 1, callbackCalls[0].attempt)
	assert.Equal(t, "test error", callbackCalls[0].err.Error())

	assert.Equal(t, 2, callbackCalls[1].attempt)
	assert.Equal(t, "test error", callbackCalls[1].err.Error())
}

func TestCreateRetryableWorkFunc_ContextCancellation(t *testing.T) {
	workFunc := func(ctx context.Context, input int) (string, error) {
		return "", errors.New("test error")
	}

	retryableFunc := CreateRetryableWorkFunc(
		workFunc,
		WithRetries(5),
		WithRetryCondition(func(error) bool { return true }),
		WithLinearBackoff(100*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context after short delay
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := retryableFunc(ctx, 42)
	duration := time.Since(start)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)

	// Should have cancelled during backoff, so duration should be around 150ms
	assert.Greater(t, duration, 100*time.Millisecond)
	assert.Less(t, duration, 250*time.Millisecond)
}

func TestRetryCondition_Composition(t *testing.T) {
	networkCondition := func(err error) bool {
		return errors.Is(err, ErrNetwork)
	}

	rateLimitCondition := func(err error) bool {
		return errors.Is(err, ErrRateLimited)
	}

	temporaryCondition := func(err error) bool {
		return errors.Is(err, ErrTemporary)
	}

	t.Run("WithAnyOf", func(t *testing.T) {
		anyCondition := WithAnyOf(networkCondition, rateLimitCondition)

		assert.True(t, anyCondition(ErrNetwork))
		assert.True(t, anyCondition(ErrRateLimited))
		assert.False(t, anyCondition(ErrTemporary))
		assert.False(t, anyCondition(ErrValidation))
	})

	t.Run("WithAllOf", func(t *testing.T) {
		// This is a contrived example since errors can't be multiple types,
		// but tests the logic
		allCondition := WithAllOf(
			func(err error) bool { return strings.Contains(err.Error(), "network") },
			func(err error) bool { return strings.Contains(err.Error(), "timeout") },
		)

		assert.True(t, allCondition(errors.New("network timeout error")))
		assert.False(t, allCondition(errors.New("network error")))
		assert.False(t, allCondition(errors.New("timeout error")))
		assert.False(t, allCondition(errors.New("different error")))
	})

	t.Run("nested composition", func(t *testing.T) {
		complexCondition := WithAnyOf(
			WithAllOf(networkCondition, temporaryCondition), // Both network AND temporary
			rateLimitCondition, // OR rate limited
		)

		assert.False(t, complexCondition(ErrNetwork))    // Only network
		assert.False(t, complexCondition(ErrTemporary))  // Only temporary
		assert.True(t, complexCondition(ErrRateLimited)) // Rate limited (matches OR)

		// For the AND condition, we'd need an error that matches both - this is contrived
		multiError := fmt.Errorf("network error: %w", ErrTemporary)
		assert.False(t, complexCondition(multiError)) // This won't work as expected due to errors.Is semantics
	})
}

func TestCreateRetryableWorkFunc_Integration(t *testing.T) {
	callLog := []string{}
	attemptCount := 0

	workFunc := func(ctx context.Context, input int) (string, error) {
		attemptCount++
		callLog = append(callLog, fmt.Sprintf("attempt-%d", attemptCount))

		switch attemptCount {
		case 1:
			return "", ErrNetwork // Should retry
		case 2:
			return "", ErrValidation // Should not retry
		default:
			return "success", nil
		}
	}

	retryableFunc := CreateRetryableWorkFunc(
		workFunc,
		WithRetries(4),
		WithRetryOnErrors(ErrNetwork, ErrRateLimited, ErrTemporary),
		WithExponentialBackoff(50*time.Millisecond, 200*time.Millisecond),
		WithRetryCallback(func(attempt int, err error) {
			callLog = append(callLog, fmt.Sprintf("retry-callback-%d", attempt))
		}),
	)

	start := time.Now()
	result, err := retryableFunc(context.Background(), 42)
	duration := time.Since(start)

	// Should fail after 2 attempts (second attempt hits validation error which doesn't retry)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed after 4 attempts")
	assert.Contains(t, err.Error(), "validation error")
	assert.Equal(t, "", result)

	// Should have made exactly 2 attempts
	expectedLog := []string{
		"attempt-1",        // First attempt fails with network error
		"retry-callback-1", // Callback for retry
		"attempt-2",        // Second attempt fails with validation error (no retry)
	}
	assert.Equal(t, expectedLog, callLog)

	// Should have taken at least 50ms (one backoff period)
	assert.Greater(t, duration, 40*time.Millisecond)
	assert.Less(t, duration, 150*time.Millisecond)
}

func TestCreateRetryableWorkFunc_NonPositiveAttempts(t *testing.T) {
	tests := []struct {
		name        string
		maxAttempts int
	}{
		{
			name:        "zero attempts",
			maxAttempts: 0,
		},
		{
			name:        "negative attempts",
			maxAttempts: -3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			work := CreateRetryableWorkFunc(func(ctx context.Context, n int) (int, error) {
				calls++
				return n * 2, nil
			}, WithRetries(tt.maxAttempts))

			result, err := work(context.Background(), 21)
			require.NoError(t, err)
			assert.Equal(t, 42, result)
			assert.Equal(t, 1, calls)
		})
	}
}

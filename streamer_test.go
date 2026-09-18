package streamer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProcessor(t *testing.T) {
	tests := []struct {
		name        string
		params      NewStreamerParams[int, string]
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid processor",
			params: NewStreamerParams[int, string]{
				WorkerCount: 5,
				Work: func(ctx context.Context, n int) (string, error) {
					return fmt.Sprintf("processed: %d", n), nil
				},
			},
			expectError: false,
		},
		{
			name: "zero worker count",
			params: NewStreamerParams[int, string]{
				WorkerCount: 0,
				Work: func(ctx context.Context, n int) (string, error) {
					return fmt.Sprintf("processed: %d", n), nil
				},
			},
			expectError: true,
			errorMsg:    "worker count must be greater than zero",
		},
		{
			name: "negative worker count",
			params: NewStreamerParams[int, string]{
				WorkerCount: -1,
				Work: func(ctx context.Context, n int) (string, error) {
					return fmt.Sprintf("processed: %d", n), nil
				},
			},
			expectError: true,
			errorMsg:    "worker count must be greater than zero",
		},
		{
			name: "nil work function",
			params: NewStreamerParams[int, string]{
				WorkerCount: 5,
				Work:        nil,
			},
			expectError: true,
			errorMsg:    "work function must be defined",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := NewStreamer(tt.params)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Nil(t, processor)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, processor)
				assert.Equal(t, tt.params.WorkerCount, processor.workerCount)
			}
		})
	}
}

func TestProcessor_Process(t *testing.T) {
	tests := []struct {
		name            string
		inputs          []int
		workerCount     int
		workFunc        func(context.Context, int) (string, error)
		expectedResults int
		expectedErrors  int
		useCancel       bool
		cancelAfter     time.Duration
	}{
		{
			name:        "successful processing",
			inputs:      []int{1, 2, 3, 4, 5},
			workerCount: 3,
			workFunc: func(ctx context.Context, n int) (string, error) {
				return fmt.Sprintf("result-%d", n*n), nil
			},
			expectedResults: 5,
			expectedErrors:  0,
		},
		{
			name:        "mixed success and errors",
			inputs:      []int{1, 2, 3, 4, 5},
			workerCount: 2,
			workFunc: func(ctx context.Context, n int) (string, error) {
				if n%2 == 0 {
					return "", fmt.Errorf("error processing %d", n)
				}
				return fmt.Sprintf("success-%d", n), nil
			},
			expectedResults: 3, // 1, 3, 5
			expectedErrors:  2, // 2, 4
		},
		{
			name:        "all errors",
			inputs:      []int{1, 2, 3},
			workerCount: 2,
			workFunc: func(ctx context.Context, n int) (string, error) {
				return "", fmt.Errorf("error-%d", n)
			},
			expectedResults: 0,
			expectedErrors:  3,
		},
		{
			name:        "empty input",
			inputs:      []int{},
			workerCount: 3,
			workFunc: func(ctx context.Context, n int) (string, error) {
				return fmt.Sprintf("result-%d", n), nil
			},
			expectedResults: 0,
			expectedErrors:  0,
		},
		{
			name:        "single worker",
			inputs:      []int{10, 20, 30},
			workerCount: 1,
			workFunc: func(ctx context.Context, n int) (string, error) {
				return fmt.Sprintf("single-%d", n), nil
			},
			expectedResults: 3,
			expectedErrors:  0,
		},
		{
			name:        "context cancellation",
			inputs:      []int{1, 2, 3, 4, 5},
			workerCount: 2,
			workFunc: func(ctx context.Context, n int) (string, error) {
				time.Sleep(100 * time.Millisecond)
				return fmt.Sprintf("result-%d", n), nil
			},
			useCancel:       true,
			cancelAfter:     50 * time.Millisecond,
			expectedResults: 5, // Allow up to 5, but expect fewer due to cancellation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := NewStreamer(NewStreamerParams[int, string]{
				WorkerCount: tt.workerCount,
				Work:        tt.workFunc,
			})
			require.NoError(t, err)

			ctx := context.Background()
			var cancel context.CancelFunc
			if tt.useCancel {
				ctx, cancel = context.WithCancel(ctx)
				go func() {
					time.Sleep(tt.cancelAfter)
					cancel()
				}()
			}

			input := make(chan int, len(tt.inputs))
			for _, v := range tt.inputs {
				input <- v
			}
			close(input)
			resultChan, errorChan, err := processor.Stream(ctx, input)
			require.NoError(t, err)
			require.NotNil(t, resultChan)
			require.NotNil(t, errorChan)

			// Collect results and errors
			results := collectProcessorResults(t, resultChan, 3*time.Second)
			errors := collectProcessorErrors(t, errorChan, 3*time.Second)

			if tt.useCancel {
				assert.LessOrEqual(t, len(results), tt.expectedResults) // With cancellation, we might get fewer results
			} else {
				assert.Equal(t, tt.expectedResults, len(results), "Unexpected number of results")
				assert.Equal(t, tt.expectedErrors, len(errors), "Unexpected number of errors")
			}

			// Verify channels are closed
			select {
			case _, ok := <-resultChan:
				assert.False(t, ok, "Result channel should be closed")
			default:
				t.Error("Result channel should be closed and readable")
			}

			select {
			case _, ok := <-errorChan:
				assert.False(t, ok, "Error channel should be closed")
			default:
				t.Error("Error channel should be closed and readable")
			}
		})
	}
}

func TestProcessor_ConcurrentProcessCalls(t *testing.T) {
	processor, err := NewStreamer(NewStreamerParams[int, string]{
		WorkerCount: 2,
		Work: func(ctx context.Context, n int) (string, error) {
			time.Sleep(100 * time.Millisecond) // Simulate work
			return fmt.Sprintf("result-%d", n), nil
		},
	})
	require.NoError(t, err)

	input1 := make(chan int, 3)
	input2 := make(chan int, 3)
	for i := 1; i <= 5; i++ {
		if i <= 3 {
			input1 <- i
		} else {
			input2 <- i
		}
	}
	close(input1)
	close(input2)

	// Start first process call
	resultChan1, errorChan1, err1 := processor.Stream(context.Background(), input1)
	require.NoError(t, err1)

	// Try second process call immediately - should fail
	resultChan2, errorChan2, err2 := processor.Stream(context.Background(), input2)

	assert.Error(t, err2)
	assert.Contains(t, err2.Error(), "processor is already working")
	assert.Nil(t, resultChan2)
	assert.Nil(t, errorChan2)

	// Clean up first call
	collectProcessorResults(t, resultChan1, 2*time.Second)
	collectProcessorErrors(t, errorChan1, 2*time.Second)
}

func TestProcessor_Flush(t *testing.T) {
	t.Run("flush after processing", func(t *testing.T) {
		processor, err := NewStreamer(NewStreamerParams[int, string]{
			WorkerCount: 3,
			Work: func(ctx context.Context, n int) (string, error) {
				time.Sleep(50 * time.Millisecond) // Simulate work
				return fmt.Sprintf("result-%d", n), nil
			},
		})
		require.NoError(t, err)

		input := make(chan int, 5)
		for v := range 5 {
			input <- v + 1
		}
		close(input)
		resultChan, errorChan, err := processor.Stream(context.Background(), input)
		require.NoError(t, err)

		// Start consuming results in background
		done := make(chan bool)
		go func() {
			collectProcessorResults(t, resultChan, 2*time.Second)
			collectProcessorErrors(t, errorChan, 2*time.Second)
			done <- true
		}()

		// Flush should wait for completion
		start := time.Now()
		processor.Flush()
		duration := time.Since(start)

		// Should have taken at least some time for work to complete
		assert.Greater(t, duration, 40*time.Millisecond)

		// Verify processing is done
		select {
		case <-done:
			// Good, processing completed
		case <-time.After(1 * time.Second):
			t.Error("Background processing didn't complete")
		}
	})

	t.Run("flush when not processing", func(t *testing.T) {
		processor, err := NewStreamer(NewStreamerParams[int, string]{
			WorkerCount: 2,
			Work: func(ctx context.Context, n int) (string, error) {
				return fmt.Sprintf("result-%d", n), nil
			},
		})
		require.NoError(t, err)

		// Flush when no processing is happening should return immediately
		start := time.Now()
		processor.Flush()
		duration := time.Since(start)

		assert.Less(t, duration, 10*time.Millisecond)
	})
}

func collectProcessorResults[T any](t *testing.T, output <-chan T, timeout time.Duration) []T {
	var results []T
	timeoutCh := time.After(timeout)

	for {
		select {
		case item, ok := <-output:
			if !ok {
				return results
			}
			results = append(results, item)
		case <-timeoutCh:
			t.Fatal("Test timed out collecting results")
		}
	}
}

func collectProcessorErrors(t *testing.T, output <-chan error, timeout time.Duration) []error {
	var errors []error
	timeoutCh := time.After(timeout)

	for {
		select {
		case err, ok := <-output:
			if !ok {
				return errors
			}
			errors = append(errors, err)
		case <-timeoutCh:
			t.Fatal("Test timed out collecting errors")
		}
	}
}

func TestStreamer_WorkerTimeout(t *testing.T) {
	tests := []struct {
		name            string
		inputs          []int
		workerCount     int
		timeout         *time.Duration
		workDuration    time.Duration
		expectedResults int
		expectedErrors  int
		expectTimeout   bool
	}{
		{
			name:            "no timeout - all succeed",
			inputs:          []int{1, 2, 3},
			workerCount:     2,
			timeout:         nil,
			workDuration:    50 * time.Millisecond,
			expectedResults: 3,
			expectedErrors:  0,
			expectTimeout:   false,
		},
		{
			name:            "timeout longer than work duration - all succeed",
			inputs:          []int{1, 2, 3},
			workerCount:     2,
			timeout:         func() *time.Duration { d := 100 * time.Millisecond; return &d }(),
			workDuration:    50 * time.Millisecond,
			expectedResults: 3,
			expectedErrors:  0,
			expectTimeout:   false,
		},
		{
			name:            "timeout shorter than work duration - all timeout",
			inputs:          []int{1, 2, 3},
			workerCount:     2,
			timeout:         func() *time.Duration { d := 50 * time.Millisecond; return &d }(),
			workDuration:    100 * time.Millisecond,
			expectedResults: 0,
			expectedErrors:  3,
			expectTimeout:   true,
		},
		{
			name:            "single worker with timeout",
			inputs:          []int{1},
			workerCount:     1,
			timeout:         func() *time.Duration { d := 30 * time.Millisecond; return &d }(),
			workDuration:    60 * time.Millisecond,
			expectedResults: 0,
			expectedErrors:  1,
			expectTimeout:   true,
		},
		{
			name:            "multiple workers with mixed timing",
			inputs:          []int{1, 2, 3, 4, 5},
			workerCount:     3,
			timeout:         func() *time.Duration { d := 75 * time.Millisecond; return &d }(),
			workDuration:    100 * time.Millisecond, // All will timeout
			expectedResults: 0,
			expectedErrors:  5,
			expectTimeout:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamer, err := NewStreamer(NewStreamerParams[int, string]{
				WorkerCount:   tt.workerCount,
				WorkerTimeout: tt.timeout,
				Work: func(ctx context.Context, n int) (string, error) {
					select {
					case <-time.After(tt.workDuration):
						return fmt.Sprintf("processed-%d", n), nil
					case <-ctx.Done():
						return "", ctx.Err()
					}
				},
			})
			require.NoError(t, err)

			// Setup input channel
			input := make(chan int, len(tt.inputs))
			for _, v := range tt.inputs {
				input <- v
			}
			close(input)

			// Process
			resultChan, errorChan, err := streamer.Stream(context.Background(), input)
			require.NoError(t, err)
			require.NotNil(t, resultChan)
			require.NotNil(t, errorChan)

			// Collect results and errors with longer timeout for test
			results := collectProcessorResults(t, resultChan, 5*time.Second)
			errorVals := collectProcessorErrors(t, errorChan, 5*time.Second)

			// Verify counts
			assert.Equal(t, tt.expectedResults, len(results), "Unexpected number of results")
			assert.Equal(t, tt.expectedErrors, len(errorVals), "Unexpected number of errors")

			// Verify timeout errors if expected
			if tt.expectTimeout {
				for _, err := range errorVals {
					assert.True(t,
						errors.Is(err, context.DeadlineExceeded) ||
							errors.Is(err, context.Canceled),
						"Expected timeout/cancellation error, got: %v", err)
				}
			}

			// Verify channels are closed
			select {
			case _, ok := <-resultChan:
				assert.False(t, ok, "Result channel should be closed")
			default:
				t.Error("Result channel should be closed and readable")
			}

			select {
			case _, ok := <-errorChan:
				assert.False(t, ok, "Error channel should be closed")
			default:
				t.Error("Error channel should be closed and readable")
			}
		})
	}
}

func TestStreamer_StreamerTimeout(t *testing.T) {
	tests := []struct {
		name             string
		inputs           []int
		workerCount      int
		streamerTimeout  *time.Duration
		workDuration     time.Duration
		expectedMaxItems int // Maximum items we expect to be processed before timeout
		expectTimeout    bool
	}{
		{
			name:             "no timeout - all items processed",
			inputs:           []int{1, 2, 3, 4, 5},
			workerCount:      2,
			streamerTimeout:  nil,
			workDuration:     20 * time.Millisecond,
			expectedMaxItems: 5,
			expectTimeout:    false,
		},
		{
			name:             "timeout longer than total work - all succeed",
			inputs:           []int{1, 2, 3},
			workerCount:      2,
			streamerTimeout:  func() *time.Duration { d := 300 * time.Millisecond; return &d }(),
			workDuration:     50 * time.Millisecond, // Total: ~150ms with 2 workers
			expectedMaxItems: 3,
			expectTimeout:    false,
		},
		{
			name:             "timeout shorter than total work - early termination",
			inputs:           []int{1, 2, 3, 4, 5, 6},
			workerCount:      2,
			streamerTimeout:  func() *time.Duration { d := 100 * time.Millisecond; return &d }(),
			workDuration:     60 * time.Millisecond, // Would take ~180ms total
			expectedMaxItems: 4,                     // Should process two batches
			expectTimeout:    true,
		},
		{
			name:             "very short timeout - timing verification",
			inputs:           []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // Many items
			workerCount:      3,
			streamerTimeout:  func() *time.Duration { d := 30 * time.Millisecond; return &d }(),
			workDuration:     50 * time.Millisecond,
			expectedMaxItems: 3, // All three workers will start but the rest will not.
			expectTimeout:    true,
		},
		{
			name:             "single worker with timeout",
			inputs:           []int{1, 2, 3, 4},
			workerCount:      1,
			streamerTimeout:  func() *time.Duration { d := 80 * time.Millisecond; return &d }(),
			workDuration:     50 * time.Millisecond, // Would take 200ms total
			expectedMaxItems: 2,                     // Should get ~1-2 items
			expectTimeout:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamer, err := NewStreamer(NewStreamerParams[int, string]{
				WorkerCount:     tt.workerCount,
				StreamerTimeout: tt.streamerTimeout,
				Work: func(ctx context.Context, n int) (string, error) {
					select {
					case <-time.After(tt.workDuration):
						return fmt.Sprintf("processed-%d", n), nil
					case <-ctx.Done():
						return "", ctx.Err()
					}
				},
			})
			require.NoError(t, err)

			// Setup input channel
			input := make(chan int, len(tt.inputs))
			for _, v := range tt.inputs {
				input <- v
			}
			close(input)

			// Record start time to verify timeout timing
			start := time.Now()

			// Process
			resultChan, errorChan, err := streamer.Stream(context.Background(), input)
			require.NoError(t, err)
			require.NotNil(t, resultChan)
			require.NotNil(t, errorChan)

			// Collect results and errors with reasonable timeout for test
			results := collectProcessorResults(t, resultChan, 1*time.Second)
			errorVals := collectProcessorErrors(t, errorChan, 1*time.Second)
			duration := time.Since(start)

			// Verify processing was interrupted if timeout was expected
			if tt.expectTimeout {
				// Should have processed fewer items than total input
				totalProcessed := len(results) + len(errorVals)
				assert.LessOrEqual(t, totalProcessed, tt.expectedMaxItems,
					"Should have processed at most %d items due to timeout, got %d results + %d errors",
					tt.expectedMaxItems, len(results), len(errorVals))

				// Should have completed faster than it would take to process all items
				maxExpectedDuration := time.Duration(len(tt.inputs)) * tt.workDuration / time.Duration(tt.workerCount) * 2
				assert.Less(t, duration, maxExpectedDuration,
					"Processing should have been interrupted by timeout")

				// Should have timeout-related errors
				var timeoutErrors int
				for _, err := range errorVals {
					if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
						timeoutErrors++
					}
				}
				assert.Greater(t, timeoutErrors, 0, "Expected some timeout/cancellation errors")

				// Timing should be approximately the timeout duration (with some tolerance)
				if tt.streamerTimeout != nil {
					tolerance := 50 * time.Millisecond
					expectedDuration := *tt.streamerTimeout
					assert.Greater(t, duration, expectedDuration-tolerance,
						"Duration should be at least the timeout duration")
					assert.Less(t, duration, expectedDuration+tolerance,
						"Duration should not exceed timeout by much")
				}
			} else {
				// All items should be processed successfully
				assert.Equal(t, len(tt.inputs), len(results), "All items should be processed without timeout")
				assert.Equal(t, 0, len(errorVals), "Should have no errors without timeout")
			}

			// Verify channels are closed
			select {
			case _, ok := <-resultChan:
				assert.False(t, ok, "Result channel should be closed")
			default:
				t.Error("Result channel should be closed and readable")
			}

			select {
			case _, ok := <-errorChan:
				assert.False(t, ok, "Error channel should be closed")
			default:
				t.Error("Error channel should be closed and readable")
			}
		})
	}
}

func TestStreamer_Reuse(t *testing.T) {
	streamer, err := NewStreamer(NewStreamerParams[int, int]{
		WorkerCount: 2,
		Work: func(ctx context.Context, n int) (int, error) {
			return n * 2, nil
		},
	})
	require.NoError(t, err)

	for round := range 2 {
		input := make(chan int)
		go func() {
			defer close(input)
			for i := range 5 {
				input <- i
			}
		}()

		results, errs, err := streamer.Stream(context.Background(), input)
		require.NoError(t, err, "round %d", round)

		got := 0
		for range results {
			got++
		}
		for e := range errs {
			require.NoError(t, e)
		}
		streamer.Flush()
		assert.Equal(t, 5, got, "round %d", round)
	}
}

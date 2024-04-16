// Package retry provides methods to retry operations
package retry

import (
	"time"
)

func WithRetry(maxRetries int, operation func(attempt int) error) error {
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err = operation(attempt); err == nil {
			return nil
		}

		time.Sleep(time.Duration(attempt) * time.Second)
	}

	return err
}

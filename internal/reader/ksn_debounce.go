package reader

import (
	"fmt"
	"time"
)

func parseKSNDebounce(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, fmt.Errorf("duration must be non-negative")
	}
	return d, nil
}

// debounceKSN owns all pending state in one goroutine. A fixed window from
// the first notification bounds latency even for continuously updated keys.
// Closing input flushes every accepted key synchronously before returning.
func debounceKSN(input <-chan dbKey, interval time.Duration, emit func(dbKey)) {
	if interval == 0 {
		for k := range input {
			emit(k)
		}
		return
	}
	pending := make(map[dbKey]time.Time)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case k, ok := <-input:
			if !ok {
				for k := range pending {
					emit(k)
				}
				return
			}
			if _, exists := pending[k]; !exists {
				pending[k] = time.Now().Add(interval)
			}
		case now := <-ticker.C:
			for k, deadline := range pending {
				if !now.Before(deadline) {
					delete(pending, k)
					emit(k)
				}
			}
		}
	}
}

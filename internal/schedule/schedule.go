// Package schedule wraps cron expression parsing and next-run calculation.
package schedule

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Parse validates a 5-field cron expression (or descriptor such as @daily).
func Parse(spec string) (cron.Schedule, error) {
	s, err := parser.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("invalid cron schedule %q: %w", spec, err)
	}
	return s, nil
}

// Next returns the first activation time strictly after `after`.
func Next(spec string, after time.Time) (time.Time, error) {
	s, err := Parse(spec)
	if err != nil {
		return time.Time{}, err
	}
	return s.Next(after), nil
}


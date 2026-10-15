package schedule

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	valid := []string{"0 2 * * *", "*/5 * * * *", "@daily", "@hourly", "30 3 * * 1-5"}
	for _, v := range valid {
		if _, err := Parse(v); err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", v, err)
		}
	}
	invalid := []string{"", "not a cron", "61 * * * *", "* * * *", "0 0 0 0 0 0 0"}
	for _, v := range invalid {
		if _, err := Parse(v); err == nil {
			t.Errorf("Parse(%q) expected error", v)
		}
	}
}

func TestNext(t *testing.T) {
	base := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		spec string
		want time.Time
	}{
		{"0 2 * * *", time.Date(2026, 10, 16, 2, 0, 0, 0, time.UTC)},
		{"*/15 * * * *", time.Date(2026, 10, 15, 12, 15, 0, 0, time.UTC)},
		{"@hourly", time.Date(2026, 10, 15, 13, 0, 0, 0, time.UTC)},
		{"0 12 * * *", time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)}, // strictly after
	}
	for _, tc := range tests {
		got, err := Next(tc.spec, base)
		if err != nil {
			t.Fatalf("Next(%q): %v", tc.spec, err)
		}
		if !got.Equal(tc.want) {
			t.Errorf("Next(%q) = %v, want %v", tc.spec, got, tc.want)
		}
	}
	if _, err := Next("bogus", base); err == nil {
		t.Error("expected error for bogus spec")
	}
}


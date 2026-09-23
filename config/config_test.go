package config

import (
	"math"
	"strings"
	"testing"
)

func TestForkScanConfigModes(t *testing.T) {
	for _, tt := range []struct {
		yaml                string
		lookback            int64
		interval            int
		continuous, invalid bool
	}{
		{"{}", 64, 60, false, false},
		{"fork_scan_lookback: 17", 17, 60, false, false},
		{"fork_scan_lookback: 0", 0, 60, false, false},
		{"fork_scan_lookback: -1", -1, 60, true, false},
		{"fork_scan_lookback: -1\nfork_scan_interval_sec: 0", -1, 0, false, false},
		{"fork_scan_lookback: -1\nfork_scan_interval_sec: -1", -1, -1, false, false},
		{"fork_scan_lookback: -2", -2, 60, false, true},
		{"fork_scan_lookback: -2\nfork_scan_interval_sec: 0", -2, 0, false, true},
		{"fork_scan_lookback: 9223372036854775807", math.MaxInt64, 60, false, false},
		{"fork_scan_lookback: 18446744073709551615", 0, 0, false, true},
		{"fork_scan_interval_sec: 9223372036854775807", 0, 0, false, true},
	} {
		t.Run(tt.yaml, func(t *testing.T) {
			c, err := decodeConfig(strings.NewReader(tt.yaml))
			if tt.invalid {
				if err == nil {
					t.Fatal("expected config error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.ForkScanLookback != tt.lookback || c.ForkScanInterval != tt.interval || c.ContinuousForkScan() != tt.continuous {
				t.Fatalf("unexpected config: %+v", c)
			}
		})
	}
}

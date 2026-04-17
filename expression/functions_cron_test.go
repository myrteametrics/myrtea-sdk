package expression

import (
	"testing"
)

func TestCronThreshold(t *testing.T) {
	entry := func(cronExpr, duration string, threshold float64, priority ...float64) map[string]interface{} {
		m := map[string]interface{}{
			"cron":      cronExpr,
			"duration":  duration,
			"threshold": threshold,
		}
		if len(priority) > 0 {
			m["priority"] = priority[0]
		}
		return m
	}

	tests := []struct {
		name    string
		now     string
		def     float64
		entries []map[string]interface{}
		want    float64
		wantErr bool
	}{
		{
			name: "inside 5h-6h window",
			now:  "2024-01-15T05:30:00Z",
			def:  0,
			entries: []map[string]interface{}{
				entry("0 5 * * *", "1h", 1000),
				entry("0 7 * * 1-6", "17h", 3000),
			},
			want: 1000,
		},
		{
			name:    "at exact cron fire time",
			now:     "2024-01-15T05:00:00Z",
			def:     0,
			entries: []map[string]interface{}{entry("0 5 * * *", "1h", 1000)},
			want:    1000,
		},
		{
			name:    "just before window ends",
			now:     "2024-01-15T05:59:59Z",
			def:     0,
			entries: []map[string]interface{}{entry("0 5 * * *", "1h", 1000)},
			want:    1000,
		},
		{
			name:    "just at window end (exclusive)",
			now:     "2024-01-15T06:00:00Z",
			def:     0,
			entries: []map[string]interface{}{entry("0 5 * * *", "1h", 1000)},
			want:    0,
		},
		{
			name: "inside 7h-0h weekday window",
			now:  "2024-01-15T14:00:00Z",
			def:  0,
			entries: []map[string]interface{}{
				entry("0 5 * * *", "1h", 1000),
				entry("0 7 * * 1-6", "17h", 3000),
			},
			want: 3000,
		},
		{
			name: "sunday: no match returns default",
			now:  "2024-01-14T14:00:00Z",
			def:  0,
			entries: []map[string]interface{}{
				entry("0 5 * * *", "1h", 1000),
				entry("0 7 * * 1-6", "17h", 3000),
			},
			want: 0,
		},
		{
			name:    "midnight window",
			now:     "2024-01-15T00:30:00Z",
			def:     0,
			entries: []map[string]interface{}{entry("0 0 * * *", "1h", 3000)},
			want:    3000,
		},
		{
			name:    "no entries returns default",
			now:     "2024-01-15T05:30:00Z",
			def:     42,
			entries: nil,
			want:    42,
		},
		{
			// Two entries match: second has higher priority
			name: "priority: higher priority wins",
			now:  "2024-01-15T05:30:00Z",
			def:  0,
			entries: []map[string]interface{}{
				entry("0 5 * * *", "1h", 1000, 1),
				entry("* * * * *", "1h", 9999, 5), // also matches, higher priority
			},
			want: 9999,
		},
		{
			// Two entries match with equal priority: first wins
			name: "priority: equal priority keeps first",
			now:  "2024-01-15T05:30:00Z",
			def:  0,
			entries: []map[string]interface{}{
				entry("0 5 * * *", "1h", 1000, 1),
				entry("* * * * *", "1h", 9999, 1),
			},
			want: 1000,
		},
		{
			name:    "invalid cron expression",
			now:     "2024-01-15T05:30:00Z",
			def:     0,
			entries: []map[string]interface{}{entry("not-a-cron", "1h", 1000)},
			wantErr: true,
		},
		{
			name:    "invalid duration",
			now:     "2024-01-15T05:30:00Z",
			def:     0,
			entries: []map[string]interface{}{entry("0 5 * * *", "bad", 1000)},
			wantErr: true,
		},
		{
			name:    "missing cron field",
			now:     "2024-01-15T05:30:00Z",
			def:     0,
			entries: []map[string]interface{}{{"duration": "1h", "threshold": float64(1000)}},
			wantErr: true,
		},
		{
			name:    "missing threshold field",
			now:     "2024-01-15T05:30:00Z",
			def:     0,
			entries: []map[string]interface{}{{"cron": "0 5 * * *", "duration": "1h"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []interface{}{tt.now, tt.def}
			for _, e := range tt.entries {
				args = append(args, e)
			}

			got, err := cronThreshold(args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("cronThreshold() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				val, ok := toFloat64(got)
				if !ok {
					t.Errorf("cronThreshold() returned non-numeric value: %v", got)
					return
				}
				if val != tt.want {
					t.Errorf("cronThreshold() = %v, want %v", val, tt.want)
				}
			}
		})
	}
}

func TestCronThresholdViaGval(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		vars       map[string]interface{}
		want       float64
	}{
		{
			name: "5h-6h window",
			expression: `cron_threshold(now, 0,
				{"cron": "0 5 * * *",   "duration": "1h",  "threshold": 1000},
				{"cron": "0 0 * * *",   "duration": "1h",  "threshold": 3000},
				{"cron": "0 7 * * 1-6", "duration": "17h", "threshold": 1})`,
			vars: map[string]interface{}{"now": "2024-01-15T05:45:00Z"},
			want: 1000,
		},
		{
			name: "midnight window",
			expression: `cron_threshold(now, 0,
				{"cron": "0 5 * * *",   "duration": "1h",  "threshold": 1000},
				{"cron": "0 0 * * *",   "duration": "1h",  "threshold": 3000},
				{"cron": "0 7 * * 1-6", "duration": "17h", "threshold": 1})`,
			vars: map[string]interface{}{"now": "2024-01-15T00:30:00Z"},
			want: 3000,
		},
		{
			name: "weekday daytime",
			expression: `cron_threshold(now, 0,
				{"cron": "0 5 * * *",   "duration": "1h",  "threshold": 1000},
				{"cron": "0 0 * * *",   "duration": "1h",  "threshold": 3000},
				{"cron": "0 7 * * 1-6", "duration": "17h", "threshold": 1})`,
			vars: map[string]interface{}{"now": "2024-01-15T14:00:00Z"},
			want: 1,
		},
		{
			name: "sunday uses default",
			expression: `cron_threshold(now, 999,
				{"cron": "0 5 * * *",   "duration": "1h",  "threshold": 1000},
				{"cron": "0 7 * * 1-6", "duration": "17h", "threshold": 1})`,
			vars: map[string]interface{}{"now": "2024-01-14T14:00:00Z"},
			want: 999,
		},
		{
			name: "priority resolves conflict",
			expression: `cron_threshold(now, 0,
				{"cron": "0 5 * * *", "duration": "1h", "threshold": 1000, "priority": 1},
				{"cron": "* * * * *", "duration": "1h", "threshold": 9999, "priority": 5})`,
			vars: map[string]interface{}{"now": "2024-01-15T05:30:00Z"},
			want: 9999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Process(LangEval, tt.expression, tt.vars)
			if err != nil {
				t.Errorf("Process() error = %v", err)
				return
			}
			val, ok := toFloat64(result)
			if !ok {
				t.Errorf("Process() returned non-numeric value: %v", result)
				return
			}
			if val != tt.want {
				t.Errorf("Process() = %v, want %v", val, tt.want)
			}
		})
	}
}

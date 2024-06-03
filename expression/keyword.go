package expression

import (
	"time"

	"github.com/myrteametrics/myrtea-sdk/v5/utils"
)

// GetDateKeywords return a list of standard date time placeholders
func GetDateKeywords(t time.Time) map[string]interface{} {
	values := map[string]interface{}{
		"now":            t.Format(utils.TimeLayout),
		"begin":          utils.GetBeginningOfDay(t), // @Deprecated - keep for compatibility
		"startofday":     utils.GetBeginningOfDay(t),
		"startofnextday": utils.GetBeginningOfDay(t.Add(24 * time.Hour)),
		"startofmonth":   utils.GetBeginningOfMonth(t),
	}
	return values
}

func GetValidDayNames() []string {
	return []string{"monday",
		"tuesday",
		"wednesday",
		"thursday",
		"friday",
		"saturday",
		"sunday",
	}
}

package expression

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v4/utils"
)

// dayOfWeek returns the input date day of week (1 to 7)
// Usage: <date>
func dayOfWeek(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 1 {
		return nil, fmt.Errorf("dayOfWeek() expects exactly one string argument")
	}
	s, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("dayOfWeek() expects exactly one string argument")
	}
	t, _, err := parseDateAllFormat(s)
	if err != nil {
		return nil, fmt.Errorf("dayOfWeek() %s", err.Error())
	}
	// One-line math for Monday = 1, Sunday = 7 (instead of 0)
	return ((int(t.Weekday()) + 6) % 7) + 1, nil
}

// day returns the input date in day (day of month)
// Usage: <date>
func day(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 1 {
		return nil, fmt.Errorf("day() expects exactly one string argument")
	}
	s, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("day() expects exactly one string argument")
	}
	t, _, err := parseDateAllFormat(s)
	if err != nil {
		return nil, fmt.Errorf("day() %s", err.Error())
	}
	return t.Day(), nil
}

// month returns the input date month
// Usage: <date>
func month(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 1 {
		return nil, fmt.Errorf("month() expects exactly one string argument")
	}
	s, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("month() expects exactly one string argument")
	}
	t, _, err := parseDateAllFormat(s)
	if err != nil {
		return nil, fmt.Errorf("month() %s", err.Error())
	}
	return int(t.Month()), nil
}

// year returns the input date year
// Usage: <date>
func year(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 1 {
		return nil, fmt.Errorf("year() expects exactly one string argument")
	}
	s, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("year() expects exactly one string argument")
	}
	t, _, err := parseDateAllFormat(s)
	if err != nil {
		return nil, fmt.Errorf("year() %s", err.Error())
	}
	return t.Year(), nil
}

func startOf(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 2 {
		return nil, fmt.Errorf("startOf() expects exactly two string argument")
	}
	s, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("startOf() expects exactly two string argument")
	}
	t, _, err := parseDateAllFormat(s)
	if err != nil {
		return nil, fmt.Errorf("startOf() %s", err.Error())
	}

	startOf, ok := arguments[1].(string)
	if !ok {
		return nil, fmt.Errorf("expects exactly two string argument")
	}
	switch startOf {
	case "day":
		return utils.GetBeginningOfDay(t), nil
	case "month":
		return utils.GetBeginningOfMonth(t), nil
	case "year":
		return utils.GetBeginningOfYear(t), nil
	}
	return nil, fmt.Errorf("startOf() expect 'day', 'month' or 'year'")
}

func endOf(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 2 {
		return nil, fmt.Errorf("endOf() expects exactly two string argument")
	}
	s, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("endOf() expects exactly two string argument")
	}
	t, _, err := parseDateAllFormat(s)
	if err != nil {
		return nil, fmt.Errorf("endOf() %s", err.Error())
	}

	endOf, ok := arguments[1].(string)
	if !ok {
		return nil, fmt.Errorf("expects exactly two string argument")
	}
	switch endOf {
	case "day":
		return utils.GetEndOfDay(t), nil
	case "month":
		return utils.GetEndOfMonth(t), nil
	case "year":
		return utils.GetEndOfYear(t), nil
	}
	return nil, fmt.Errorf("endOf() expect 'day', 'month' or 'year'")
}

// dateToMillis returns the input date in milliseconds
// Usage: <date>
func dateToMillis(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 1 {
		return nil, fmt.Errorf("dateToMillis() expects exactly one string argument")
	}
	s, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("dateToMillis() expects exactly one string argument")
	}
	t, _, err := parseDateAllFormat(s)
	if err != nil {
		return nil, fmt.Errorf("dateToMillis() %s", err.Error())
	}
	return t.UnixNano() / 1e6, nil
}

// delayInDays returns the duration between two date in open days/time
// Usage: <date1> <date2> [calendar_name]
func delayInDays(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 2 {
		return nil, fmt.Errorf("delayInDays() expects exactly 2 string argument")
	}
	s1, ok1 := arguments[0].(string)
	s2, ok2 := arguments[1].(string)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("delayInDays() expects exactly 2 string argument")
	}

	t1, _, err := parseDateAllFormat(s1)
	if err != nil {
		return nil, fmt.Errorf("delayInDays() %s", err.Error())
	}
	t2, _, err := parseDateAllFormat(s2)
	if err != nil {
		return nil, fmt.Errorf("delayInDays() %s", err.Error())
	}
	if t1.IsZero() || t2.IsZero() {
		return nil, fmt.Errorf("delayInDays() at least one date is empty")
	}
	return t2.Sub(t1).Nanoseconds() / 1e6, nil
}

// addDurationDays adds a duration in "open days/time" to a specific date
// Usage: <date> <duration> [calendar_name]
func addDurationDays(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 2 {
		return nil, fmt.Errorf("addDurationDays() expects exactly 2 string argument")
	}
	s1, ok1 := arguments[0].(string)
	s2, ok2 := arguments[1].(string)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("addDurationDays() expects exactly 2 string argument")
	}
	d, err := time.ParseDuration(s2)
	if err != nil {
		return nil, fmt.Errorf("addDurationDays() %s", err.Error())
	}
	t, _, err := parseDateAllFormat(s1)
	if err != nil {
		return nil, fmt.Errorf("addDurationDays() %s", err.Error())
	}
	return t.Add(d).Format(utils.TimeLayout), nil
}

func truncateDate(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 2 {
		return nil, fmt.Errorf("truncateDate() expects exactly 2 string argument")
	}
	s1, ok1 := arguments[0].(string)
	s2, ok2 := arguments[1].(string)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("truncateDate() expects exactly 2 string argument")
	}
	t, _, err := parseDateAllFormat(s1)
	if err != nil {
		return nil, fmt.Errorf("truncateDate() %s", err.Error())
	}
	d, err := time.ParseDuration(s2)
	if err != nil {
		return nil, fmt.Errorf("truncateDate() %s", err.Error())
	}
	return t.Truncate(d).Format(utils.TimeLayout), nil
}

func extractFromDate(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 2 {
		return nil, fmt.Errorf("extractFromDate() expects exactly two string argument <date> <component>")
	}
	s1, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("extractFromDate() expects exactly two string argument <date> <component>")
	}
	s2, ok := arguments[1].(string)
	if !ok {
		return nil, fmt.Errorf("extractFromDate() expects exactly two string argument <date> <component>")
	}
	t, _, err := parseDateAllFormat(s1)
	if err != nil {
		return nil, fmt.Errorf("extractFromDate() %s", err.Error())
	}
	switch s2 {
	case "year":
		return t.Year(), nil
	case "month":
		return int(t.Month()), nil
	case "day":
		return t.Day(), nil
	case "dayOfMonth":
		return ((int(t.Weekday()) + 6) % 7) + 1, nil
	case "hour":
		return t.Hour(), nil
	case "minute":
		return t.Minute(), nil
	case "second":
		return t.Second(), nil
	}
	return nil, fmt.Errorf("extractFromDate() %s is not a valid component", s2)
}

func formatDate(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 2 {
		return nil, fmt.Errorf("formatDate() expects exactly two string argument <date> <format>")
	}

	s1, ok := arguments[0].(string)
	if !ok {
		return nil, fmt.Errorf("formatDate() expects exactly two string argument <date> <format>")
	}
	s2, ok := arguments[1].(string)
	if !ok {
		return nil, fmt.Errorf("formatDate() expects exactly two string argument <date> <format>")
	}
	t, _, err := parseDateAllFormat(s1)
	if err != nil {
		return nil, fmt.Errorf("extractFromDate() %s", err.Error())
	}

	// if s2 layout is wrong, the format function will output given s2 string as result
	return t.Format(s2), nil
}

func getValueForCurrentDay(arguments ...interface{}) (interface{}, error) {
	if len(arguments) != 3 {
		return nil, fmt.Errorf("getValueForCurrentDay() expects 3 arguments, a list of values, a list of days and a default value")
	}

	values, ok := arguments[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("getValueForCurrentDay() list of values is not valid")
	}

	keys, ok := arguments[1].([]interface{})
	if !ok {
		return nil, fmt.Errorf("getValueForCurrentDay() list of days is not valid")
	}

	defaultValue := arguments[2]
	if !ok {
		return nil, fmt.Errorf("getValueForCurrentDay() default value is not valid")
	}

	if len(values) != len(keys) {
		return nil, fmt.Errorf("getValueForCurrentDay() list of values and list of days should have the same size")
	}

	// maybe add French ?
	validDayNames := GetValidDayNames()
	result := map[string]interface{}{}

	// check keys
	for i, keyInt := range keys {
		key, valid := keyInt.(string)

		if valid {
			valid = false
			for _, validDay := range validDayNames {
				if key == validDay {
					valid = true
					break
				}
			}
		}

		if !valid {
			return nil, fmt.Errorf("getValueForCurrentDay() key in keys list not valid: \"%s\" (valid are %s)", key, strings.Join(validDayNames, ", "))
		}

		result[strings.ToLower(key)] = values[i]

	}

	currentDay := strings.ToLower(time.Now().Weekday().String())

	if value, ok := result[currentDay]; ok {
		return value, nil
	}

	return defaultValue, nil
}

func getFormattedDuration(duration, inputUnit, format, separator, keepSeparator, printZeroValues interface{}) string {
	durationTyped, ok := duration.(float64)
	durationInt, ok2 := duration.(int)
	if !ok && !ok2 {
		return "error"
	}
	if ok2 {
		durationTyped = float64(durationInt)
	}
	inputUnitTyped, ok := inputUnit.(string)
	if !ok {
		return "error"
	}
	formatTyped, ok := format.(string)
	if !ok {
		return "error"
	}
	separatorTyped, ok := separator.(string)
	if !ok {
		return "error"
	}
	keepSeparatorTyped, ok := keepSeparator.(bool)
	if !ok {
		return "error"
	}
	printZeroValuesTyped, ok := printZeroValues.(bool)
	if !ok {
		return "error"
	}
	return getFormattedDurationTyped(durationTyped, inputUnitTyped, formatTyped, separatorTyped, keepSeparatorTyped, printZeroValuesTyped)
}

func getFormattedDurationTyped(duration float64, inputUnit, format, separator string, keepSeparator, printZeroValues bool) string {

	durationMs := asMilliseconds(duration, inputUnit)
	nextIndex := 0

	durationFormatSplited := splitFormat(format, separator)

	insertCalculatedUnit(&durationMs, &nextIndex, 1000*60*60*24, &durationFormatSplited, format, "{d}", printZeroValues)
	insertCalculatedUnit(&durationMs, &nextIndex, 1000*60*60, &durationFormatSplited, format, "{h}", printZeroValues)
	insertCalculatedUnit(&durationMs, &nextIndex, 1000*60, &durationFormatSplited, format, "{m}", printZeroValues)
	insertCalculatedUnit(&durationMs, &nextIndex, 1000, &durationFormatSplited, format, "{s}", printZeroValues)
	insertCalculatedUnit(&durationMs, &nextIndex, 1, &durationFormatSplited, format, "{ms}", printZeroValues)

	if keepSeparator {
		return fmt.Sprintf("%v", strings.Join(durationFormatSplited, separator))
	} else {
		return fmt.Sprintf("%v", strings.Join(durationFormatSplited, ""))
	}
}

// Sépare les éléments du format de date
func splitFormat(format, separator string) []string {
	var durationFormatSplited []string
	if separator == "" {
		//Tente une séparation intelligente sans séparateur
		var isTextAfter = strings.HasPrefix(strings.Trim(format, " "), "{")
		if isTextAfter {
			format = strings.Join(strings.Split(format, "{"), "&separator;{")
		} else {
			format = strings.Join(strings.Split(format, "}"), "}&separator;")
		}
		durationFormatSplited = strings.Split(format, "&separator;")
		if isTextAfter {
			durationFormatSplited = durationFormatSplited[1:len(durationFormatSplited)]
		}
	} else {
		durationFormatSplited = strings.Split(format, separator)
	}
	return durationFormatSplited
}

// Convertit le nombre de milliseconds en une autre unité à l'aide de "convertUnit"
// ajoute dans "durationFormatSplited" à la place de "regex" la valeur
// mais retire l'entrée dans le tableau si "printZeroValue" = false
func insertCalculatedUnit(durationMs *float64, nextIndex *int, convertUnit int, durationFormatSplited *[]string, format string, regex string, printZeroValues bool) {
	if strings.Contains(format, regex) {
		unitValue := math.Floor(*durationMs / float64(convertUnit))
		if unitValue > 0 || printZeroValues {
			(*durationFormatSplited)[*nextIndex] = strings.Replace((*durationFormatSplited)[*nextIndex], regex, strconv.Itoa(int(unitValue)), -1)
			*durationMs -= unitValue * float64(convertUnit)
			*nextIndex++
		} else {
			*durationFormatSplited = append((*durationFormatSplited)[:*nextIndex], (*durationFormatSplited)[*nextIndex+1:]...)
		}
	}
}

// Convertit une durée en millisecondes
func asMilliseconds(duration float64, inputUnit string) float64 {
	switch inputUnit {
	case "ms":
		return duration
	case "s":
		return duration * 1000
	case "m":
		return asMilliseconds(duration*60, "s")
	case "h":
		return asMilliseconds(duration*60, "m")
	case "d":
		return asMilliseconds(duration*24, "h")
	default:
		return 0
	}
}

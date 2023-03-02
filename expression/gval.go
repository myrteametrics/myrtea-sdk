package expression

import (
	"context"
	"fmt"
	"time"

	"github.com/PaesslerAG/gval"
	ttlcache "github.com/myrteametrics/myrtea-sdk/v4/cache"
)

var (
	// cache is a global ttlcache for gval expression
	cache = ttlcache.NewCache(7 * 24 * time.Hour)

	// LangEval is a custom GVal evaluator for business rules and facts conditions
	// It contains all supported custom functions (math, date, dateopendays, etc.)
	LangEval = gval.NewLanguage(
		gval.Full(),
		LangExprMath,
		LangEvalDate,
		LangEvalDateOpenDays,
		LangAdvancedInfix,
		LangEvalMap,
		LangEvalString,
		LangEvalSlice,
	)

	// LangExprMath is a custom GVal evaluator for business rules and facts conditions
	// It contains custom functions related to math
	LangExprMath = gval.NewLanguage(
		gval.Full(),
		gval.Function("length", length),
		gval.Function("max", max),
		gval.Function("min", min),
		gval.Function("sum", sum),
		gval.Function("average", average),
	)

	// LangEvalDate is a custom GVal evaluator for business rules and facts conditions
	// It contains custom functions related to date
	LangEvalDate = gval.NewLanguage(
		gval.Full(),
		gval.Function("dayOfWeek", dayOfWeek),
		gval.Function("day", day),
		gval.Function("month", month),
		gval.Function("year", year),
		gval.Function("startOf", startOf),
		gval.Function("endOf", endOf),
		gval.Function("datemillis", dateToMillis),
		gval.Function("calendar_add", addDurationDays),
		gval.Function("calendar_delay", delayInDays),
		gval.Function("truncate_date", truncateDate),
		gval.Function("extract_from_date", extractFromDate),
	)

	// LangEvalDateOpenDays is a custom GVal evaluator for business rules and facts conditions
	// It contains custom functions related to date (opendays support)
	LangEvalDateOpenDays = gval.NewLanguage(
		gval.Full(),
		gval.Function("calendar_add_od", addDurationOpenDays),
		gval.Function("calendar_delay_od", delayInOpenDays),
	)

	// LangAdvancedInfix is a custom Gval evaluator for maps operations
	LangAdvancedInfix = gval.NewLanguage(
		gval.Full(),
		gval.InfixOperator("+", advancedAddition),
		gval.InfixOperator("-", advancedSubtraction),
		gval.InfixOperator("*", advancedMultiplication),
		gval.InfixOperator("/", advancedDivision),
	)

	LangEvalMap = gval.NewLanguage(
		gval.Full(),
		gval.Function("flatten_fact", flattenFact),
	)

	// LangEvalString is a custom GVal evaluator for handling char arrays (strings)
	LangEvalString = gval.NewLanguage(
		gval.Full(),
		gval.Function("replace", replace),
	)

	LangEvalSlice = gval.NewLanguage(
		gval.Full(),
		gval.Function("contains", contains),
	)
)

// Process process an expression with a map of properties using a specific GVal language
func Process(langEval gval.Language, expression string, variables map[string]interface{}) (interface{}, error) {
	exp, err := getEvaluable(langEval, expression)
	if err != nil {
		return nil, err
	}
	result, err := exp(context.Background(), variables)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("expression returned nil value")
	}
	return result, nil
}

func getEvaluable(langEval gval.Language, expression string) (gval.Evaluable, error) {
	exp, found := cache.Get(expression)
	if found {
		return exp.(gval.Evaluable), nil
	}

	newExp, err := langEval.NewEvaluable(expression)
	if err != nil {
		return nil, err
	}
	cache.Set(expression, newExp)
	return newExp, nil
}

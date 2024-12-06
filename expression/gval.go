package expression

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"

	"github.com/PaesslerAG/gval"
	ttlcache "github.com/myrteametrics/myrtea-sdk/v5/cache"
)

type GlobalVariables struct {
	listKeyValue   map[string]interface{}
	listKeyValueMu sync.RWMutex
}

type MergedVariables struct {
	globalVars *GlobalVariables
	localVars  map[string]interface{}
}

const prefixGlobalVars = "global_variable_"

var (
	GlobalVars *GlobalVariables = &GlobalVariables{listKeyValue: make(map[string]interface{})}

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
		LangEvalUrl,
	)

	// LangExprMath is a custom GVal evaluator for business rules and facts conditions
	// It contains custom functions related to math
	LangExprMath = gval.NewLanguage(
		gval.Full(),
		gval.Function("length", length),
		gval.Function("max", mathMax),
		gval.Function("min", mathMin),
		gval.Function("sum", sum),
		gval.Function("average", average),
		gval.Function("roundToDecimal", roundToDecimal),
		gval.Function("safeDivide", safeDivide),
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
		gval.Function("format_date", formatDate),
		gval.Function("get_value_current_day", getValueForCurrentDay),
		gval.Function("get_formatted_duration", getFormattedDuration),
		gval.Function("numberWithoutExponent", numberWithoutExponent),
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
		gval.Function("append", appendSlice),
	)

	LangEvalUrl = gval.NewLanguage(
		gval.Full(),
		gval.Function("url_encode", urlEncode),
		gval.Function("url_decode", urlDecode),
	)
)

// Process process an expression with a map of properties using a specific GVal language
func Process(langEval gval.Language, expression string, variables map[string]interface{}) (interface{}, error) {
	exp, err := getEvaluable(langEval, expression)
	if err != nil {
		return nil, err
	}

	mergedVars := &MergedVariables{
		globalVars: GlobalVars,
		localVars:  variables,
	}

	result, err := exp(context.Background(), mergedVars.toMap())
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

func (gv *GlobalVariables) Load(listKeyValue map[string]interface{}) {
	zap.L().Info("Fetching global variables")

	gv.listKeyValueMu.Lock()
	defer gv.listKeyValueMu.Unlock()
	for k, v := range listKeyValue {
		gv.listKeyValue[prefixGlobalVars+k] = v
	}
	gv.listKeyValue = listKeyValue
	zap.L().Info("Global variables loaded", zap.Int("count", len(gv.listKeyValue)))
}

func (gv *GlobalVariables) Set(key string, value interface{}) {
	gv.listKeyValueMu.Lock()
	defer gv.listKeyValueMu.Unlock()

	gv.listKeyValue[prefixGlobalVars+key] = value

	zap.L().Info("Global variable set", zap.String("key", prefixGlobalVars+key), zap.Any("value", value), zap.Int("total_count", len(gv.listKeyValue)))
}

func (m *MergedVariables) toMap() map[string]interface{} {

	if len(m.localVars) == 0 {
		m.globalVars.listKeyValueMu.RLock()
		defer m.globalVars.listKeyValueMu.RUnlock()
		return m.globalVars.listKeyValue
	}

	m.globalVars.listKeyValueMu.RLock()
	merged := make(map[string]interface{}, len(m.globalVars.listKeyValue)+len(m.localVars))
	for k, v := range m.globalVars.listKeyValue {
		merged[k] = v
	}
	m.globalVars.listKeyValueMu.RUnlock()

	for k, v := range m.localVars {
		merged[k] = v
	}

	return merged
}

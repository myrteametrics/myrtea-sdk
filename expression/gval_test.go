package expression

import (
	"testing"
	"time"
)

func TestProcess(t *testing.T) {
	variables := map[string]interface{}{"a": 10}

	result, err := Process(LangEval, "a,3", variables)
	if err == nil {
		t.Error("expression should not be evaluable")
	}
	_ = result
}

func TestGetEvaluable(t *testing.T) {
	eval, err := getEvaluable(LangEval, "a,3")
	if err == nil {
		t.Error("expression should not be evaluable")
		t.FailNow()
	}

	eval, err = getEvaluable(LangEval, "a > 1")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	eval, err = getEvaluable(LangEval, "a > 1") // cache
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	_ = eval
}

func TestComplexDate(t *testing.T) {
	eval, err := Process(LangEval, "startOf(calendar_add(startOf(now, \"month\"), \"-24h\"), \"month\")", GetDateKeywords(time.Date(2020, 05, 17, 12, 30, 00, 0, time.UTC)))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if eval != "2020-04-01T00:00:00.000" {
		t.Error("invalid result")
		t.FailNow()
	}
}

func TestAdvancedAddition(t *testing.T) {

	variables := map[string]interface{}{
		"map1": map[string]interface{}{
			"a": 1,
			"b": 2,
		},
		"map2": map[string]interface{}{
			"b": float32(3),
			"c": 4,
		},
		"a": 5,
	}

	eval, err := Process(LangEval, "map1 + map2", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 1 || result["b"].(float64) != 5 || result["c"].(float64) != 4 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 + a", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 6 || result["b"].(float64) != 7 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 + 3", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 4 || result["b"].(float64) != 5 {
		t.Error("Result value is not as expected")
	}

}

func TestAdvancedSubtraction(t *testing.T) {

	variables := map[string]interface{}{
		"map1": map[string]interface{}{
			"a": 5,
			"b": 6,
		},
		"map2": map[string]interface{}{
			"b": float32(3),
			"c": 4,
		},
		"a": 5,
	}

	eval, err := Process(LangEval, "map1 - map2", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 5 || result["b"].(float64) != 3 || result["c"].(float64) != -4 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 - a", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 0 || result["b"].(float64) != 1 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 - 3", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 2 || result["b"].(float64) != 3 {
		t.Error("Result value is not as expected")
	}

}

func TestAdvancedMultiplication(t *testing.T) {

	variables := map[string]interface{}{
		"map1": map[string]interface{}{
			"a": 1,
			"b": 2,
		},
		"map2": map[string]interface{}{
			"b": float32(3),
			"c": 4,
		},
		"a": 5,
	}

	eval, err := Process(LangEval, "map1 * map2", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 0 || result["b"].(float64) != 6 || result["c"].(float64) != 0 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 * a", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 5 || result["b"].(float64) != 10 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 * 3", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"].(float64) != 3 || result["b"].(float64) != 6 {
		t.Error("Result value is not as expected")
	}

}

func TestAdvancedDivision(t *testing.T) {

	variables := map[string]interface{}{
		"map1": map[string]interface{}{
			"a": 6,
			"b": 9,
		},
		"map2": map[string]interface{}{
			"b": float32(3),
			"c": 4,
		},
		"a": 3,
	}

	eval, err := Process(LangEval, "map1 / map2", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"] != nil || result["b"].(float64) != 3 || result["c"].(float64) != 0 {
		t.Error("Result value is not as expected")
	}

	variables["map1"] = result
	eval, err = Process(LangEval, "map1 / a", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})

	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"] != nil || result["b"].(float64) != 1 || result["c"].(float64) != 0 {
		t.Error("Result value is not as expected")
	}

	eval, err = Process(LangEval, "map1 / 3", variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type is not as expected")
	}
	if result["a"] != nil || result["b"].(float64) != 1 || result["c"].(float64) != 0 {
		t.Error("Result value is not as expected")
	}

}

func TestFlatten(t *testing.T){
	variables := map[string]interface{}{
		"fact": []interface{}{map[string]interface{}{
			"aggs": map[string]interface{}{"doc_count":map[string]interface{}{"value":12}},
			"key":"2022-04-11T11:00:00.000"}},
		"key": "key",
		"path":"aggs.doc_count.value",
	}	

	eval, err := Process(LangEval,"flatten_fact(fact,key,path)",variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}	
	resultat, ok := eval.(map[string]interface{})
	if !ok {
		t.Error("Result type not as expected")
	}
	if resultat["2022-04-11T11:00:00.000"].(int) != 2{
		t.Error("result is not as expected")
	}

	eval, err = Process(LangEval,"flatten_fact(fact,key,path) / 2",variables)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}	
	resultat, ok = eval.(map[string]interface{})
	if !ok {
		t.Error("Result type not as expected")
	}
	if resultat["2022-04-11T11:00:00.000"].(float64) != 6{
		t.Error("result is not as expected")
	}
}
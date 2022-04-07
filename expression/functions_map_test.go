package expression

import "testing"




func TestFlatMap(t *testing.T){
	res, err := flatMap(		
		"key",
		"aggs.doc_count.value",
		[]map[string]interface{}{{
			"key": "2022-02-24T05:00:00.000",
			"aggs": map[string]interface{}{
				"doc_count": map[string]interface{}{
					"value": 2,
				},
			},
		},
		{
			"key": "2022-02-24T08:00:00.000",
			"aggs": map[string]interface{}{
				"doc_count": map[string]interface{}{
					"value": 12,
				},
			},
		},
	})

	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	val, ok := res["2022-02-24T05:00:00.000"]
	if !ok {
		t.FailNow()
	}
	if val.(int) != 2 {
		t.Error(err)
		t.Logf("Result: %d, Expected: %d", val.(int), 2)
	}

	val, ok = res["2022-02-24T08:00:00.000"]
	if !ok {
		t.FailNow()
	}
	if val.(int) != 12 {
		t.Error(err)
		t.Logf("Result: %d, Expected: %d", val.(int), 12)
	}
}
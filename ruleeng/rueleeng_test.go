package ruleeng

import (
	"encoding/json"
	"testing"
)

func TestRuleEng(t *testing.T) {

	engine := NewRuleEngine()

	var rule DefaultRule
	json.Unmarshal([]byte(ruleStr), &rule)

	facts := map[string]interface{}{
		"fact_test_1": map[string]interface{}{
			"aggs": map[string]interface{}{
				"agg0":      map[string]interface{}{"value": 1},
				"doc_count": map[string]interface{}{"value": 1},
			},
		},
	}

	engine.InsertRule(&rule)
	engine.knowledgeBase.SetFacts(facts)
	engine.ExecuteAllRules()

	actions := engine.GetResults()

	if actions[0].GetName() != "set" || actions[0].GetParameters()["status.A"].(float64) != 3 {
		t.Errorf("The engine actins are not as expected")
	}
	if actions[1].GetName() != "set" || actions[1].GetParameters()["status.B"].(float64) != 5 {
		t.Errorf("The engine actins are not as expected")
	}
	if actions[2].GetName() != "notify" || actions[2].GetParameters()["id"].(string) != "notify-1" ||
		actions[2].GetParameters()["description"].(string) != "my_description" {
		t.Errorf("The engine actins are not as expected")
	}

}

var ruleStr = `{	
	"cases": [
	  {
		"name": "case1",
		"condition": "fact_test_1.aggs.agg0.value == fact_test_1.aggs.doc_count.value",
		"actions": [
		  {
			"name": "\"set\"",
			"parameters": {
			  "status.A": "param1"
			}
		  },
		  {
			"name": "\"set\"",
			"parameters": {
			  "status.B": "2 + param1"
			}
		  },
		  {
			"name": "\"notify\"",
			"parameters": {
			  "id": "\"notify-1\"",
			  "level": "\"info\"",
			  "title": "\"my_title\"",
			  "description": "param2",
			  "timeout": "\"10s\"",
			  "groups": "[1,2]"
			}
		  }
		]
	  }
	],
	"version": 0,
	"parameters": {
		"param1": 3,
		"param2": "my_description"
	}
  }`

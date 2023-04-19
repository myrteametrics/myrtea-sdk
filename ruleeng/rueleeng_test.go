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
	t.Fail()
	t.Log(actions)
}

func TestRule(t *testing.T) {

	data := map[string]interface{}{
		"fact_test_1": map[string]interface{}{
			"aggs": map[string]interface{}{
				"doc_count": map[string]interface{}{
					"value": 100,
				},
				"doc_count_2": map[string]interface{}{
					"value": 1,
				},
			},
		},
	}

    
	rule1 := DefaultRule{
		ID:         1,
		Version:    1,
		Parameters: make(map[string]interface{}),
		EvaluateAllCases: false,
		Cases: []Case{
			{
				Name:      "case1",
				Condition: "fact_test_1.aggs.doc_count.value > 25",
				Enabled: false,
				Actions: []ActionDef{
					{
						Name: `"action1_Case1_Rule1"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name: `"action2_Case1_Rule1"`,
						 Enabled: false,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
			{
				Name:      "case2",
				Condition: "fact_test_1.aggs.doc_count.value > 25",
				Enabled: true,
				Actions: []ActionDef{
					{
						Name: `"action1_Case2_Rule1"`,
						 Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name: `"action2_Case2_Rule1"`,
						Enabled: false,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
			{
				Name:      "case3",
				Condition: "fact_test_1.aggs.doc_count.value > 25",
				Enabled: true,
				Actions: []ActionDef{
					{
						Name: `"action1_Case3_Rule1"`,
						 Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name: `"action2_Case3_Rule1"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
		},
	}
    
	Actions_Rule1_Expected := []Action{
		DefaultAction{
			Name:       "action1_Case2_Rule1",
			Parameters: map[string]interface{}{"val": "myvalue"},
			MetaData:   map[string]interface{}{"caseName": "case2", "ruleID": "1", "ruleVersion": "1"},
		},
	}
	rule2 := DefaultRule{
		ID:         1,
		Version:    1,
		Parameters: make(map[string]interface{}),
		EvaluateAllCases: true,
		Cases: []Case{
			{
				Name:      "case1",
				Condition: "fact_test_1.aggs.doc_count.value > 25",
				Enabled: true,
				Actions: []ActionDef{
					{
						Name: `"action1_Case1_Rule2"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name: `"action2_Case1_Rule2"`,
						 Enabled: false,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
			{
				Name:      "case2",
				Condition: "fact_test_1.aggs.doc_count.value > 25",
				Enabled: true,
				Actions: []ActionDef{
					{
						Name: `"action1_Case2_Rule2"`,
						 Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name: `"action2_Case2_Rule2"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
			{
				Name:      "case3",
				Condition: "fact_test_1.aggs.doc_count.value > 25",
				Enabled: true,
				Actions: []ActionDef{
					{
						Name: `"action1_Case3_Rule2"`,
						 Enabled: false,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name: `"action2_Case3_Rule2"`,
						Enabled: false,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
			{
				Name:      "case4",
				Condition: "fact_test_1.aggs.doc_count2.value > 25",
				Enabled: true,
				Actions: []ActionDef{
					{
						Name: `"action1_Case4_Rule2"`,
						 Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name: `"action2_Case4_Rule2"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
		},
	}

	Actions_Rule2_Expected := []Action{
		DefaultAction{
			Name:       "action1_Case1_Rule2",
			Parameters: map[string]interface{}{"val": "myvalue"},
			MetaData:   map[string]interface{}{"caseName": "case1", "ruleID": "1", "ruleVersion": "1"},
		},
		DefaultAction{
			Name:       "action1_Case2_Rule2",
			Parameters: map[string]interface{}{"val": "myvalue"},
			MetaData:   map[string]interface{}{"caseName": "case2", "ruleID": "1", "ruleVersion": "1"},
		},
		DefaultAction{
			Name:       "action2_Case2_Rule2",
			Parameters: map[string]interface{}{"val2": "myvalue2"},
			MetaData:   map[string]interface{}{"caseName": "case2", "ruleID": "1", "ruleVersion": "1"},
		},
	}
	rules := []DefaultRule{rule1, rule2}
	
	for i, rule := range rules{
		//t.Log(rule)


		engine := NewRuleEngine()

		engine.InsertRule(&rule)
		engine.knowledgeBase.SetFacts(data)
		engine.ExecuteRules([]int64{1})

		actions := engine.GetResults()

		if i ==0 {
            if !CompareActionArrays(Actions_Rule1_Expected,actions) {
				t.Fail()
				t.Log("\n Expected : \n",Actions_Rule1_Expected, "\nBut find : \n ",actions)
			}
		}else{
			if !CompareActionArrays(Actions_Rule2_Expected,actions) {
				t.Fail()
				t.Log("\n Expected : \n",Actions_Rule2_Expected, "\nBut find : \n",actions)
			}
		}

		
	}
}
// compare two tab of actions 
func CompareActionArrays(actions1, actions2 []Action) bool {
	if len(actions1) != len(actions2) {
		return false
	}

	for i := range actions1 {
		if !CompareActions(actions1[i], actions2[i]) {
			return false
		}
	}

	return true
}

// compare two actions 
func CompareActions(action1, action2 Action) bool {
	return action1.GetName() == action2.GetName();
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

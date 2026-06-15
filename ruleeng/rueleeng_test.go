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

	if len(actions) != 3 {
		t.Errorf("Invalid number of actions returned %d", len(actions))
		t.FailNow()
	}

	if actions[0].GetName() != "set" || actions[0].GetParameters()["status.A"].(float64) != 3 ||
		actions[0].GetID() != "set-action-a" {
		t.Errorf("The engine actins are not as expected")
	}
	if actions[1].GetName() != "set" || actions[1].GetParameters()["status.B"].(float64) != 5 ||
		actions[1].GetID() != "set-action-b" {
		t.Errorf("The engine actins are not as expected")
	}
	if actions[2].GetName() != "notify" || actions[2].GetParameters()["id"].(string) != "notify-1" ||
		actions[2].GetParameters()["description"].(string) != "my_description" {
		t.Errorf("The engine actins are not as expected")
	}
}

func TestRuleActionDependency(t *testing.T) {

	var rule DefaultRule
	if err := json.Unmarshal([]byte(ruleDependencyStr), &rule); err != nil {
		t.Fatalf("could not unmarshal rule: %v", err)
	}

	if rule.ID != 1 {
		t.Errorf("invalid rule id, expected 1, got %d", rule.ID)
	}
	if len(rule.Cases) != 2 {
		t.Fatalf("invalid number of cases, expected 2, got %d", len(rule.Cases))
	}

	// First case: "set" action followed by a "create-issue" action depending on it
	case1 := rule.Cases[0]
	if len(case1.Actions) != 2 {
		t.Fatalf("invalid number of actions in case1, expected 2, got %d", len(case1.Actions))
	}

	setAction := case1.Actions[0]
	if setAction.Name != `"set"` {
		t.Errorf("invalid action name, expected \"set\", got %s", setAction.Name)
	}
	if setAction.ID != "718689fb-adb9-4de3-9b93-8dfddc82f9e1" {
		t.Errorf("invalid action id, got %s", setAction.ID)
	}
	if setAction.EnableActionCondition {
		t.Errorf("expected enableActionCondition to be false on the set action")
	}

	createIssueAction := case1.Actions[1]
	if createIssueAction.Name != `"create-issue"` {
		t.Errorf("invalid action name, expected \"create-issue\", got %s", createIssueAction.Name)
	}
	if !createIssueAction.EnableActionCondition {
		t.Errorf("expected enableActionCondition to be true on the create-issue action")
	}
	if createIssueAction.ActionCondition == nil {
		t.Fatalf("expected actionCondition to be set on the create-issue action")
	}

	for _, slot := range []*ActionConditionSlot{createIssueAction.ActionCondition.T, createIssueAction.ActionCondition.TMinus1} {
		if slot == nil {
			t.Fatalf("expected both t and t_minus_1 to be set on the create-issue action")
		}
		if !slot.Enabled {
			t.Errorf("expected slot to be enabled")
		}
		if slot.ActionSetID != setAction.ID {
			t.Errorf("invalid actionSetId, expected %s, got %s", setAction.ID, slot.ActionSetID)
		}
		if len(slot.Conditions) != 1 || slot.Conditions[0].Key != "statut" || slot.Conditions[0].Value != `"critical"` {
			t.Errorf("invalid conditions, got %+v", slot.Conditions)
		}
	}

	// Second case: "situation-reporting" action depending only on "t" (t_minus_1 disabled)
	case2 := rule.Cases[1]
	if len(case2.Actions) != 1 {
		t.Fatalf("invalid number of actions in case2, expected 1, got %d", len(case2.Actions))
	}

	situationReportingAction := case2.Actions[0]
	if !situationReportingAction.EnableActionCondition {
		t.Errorf("expected enableActionCondition to be true on the situation-reporting action")
	}
	if situationReportingAction.ActionCondition == nil {
		t.Fatalf("expected actionCondition to be set on the situation-reporting action")
	}
	if situationReportingAction.ActionCondition.T == nil || !situationReportingAction.ActionCondition.T.Enabled ||
		situationReportingAction.ActionCondition.T.ActionSetID != setAction.ID {
		t.Errorf("invalid t actionCondition, got %+v", situationReportingAction.ActionCondition.T)
	}
	if situationReportingAction.ActionCondition.TMinus1 == nil || situationReportingAction.ActionCondition.TMinus1.Enabled {
		t.Errorf("expected t_minus_1 to be disabled, got %+v", situationReportingAction.ActionCondition.TMinus1)
	}

	// Executing the rule should resolve the "set" action and expose its id in the resolved parameters
	engine := NewRuleEngine()
	engine.InsertRule(&rule)
	engine.knowledgeBase.SetFacts(map[string]interface{}{})
	engine.ExecuteAllRules()

	actions := engine.GetResults()
	if len(actions) == 0 {
		t.Fatalf("expected at least one resolved action")
	}

	if actions[0].GetName() != "set" ||
		actions[0].GetParameters()["statut"].(string) != "critical" ||
		actions[0].GetID() != "718689fb-adb9-4de3-9b93-8dfddc82f9e1" {
		t.Errorf("the resolved set action is not as expected: id=%s params=%+v", actions[0].GetID(), actions[0].GetParameters())
	}
	if actions[0].GetEnableActionCondition() {
		t.Errorf("expected enableActionCondition to be false on the resolved set action")
	}

	if len(actions) < 2 || actions[1].GetName() != "create-issue" {
		t.Fatalf("expected a resolved create-issue action, got %+v", actions)
	}
	if !actions[1].GetEnableActionCondition() {
		t.Errorf("expected enableActionCondition to be true on the resolved create-issue action")
	}
	resolvedCondition := actions[1].GetActionCondition()
	if resolvedCondition == nil {
		t.Fatalf("expected actionCondition to be set on the resolved create-issue action")
	}
	if resolvedCondition.T == nil || resolvedCondition.T.ActionSetID != actions[0].GetID() {
		t.Errorf("invalid resolved actionCondition.t: %+v", resolvedCondition.T)
	}
}

type ruleTestCase struct {
	name     string
	rule     DefaultRule
	expected []Action
}

var data = map[string]interface{}{
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

func TestRuleCaseEnable(t *testing.T) {

	rulesTests := []ruleTestCase{
		{
			name: "rule 1",
			rule: DefaultRule{
				ID:               1,
				Version:          1,
				Parameters:       make(map[string]interface{}),
				EvaluateAllCases: true,
				Cases: []Case{
					{
						Name:      "case1",
						Condition: "fact_test_1.aggs.doc_count.value > 25",
						Enabled:   false,
						Actions: []ActionDef{
							{
								Name:    `"action1_Case1_Rule1"`,
								Enabled: true,
								Parameters: map[string]Expression{
									"val": `"myvalue"`,
								},
							},
							{
								Name:    `"action2_Case1_Rule1"`,
								Enabled: true,
								Parameters: map[string]Expression{
									"val2": `"myvalue2"`,
								},
							},
						},
					},
					{
						Name:      "case2",
						Condition: "fact_test_1.aggs.doc_count.value > 25",
						Enabled:   true,
						Actions: []ActionDef{
							{
								Name:    `"action1_Case2_Rule1"`,
								Enabled: false,
								Parameters: map[string]Expression{
									"val": `"myvalue"`,
								},
							},
							{
								Name:    `"action2_Case2_Rule1"`,
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
						Enabled:   true,
						Actions: []ActionDef{
							{
								Name:    `"action1_Case3_Rule1"`,
								Enabled: true,
								Parameters: map[string]Expression{
									"val": `"myvalue"`,
								},
							},
							{
								Name:    `"action2_Case3_Rule1"`,
								Enabled: true,
								Parameters: map[string]Expression{
									"val2": `"myvalue2"`,
								},
							},
						},
					},
				},
			},
			expected: []Action{
				DefaultAction{
					Name:       "action1_Case3_Rule1",
					Parameters: map[string]interface{}{"val": "myvalue"},
					MetaData:   map[string]interface{}{"caseName": "case3", "ruleID": "1", "ruleVersion": "1"},
				},
				DefaultAction{
					Name:       "action2_Case3_Rule1",
					Parameters: map[string]interface{}{"val2": "myvalue2"},
					MetaData:   map[string]interface{}{"caseName": "case3", "ruleID": "1", "ruleVersion": "1"},
				},
			},
		},
	}

	for _, ruleTest := range rulesTests {

		engine := NewRuleEngine()

		engine.InsertRule(&ruleTest.rule)
		engine.knowledgeBase.SetFacts(data)
		engine.ExecuteRules([]int64{1})

		actions := engine.GetResults()

		if !compareActionArrays(ruleTest.expected, actions) {
			t.Error("\n Expected : \n", ruleTest.expected, "\nBut find : \n ", actions)
		}

	}

}

func TestRuleCaseActionEnable(t *testing.T) {

	rulesTests := []ruleTestCase{
		{
			name: "rule 1",
			rule: DefaultRule{
				ID:               1,
				Version:          1,
				Parameters:       make(map[string]interface{}),
				EvaluateAllCases: true,
				Cases: []Case{
					{
						Name:      "case1",
						Condition: "fact_test_1.aggs.doc_count.value > 25",
						Enabled:   true,
						Actions: []ActionDef{
							{
								Name:    `"action1_Case1_Rule1"`,
								Enabled: false,
								Parameters: map[string]Expression{
									"val": `"myvalue"`,
								},
							},
							{
								Name:    `"action2_Case1_Rule1"`,
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
						Enabled:   true,
						Actions: []ActionDef{
							{
								Name:    `"action1_Case2_Rule1"`,
								Enabled: true,
								Parameters: map[string]Expression{
									"val": `"myvalue"`,
								},
							},
							{
								Name:    `"action2_Case2_Rule1"`,
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
						Enabled:   true,
						Actions: []ActionDef{
							{
								Name:    `"action1_Case3_Rule1"`,
								Enabled: false,
								Parameters: map[string]Expression{
									"val": `"myvalue"`,
								},
							},
							{
								Name:    `"action2_Case3_Rule1"`,
								Enabled: true,
								Parameters: map[string]Expression{
									"val2": `"myvalue2"`,
								},
							},
						},
					},
				},
			},
			expected: []Action{
				DefaultAction{
					Name:       "action1_Case2_Rule1",
					Parameters: map[string]interface{}{"val": "myvalue"},
					MetaData:   map[string]interface{}{"caseName": "case2", "ruleID": "1", "ruleVersion": "1"},
				},
				DefaultAction{
					Name:       "action2_Case2_Rule1",
					Parameters: map[string]interface{}{"val2": "myvalue2"},
					MetaData:   map[string]interface{}{"caseName": "case2", "ruleID": "1", "ruleVersion": "1"},
				},
				DefaultAction{
					Name:       "action2_Case3_Rule1",
					Parameters: map[string]interface{}{"val2": "myvalue2"},
					MetaData:   map[string]interface{}{"caseName": "case3", "ruleID": "1", "ruleVersion": "1"},
				},
			},
		},
	}

	for _, ruleTest := range rulesTests {

		engine := NewRuleEngine()

		engine.InsertRule(&ruleTest.rule)
		engine.knowledgeBase.SetFacts(data)
		engine.ExecuteRules([]int64{1})

		actions := engine.GetResults()

		if !compareActionArrays(ruleTest.expected, actions) {

			t.Error("\n Expected : \n", ruleTest.expected, "\nBut find : \n ", actions)
		}

	}

}

func TestRuleEvaluateAllCase(t *testing.T) {
	rule1 := DefaultRule{
		ID:               1,
		Version:          1,
		Parameters:       make(map[string]interface{}),
		EvaluateAllCases: false,
		Cases: []Case{
			{
				Name:      "case1",
				Condition: "fact_test_1.aggs.doc_count.value > 25",
				Enabled:   false,
				Actions: []ActionDef{
					{
						Name:    `"action1_Case1_Rule1"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name:    `"action2_Case1_Rule1"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
			{
				Name:      "case2",
				Condition: "fact_test_1.aggs.doc_count.value > 25",
				Enabled:   true,
				Actions: []ActionDef{
					{
						Name:    `"action1_Case2_Rule1"`,
						Enabled: false,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name:    `"action2_Case2_Rule1"`,
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
				Enabled:   true,
				Actions: []ActionDef{
					{
						Name:    `"action1_Case3_Rule1"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name:    `"action2_Case3_Rule1"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
		},
	}

	rule2 := DefaultRule{
		ID:               1,
		Version:          1,
		Parameters:       make(map[string]interface{}),
		EvaluateAllCases: true,
		Cases: []Case{
			{
				Name:      "case1",
				Condition: "fact_test_1.aggs.doc_count.value > 25",
				Enabled:   true,
				Actions: []ActionDef{
					{
						Name:    `"action1_Case1_Rule2"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name:    `"action2_Case1_Rule2"`,
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
				Enabled:   true,
				Actions: []ActionDef{
					{
						Name:    `"action1_Case2_Rule2"`,
						Enabled: false,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name:    `"action2_Case2_Rule2"`,
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
				Enabled:   true,
				Actions: []ActionDef{
					{
						Name:    `"action1_Case3_Rule2"`,
						Enabled: false,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name:    `"action2_Case3_Rule2"`,
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
				Enabled:   true,
				Actions: []ActionDef{
					{
						Name:    `"action1_Case4_Rule2"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val": `"myvalue"`,
						},
					},
					{
						Name:    `"action2_Case4_Rule2"`,
						Enabled: true,
						Parameters: map[string]Expression{
							"val2": `"myvalue2"`,
						},
					},
				},
			},
		},
	}

	rulesTests := []ruleTestCase{
		{
			name:     "rule 1",
			rule:     rule1,
			expected: []Action{},
		},
		{
			name: "rule 2",
			rule: rule2,
			expected: []Action{
				DefaultAction{
					Name:       "action1_Case1_Rule2",
					Parameters: map[string]interface{}{"val": "myvalue"},
					MetaData:   map[string]interface{}{"caseName": "case1", "ruleID": "1", "ruleVersion": "1"},
				},
				DefaultAction{
					Name:       "action2_Case2_Rule2",
					Parameters: map[string]interface{}{"val2": "myvalue2"},
					MetaData:   map[string]interface{}{"caseName": "case2", "ruleID": "1", "ruleVersion": "1"},
				},
			},
		},
	}

	for _, ruleTest := range rulesTests {

		engine := NewRuleEngine()

		engine.InsertRule(&ruleTest.rule)
		engine.knowledgeBase.SetFacts(data)
		engine.ExecuteRules([]int64{1})

		actions := engine.GetResults()

		if !compareActionArrays(ruleTest.expected, actions) {

			t.Error("\n Expected : \n", ruleTest.expected, "\nBut find : \n ", actions)
		}

	}

}

// compare two tab of actions
func compareActionArrays(actions1, actions2 []Action) bool {
	if len(actions1) != len(actions2) {
		return false
	}

	for i := range actions1 {
		if !compareActions(actions1[i], actions2[i]) {
			return false
		}
	}

	return true
}

// compare two actions
func compareActions(action1, action2 Action) bool {
	return action1.GetName() == action2.GetName()
}

var ruleStr = `{
	"evaluateallcase": true,
	"cases": [
	  {
		"name": "case1",
		"enabled": true,
		"condition": "fact_test_1.aggs.agg0.value == fact_test_1.aggs.doc_count.value",
		"actions": [
		  {
			"name": "\"set\"",
			"enabled": true,
			"id": "set-action-a",
			"parameters": {
			  "status.A": "param1"
			}
		  },
		  {
			"name": "\"set\"",
			"enabled": true,
			"id": "set-action-b",
			"parameters": {
			  "status.B": "2 + param1"
			}
		  },
		  {
			"name": "\"notify\"",
			"enabled": true,
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

var ruleDependencyStr = `{
    "name": "missing_objects_rule",
    "description": "Surveillance des nombre de fichiers manquants.",
    "enabled": true,
    "calendarId": 0,
    "id": 1,
    "cases": [
        {
            "name": "[Critical] Fichiers manquants",
            "condition": "true",
            "actions": [
                {
                    "name": "\"set\"",
                    "parameters": {
                        "statut": "\"critical\""
                    },
                    "enabled": true,
                    "enabledDepends": false,
                    "enableActionCondition": false,
                    "id": "718689fb-adb9-4de3-9b93-8dfddc82f9e1"
                },
                {
                    "name": "\"create-issue\"",
                    "parameters": {
                        "id": "\"now\"",
                        "isNotification": "false",
                        "level": "\"critical\"",
                        "name": "\"now\"",
                        "timeout": "\"0m\""
                    },
                    "enabled": true,
                    "enabledDepends": false,
                    "enableActionCondition": true,
                    "actionCondition": {
                        "t": {
                            "enabled": true,
                            "actionSetId": "718689fb-adb9-4de3-9b93-8dfddc82f9e1",
                            "conditions": [
                                {
                                    "key": "statut",
                                    "value": "\"critical\""
                                }
                            ]
                        },
                        "t_minus_1": {
                            "enabled": true,
                            "actionSetId": "718689fb-adb9-4de3-9b93-8dfddc82f9e1",
                            "conditions": [
                                {
                                    "key": "statut",
                                    "value": "\"critical\""
                                }
                            ]
                        }
                    }
                }
            ],
            "enabled": true,
            "enableDependsForALLAction": false
        },
        {
            "name": "send mail",
            "condition": "true",
            "actions": [
                {
                    "name": "\"situation-reporting\"",
                    "parameters": {
                        "attachmentFactIds": "",
                        "attachmentFileNames": "",
                        "bodyTemplate": "\"ismail\"",
                        "cc": "",
                        "columns": "",
                        "columnsLabel": "",
                        "formateColumns": "",
                        "id": "\"ismail\"",
                        "issueId": "",
                        "separator": "",
                        "subject": "\"ismail\"",
                        "timeout": "\"0m\"",
                        "to": "\"liamsi2019@gmail.com\""
                    },
                    "enabled": true,
                    "enabledDepends": false,
                    "enableActionCondition": true,
                    "actionCondition": {
                        "t": {
                            "enabled": true,
                            "actionSetId": "718689fb-adb9-4de3-9b93-8dfddc82f9e1",
                            "conditions": [
                                {
                                    "key": "statut",
                                    "value": "\"critical\""
                                }
                            ]
                        },
                        "t_minus_1": {
                            "enabled": false,
                            "actionSetId": "",
                            "conditions": []
                        }
                    }
                }
            ],
            "enabled": false,
            "enableDependsForALLAction": false
        }
    ],
    "version": 17,
    "parameters": {},
    "evaluateallcase": true
}`

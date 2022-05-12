package ruleeng

import (
	"errors"
	"strconv"

	"go.uber.org/zap"
)

// DefaultRuleBase default rule base implementation
type DefaultRuleBase struct {
	rules map[int64]Rule
}

// NewRBase creates a new rulesBase
func NewRBase() RuleBase {
	return &DefaultRuleBase{rules: make(map[int64]Rule, 0)}
}

// GetRules returns the rules
func (rBase *DefaultRuleBase) GetRules() map[int64]Rule {
	return rBase.rules
}

// InsertRule allows to inser a Rule in the rulesBase
func (rBase *DefaultRuleBase) InsertRule(rule Rule) {
	rBase.rules[rule.GetID()] = rule
}

// InsertRules allows to inser a liste of Rules in the rulesBase
func (rBase *DefaultRuleBase) InsertRules(rules []Rule) {
	for _, rule := range rules {
		rBase.rules[rule.GetID()] = rule
	}
}

// RemoveRule allows to remove a Rule in the rulesBase
func (rBase *DefaultRuleBase) RemoveRule(id int64) {
	if _, ok := rBase.rules[id]; ok {
		delete(rBase.rules, id)
	}
}

// Reset removes the rules, tasks and errors of the rulesBase
func (rBase *DefaultRuleBase) Reset() {
	rBase.rules = make(map[int64]Rule, 0)
}

// ExecuteAll executes all the rules of the ruleBase using the knowledgeBase provided as parameter
func (rBase *DefaultRuleBase) ExecuteAll(k KnowledgeBase) []Action {
	results := make([]Action, 0)
	for _, rule := range rBase.rules {
		actions := rBase.executeRule(rule, k)
		if actions != nil {
			results = append(results, actions...)
		}
	}
	return results
}

// ExecuteRules executes a list of the rules of the ruleBase using the knowledgeBase provided as parameter
func (rBase *DefaultRuleBase) ExecuteRules(ruleIDs []int64, k KnowledgeBase) []Action {
	results := make([]Action, 0)
	for _, ruleID := range ruleIDs {
		actions, err := rBase.ExecuteByID(ruleID, k)
		if err != nil {
			zap.L().Warn("Trying to execute non existing rule:", zap.Int64("ruleID", ruleID))
		} else {
			results = append(results, actions...)
		}
	}
	return results
}

// ExecuteByID executes the rule with the name provide in the parameter 'ruleName' using the knowledgeBase provided as parameter
func (rBase *DefaultRuleBase) ExecuteByID(ruleID int64, k KnowledgeBase) ([]Action, error) {
	if rule, ok := rBase.rules[ruleID]; ok {
		return rBase.executeRule(rule, k), nil
	}
	return nil, errors.New(strconv.FormatInt(ruleID, 10) + " does not exists")
}

func (rBase *DefaultRuleBase) executeRule(rule Rule, k KnowledgeBase) []Action {
	return rule.Execute(k)
}

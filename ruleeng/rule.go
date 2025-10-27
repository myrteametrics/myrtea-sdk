package ruleeng

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/myrteametrics/myrtea-sdk/v5/expression"
)

// Rule ...
// See DefaultRule for an example implementation.
type Rule interface {
	GetID() int64
	GetDefaultValues() map[string]interface{}
	Execute(k KnowledgeBase) []Action
	IsValid() (bool, error)
}

// DefaultRule default rule implementation
type DefaultRule struct {
	ID               int64                  `json:"id,omitempty"`
	Cases            []Case                 `json:"cases"`
	Version          int64                  `json:"version"`
	Parameters       map[string]interface{} `json:"parameters"`
	EvaluateAllCases bool                   `json:"evaluateallcase"`
}

// GetID returns the rule id
func (r DefaultRule) GetID() int64 {
	return r.ID
}

// GetDefaultValues returns rule default values
func (r DefaultRule) GetDefaultValues() map[string]interface{} {
	return r.Parameters
}

// IsValid validates the rule structure and returns validation status
func (r DefaultRule) IsValid() (bool, error) {
	if r.Cases == nil {
		return false, errors.New("missing rule cases")
	}
	if len(r.Cases) <= 0 {
		return false, errors.New("missing rule cases")
	}

	// Validate each case
	for i, c := range r.Cases {
		if c.Name == "" {
			return false, fmt.Errorf("missing case name at index %d", i)
		}
		if c.Condition == "" {
			return false, fmt.Errorf("missing case condition for case: %s", c.Name)
		}

		// Validate condition expression syntax
		if err := ValidateExpressionSyntax(string(c.Condition)); err != nil {
			return false, fmt.Errorf("invalid condition syntax in case '%s': %w", c.Name, err)
		}
		if c.Actions == nil {
			return false, fmt.Errorf("missing case actions for case: %s", c.Name)
		}
		if len(c.Actions) <= 0 {
			return false, fmt.Errorf("missing case actions for case: %s", c.Name)
		}

		// Validate each action in the case
		for j, a := range c.Actions {
			if a.Name == "" {
				return false, fmt.Errorf("missing action name at index %d in case: %s", j, c.Name)
			}

			// Validate action name expression syntax
			if err := ValidateExpressionSyntax(string(a.Name)); err != nil {
				return false, fmt.Errorf("invalid action name syntax at index %d in case '%s': %w", j, c.Name, err)
			}

			// Validate action parameters expression syntax
			for paramName, paramExpr := range a.Parameters {
				if paramExpr == "" {
					continue
				}

				if err := ValidateExpressionSyntax(string(paramExpr)); err != nil {
					return false, fmt.Errorf("invalid parameter '%s' syntax in action at index %d in case '%s': %w", paramName, j, c.Name, err)
				}
			}
		}
	}

	return true, nil
}

// Execute executes the rule and return the resulting actions
func (r DefaultRule) Execute(k KnowledgeBase) []Action {
	result := make([]Action, 0)

	k.SetDefaultValues(r.Parameters)

	for _, c := range r.Cases {

		if !c.Enabled {
			continue
		}
		actions := c.evaluate(k)
		if actions != nil {
			for _, a := range actions {

				a.MetaData["ruleID"] = r.ID
				a.MetaData["ruleVersion"] = r.Version
				result = append(result, a)
			}
			if !r.EvaluateAllCases {
				return result
			}

		}
	}

	if len(result) > 0 {
		return result
	}
	return nil
}

// UnmarshalJSON unmashals a quoted json string to Expression
func (r *DefaultRule) UnmarshalJSON(data []byte) error {
	type Alias DefaultRule
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(r),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if r.Cases == nil {
		return errors.New("missing rule cases")
	}
	if len(r.Cases) <= 0 {
		return errors.New("missing rule cases")
	}

	return nil
}

// Case : pair condition tasks use to compose a Rule
type Case struct {
	Name                      string      `json:"name"`
	Condition                 Expression  `json:"condition"`
	Actions                   []ActionDef `json:"actions"`
	Enabled                   bool        `json:"enabled"`
	EnableDependsForALLAction bool        `json:"enableDependsForALLAction"`
	CheckPrevSetAction        bool        `json:"checkPrevSetAction"` // CheckPrevSet indicates whether to verify the previously set action parameters in the rule evaluation process.
}

func (c Case) evaluate(k KnowledgeBase) []DefaultAction {

	val, _ := c.Condition.EvaluateAsBool(k)
	if val {
		return resolve(c, k)
	}
	return nil
}

// resolve creates a lis86t of actions from the case actions Definitions
func resolve(c Case, k KnowledgeBase) []DefaultAction {
	resolvedActions := make([]DefaultAction, 0)

	for _, a := range c.Actions {

		if !a.Enabled {
			continue
		}
		rAction, err := a.Resolve(k, c)
		if err == nil {
			rAction.MetaData["caseName"] = c.Name
			resolvedActions = append(resolvedActions, rAction)
		}
	}
	return resolvedActions
}

// UnmarshalJSON unmashals a quoted json string to Expression
func (c *Case) UnmarshalJSON(data []byte) error {
	type Alias Case
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if c.Name == "" {
		return errors.New("Missing case name")
	}
	if c.Condition == "" {
		return errors.New("Missing case condition")
	}
	if c.Actions == nil {
		return errors.New("Missing case actions")
	}
	if len(c.Actions) <= 0 {
		return errors.New("Missing case actions")
	}

	return nil
}

// ActionDef action definition
type ActionDef struct {
	Name           Expression            `json:"name"`
	Parameters     map[string]Expression `json:"parameters"`
	Enabled        bool                  `json:"enabled"`
	EnabledDepends bool                  `json:"enabledDepends"`
}

// Resolve resolves the ActionDef into a DefaultAction
func (a ActionDef) Resolve(k KnowledgeBase, c Case) (DefaultAction, error) {

	name, err := a.Name.EvaluateAsString(k)

	if err != nil {
		return DefaultAction{}, err
	}

	rAction := DefaultAction{
		Name:                      name,
		Parameters:                make(map[string]interface{}),
		MetaData:                  make(map[string]interface{}),
		EnabledDependsAction:      a.EnabledDepends,
		EnableDependsForALLAction: c.EnableDependsForALLAction,
		CheckPrevSetAction:        c.CheckPrevSetAction,
	}

	for key, exp := range a.Parameters {
		value, err := exp.Evaluate(k)
		if err == nil {
			rAction.Parameters[key] = value
		}
	}

	if name == "set" {
		for key, value := range rAction.Parameters {
			k.InsertFact(key, value)
		}
	}

	return rAction, nil
}

// UnmarshalJSON unmashals a quoted json string to Expression
func (a *ActionDef) UnmarshalJSON(data []byte) error {
	type Alias ActionDef
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(a),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if a.Name == "" {
		return errors.New("Missing action name")
	}

	return nil
}

// ValidateExpressionSyntax validates the syntax of the expression
func ValidateExpressionSyntax(expr string) error {
	_, err := expression.LangEval.NewEvaluable(expr)
	if err != nil {
		return fmt.Errorf("invalid expression syntax: %w", err)
	}
	return nil
}

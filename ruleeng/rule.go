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

			if a.EnableActionCondition {
				if err := a.ActionCondition.Validate(); err != nil {
					return false, fmt.Errorf("invalid action condition at index %d in case '%s': %w", j, c.Name, err)
				}
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
	EnableDependsForAllAction bool        `json:"enableDependsForALLAction"`
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
	Name                  Expression            `json:"name"`
	Parameters            map[string]Expression `json:"parameters"`
	Enabled               bool                  `json:"enabled"`
	EnabledDepends        bool                  `json:"enabledDepends"`
	EnableActionCondition bool                  `json:"enableActionCondition"`
	ID                    string                `json:"id,omitempty"`
	ActionCondition       *ActionCondition      `json:"actionCondition,omitempty"`
}

// ActionConditionParameter is a snapshot of a "set" action parameter (key/value)
// captured at the time a dependent action was configured to depend on it.
type ActionConditionParameter struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// MaxActionConditionOffset is the deepest history offset an action condition can look back to.
const MaxActionConditionOffset = 10

// ActionConditionSlot describes a dependency on a "set" action, identified by
// its stable ID, along with a snapshot of the conditions (parameters) it was
// configured with. Offset is the number of ticks to look back: 0 is the current
// tick (T), 1 the previous tick (T-1), 2 the one before (T-2), and so on.
type ActionConditionSlot struct {
	Offset      int                        `json:"offset"`
	Enabled     bool                       `json:"enabled"`
	ActionSetID string                     `json:"actionSetId"`
	Conditions  []ActionConditionParameter `json:"conditions"`
}

// ActionCondition holds the dependency configuration of an action on "set"
// actions evaluated at any number of ticks (T, T-1, T-2, ...), one slot per offset.
// All the enabled slots must be verified (AND).
type ActionCondition struct {
	Slots []ActionConditionSlot `json:"slots"`
}

// legacyActionCondition is the former fixed-slot format, still read for the rules already stored.
type legacyActionCondition struct {
	T       *ActionConditionSlot `json:"t"`
	TMinus1 *ActionConditionSlot `json:"t_minus_1"`
}

// UnmarshalJSON reads the current format ({"slots": [...]}) as well as the former one
// ({"t": {...}, "t_minus_1": {...}}), which is converted to offsets 0 and 1.
func (c *ActionCondition) UnmarshalJSON(data []byte) error {
	var current struct {
		Slots []ActionConditionSlot `json:"slots"`
	}
	if err := json.Unmarshal(data, &current); err != nil {
		return err
	}
	var legacy legacyActionCondition
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}

	c.Slots = current.Slots
	if len(c.Slots) > 0 {
		return nil
	}
	if legacy.T != nil {
		slot := *legacy.T
		slot.Offset = 0
		c.Slots = append(c.Slots, slot)
	}
	if legacy.TMinus1 != nil {
		slot := *legacy.TMinus1
		slot.Offset = 1
		c.Slots = append(c.Slots, slot)
	}
	return nil
}

// Slot returns the slot configured for the given offset, or nil if there is none.
func (c *ActionCondition) Slot(offset int) *ActionConditionSlot {
	if c == nil {
		return nil
	}
	for i := range c.Slots {
		if c.Slots[i].Offset == offset {
			return &c.Slots[i]
		}
	}
	return nil
}

// MaxEnabledOffset returns the deepest offset among the enabled slots, or 0 if there is none.
func (c *ActionCondition) MaxEnabledOffset() int {
	if c == nil {
		return 0
	}
	max := 0
	for _, slot := range c.Slots {
		if slot.Enabled && slot.Offset > max {
			max = slot.Offset
		}
	}
	return max
}

// Validate checks that the offsets are within bounds and unique.
func (c *ActionCondition) Validate() error {
	if c == nil {
		return nil
	}
	seen := make(map[int]struct{}, len(c.Slots))
	for _, slot := range c.Slots {
		if slot.Offset < 0 || slot.Offset > MaxActionConditionOffset {
			return fmt.Errorf("invalid action condition offset %d (must be between 0 and %d)", slot.Offset, MaxActionConditionOffset)
		}
		if _, dup := seen[slot.Offset]; dup {
			return fmt.Errorf("duplicate action condition offset %d", slot.Offset)
		}
		seen[slot.Offset] = struct{}{}
		if slot.Enabled && slot.ActionSetID == "" {
			return fmt.Errorf("missing actionSetId for the action condition at offset %d", slot.Offset)
		}
	}
	return nil
}

// Resolve resolves the ActionDef into a DefaultAction
func (a ActionDef) Resolve(k KnowledgeBase, c Case) (DefaultAction, error) {

	name, err := a.Name.EvaluateAsString(k)

	if err != nil {
		return DefaultAction{}, err
	}

	rAction := DefaultAction{
		ID:                        a.ID,
		Name:                      name,
		Parameters:                make(map[string]interface{}),
		MetaData:                  make(map[string]interface{}),
		EnabledDependsAction:      a.EnabledDepends,
		EnableDependsForAllAction: c.EnableDependsForAllAction,
		EnableActionCondition:     a.EnableActionCondition,
		ActionCondition:           a.ActionCondition,
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

package ruleeng

// Action ...
// See DefaultAction for an example implementation.
type Action interface {
	GetID() string
	GetName() string
	GetParameters() map[string]interface{}
	GetMetaData() map[string]interface{}
	GetEnabledDependsAction() bool
	GetEnableDependsForAllAction() bool
	GetEnableActionCondition() bool
	GetActionCondition() *ActionCondition
}

// DefaultAction default action implementation
type DefaultAction struct {
	ID                        string                 `json:"id,omitempty"`
	Name                      string                 `json:"name"`
	Parameters                map[string]interface{} `json:"parameters"`
	MetaData                  map[string]interface{} `json:"metaData"`
	EnabledDependsAction      bool                   `json:"enabledDepends"`
	EnableDependsForAllAction bool                   `json:"enableDependsForALLAction"`
	EnableActionCondition     bool                   `json:"enableActionCondition"`
	ActionCondition           *ActionCondition       `json:"actionCondition,omitempty"`
}

// GetID returns the action id (the id of the action set it originates from)
func (a DefaultAction) GetID() string {
	return a.ID
}

// GetEnableActionCondition returns whether this action depends on a "set" action
func (a DefaultAction) GetEnableActionCondition() bool {
	return a.EnableActionCondition
}

// GetActionCondition returns the action dependency conditions, or nil if none is configured
func (a DefaultAction) GetActionCondition() *ActionCondition {
	return a.ActionCondition
}

// GetParameters returns the action parameters
func (a DefaultAction) GetParameters() map[string]interface{} {
	return a.Parameters
}

// GetMetaData returns the action metadata
func (a DefaultAction) GetMetaData() map[string]interface{} {
	return a.MetaData
}

// GetName returns the action name
func (a DefaultAction) GetName() string {
	return a.Name
}

// GetDisableDepends return if the action agrees dependency management
func (a DefaultAction) GetEnabledDependsAction() bool {
	return a.EnabledDependsAction
}

// GetEnableDependsForAllAction return if the case supports dependency management
func (a DefaultAction) GetEnableDependsForAllAction() bool {
	return a.EnableDependsForAllAction
}

package ruleeng

// Action ...
// See DefaultAction for an example implementation.
type Action interface {
	GetName() string
	GetParameters() map[string]interface{}
	GetMetaData() map[string]interface{}
	GetDisableDepends() bool
	GetEnableDependsForALLAction() bool
}

// DefaultAction default action implementation
type DefaultAction struct {
	Name                      string                 `json:"name"`
	Parameters                map[string]interface{} `json:"parameters"`
	MetaData                  map[string]interface{} `json:"metaData"`
	DisableDepends            bool                   `json:"disableDepends"`
	EnableDependsForALLAction bool                   `json:"enableDependsForALLAction"`
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
func (a DefaultAction) GetDisableDepends() bool {
	return a.DisableDepends
}

// GetEnableDependsForALLAction return if the case supports dependency management
func (a DefaultAction) GetEnableDependsForALLAction() bool {
	return a.EnableDependsForALLAction
}

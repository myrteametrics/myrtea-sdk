package connector

// Config wraps all rules for document merging
type Config struct {
	Mode             Mode    `json:"mode"`
	ExistingAsMaster bool    `json:"existingAsMaster"`
	Type             string  `json:"type,omitempty"`
	LinkKey          string  `json:"linkKey,omitempty"`
	Groups           []Group `json:"groups,omitempty"`
}

// Group allows to group un set of merge fields and to define an optional condition to applay the merge fields
type Group struct {
	Condition             string      `json:"condition,omitempty"`
	FieldReplace          []string    `json:"fieldReplace,omitempty"`
	FieldReplaceIfMissing []string    `json:"fieldReplaceIfMissing,omitempty"`
	FieldMerge            []string    `json:"fieldMerge,omitempty"`
	FieldMath             []FieldMath `json:"fieldMath,omitempty"`
	FieldKeepLatest       []string    `json:"fieldKeepLatest,omitempty"`
	FieldKeepEarliest     []string    `json:"fieldKeepEarliest,omitempty"`
	FieldForceUpdate      []string    `json:"fieldForceUpdate,omitempty"`
}

// FieldMath specify a merge rule using a math expression
type FieldMath struct {
	Expression  string `json:"expression"`
	OutputField string `json:"outputField"`
}

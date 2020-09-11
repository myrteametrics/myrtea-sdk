package ruleeng

// RuleEngine represents an instance of a rule engine
type RuleEngine struct {
	knowledgeBase KnowledgeBase
	ruleBase      RuleBase
	agenda        []Action
}

//KnowledgeBase ...
type KnowledgeBase interface {
	GetFacts() map[string]interface{}
	InsertFact(key string, value interface{})
	SetFacts(facts map[string]interface{})
	SetDefaultValues(parameters map[string]interface{})
	GetFact(key string) interface{}
	Reset()
}

//RuleBase ...
type RuleBase interface {
	GetRules() map[int64]Rule
	InsertRule(r Rule)
	InsertRules(rules []Rule)
	RemoveRule(id int64)
	Reset()
	ExecuteAll(k KnowledgeBase) []Action
	ExecuteRules(ids []int64, k KnowledgeBase) []Action
	ExecuteByID(id int64, k KnowledgeBase) ([]Action, error)
}

// NewRuleEngine builds a RuleEngine
func NewRuleEngine() *RuleEngine {
	return &RuleEngine{
		knowledgeBase: NewKBase(),
		ruleBase:      NewRBase(),
		agenda:        make([]Action, 0),
	}
}

// SetRules overwrites the ruleBase in the  RuleEngine
func (engine *RuleEngine) SetRules(r RuleBase) {
	engine.ruleBase = r
}

// SetKnowledge overwrites the knowledgeBase in the  RuleEngine
func (engine *RuleEngine) SetKnowledge(k KnowledgeBase) {
	engine.knowledgeBase = k
}

// GetKnowledgeBase returns the knowledgeBase in the  RuleEngine
func (engine *RuleEngine) GetKnowledgeBase() KnowledgeBase {
	return engine.knowledgeBase
}

// GetRulesBase returns the RulesBase in the  RuleEngine
func (engine *RuleEngine) GetRulesBase() RuleBase {
	return engine.ruleBase
}

// Reset remove all the results added in previous rules execution
func (engine *RuleEngine) Reset() {
	engine.agenda = []Action{}
	engine.knowledgeBase.Reset()
}

// InsertRule inserts a rule in the ruleBase of the RuleEngine
func (engine *RuleEngine) InsertRule(r Rule) {
	engine.ruleBase.InsertRule(r)
}

// InsertRules inserts a slice of rules in the ruleBase of the RuleEngine
func (engine *RuleEngine) InsertRules(rules []Rule) {
	engine.ruleBase.InsertRules(rules)
}

// RemoveRule remocves a rule in the ruleBase of the RuleEngine
func (engine *RuleEngine) RemoveRule(id int64) {
	engine.ruleBase.RemoveRule(id)
}

// InsertKnowledge inserts a key value (fact) in the knowledgeBase of the RuleEngine
func (engine *RuleEngine) InsertKnowledge(key string, value interface{}) {
	engine.knowledgeBase.InsertFact(key, value)
}

// ExecuteAllRules executes all the rules in the baseRules using the knowledgeBase
func (engine *RuleEngine) ExecuteAllRules() {
	engine.agenda = engine.ruleBase.ExecuteAll(engine.knowledgeBase)
}

// ExecuteRules executes all a list of rules in the baseRules using the knowledgeBase
func (engine *RuleEngine) ExecuteRules(ids []int64) {
	engine.agenda = engine.ruleBase.ExecuteRules(ids, engine.knowledgeBase)
}

// ExecuteRule executes a single rule by id using the knowledgeBase
func (engine *RuleEngine) ExecuteRule(id int64) error {
	actions, err := engine.ruleBase.ExecuteByID(id, engine.knowledgeBase)
	if err != nil {
		engine.agenda = append(engine.agenda, actions...)
		return nil
	}
	return err
}

// GetResults returns the Results of the rules executed
func (engine *RuleEngine) GetResults() []Action {
	return engine.agenda
}

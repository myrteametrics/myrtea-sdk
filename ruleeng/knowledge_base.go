package ruleeng

import (
	"fmt"
	"strings"
)

// DefaultKnowledgeBase default knowledge base implementation
type DefaultKnowledgeBase struct {
	facts       map[string]interface{}
	defaultKeys []string
	indexs      map[string]interface{}
}

// NewKBase builds a KBase
func NewKBase() KnowledgeBase {
	return &DefaultKnowledgeBase{
		facts:       make(map[string]interface{}),
		defaultKeys: make([]string, 0),
		indexs:      make(map[string]interface{}),
	}
}

// GetFacts returns the facts maps of the KBase
func (kBase *DefaultKnowledgeBase) GetFacts() map[string]interface{} {
	return kBase.facts
}

// InsertFact inserts a key value (fact) in the facts map
func (kBase *DefaultKnowledgeBase) InsertFact(key string, value interface{}) {
	kBase.facts[key] = value
}

// SetFacts overwrides the facts in the KBase
func (kBase *DefaultKnowledgeBase) SetFacts(facts map[string]interface{}) {
	kBase.facts = facts
}

// SetDefaultValues add defaults values at the facts in the KBase
func (kBase *DefaultKnowledgeBase) SetDefaultValues(parameters map[string]interface{}) {

	for _, key := range kBase.defaultKeys {
		delete(kBase.facts, key)
	}
	kBase.defaultKeys = make([]string, 0)

	for key, value := range parameters {
		if _, ok := kBase.facts[key]; !ok {
			kBase.defaultKeys = append(kBase.defaultKeys, key)
			kBase.facts[key] = value
		}
	}
}

// GetFact returns the value of a key in the facts map
func (kBase *DefaultKnowledgeBase) GetFact(key string) interface{} {

	if val, ok := kBase.indexs[key]; ok {
		return val
	}

	keys := strings.Split(key, ".")
	var value interface{}
	if val, ok := kBase.facts[keys[0]]; ok {
		value = val
	} else {
		return key
	}

	keys = keys[1:]
	for _, _key := range keys {
		switch v := value.(type) {
		case map[string]interface{}:
			if val, ok := v[_key]; ok {
				value = val
			} else {
				return key
			}
		case map[interface{}]interface{}:
			if val, ok := v[_key]; ok {
				value = val
			} else {
				return key
			}
		default:
			return key

		}
	}

	kBase.indexs[key] = value
	return value
}

// Reset removes all de facts and indexs in the KBase
func (kBase *DefaultKnowledgeBase) Reset() {
	kBase.facts = make(map[string]interface{})
	kBase.defaultKeys = make([]string, 0)
	kBase.indexs = make(map[string]interface{})
}

func (kBase *DefaultKnowledgeBase) String() string {
	return fmt.Sprint(kBase.facts)
}

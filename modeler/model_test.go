package modeler

import (
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/myrteametrics/myrtea-sdk/v5/expression"
	"strings"
	"testing"
)

var model = Model{
	ID:   1,
	Name: "model-1",
	Fields: []Field{
		&FieldLeaf{Name: "f1", Ftype: Int, Synonyms: []string{"f1", "f1other"}},
		&FieldLeaf{Name: "f2", Ftype: String, Synonyms: []string{"f2", "f2other"}},
		&FieldLeaf{Name: "f3", Ftype: DateTime, Synonyms: []string{"f3", "f3other"}},
		&FieldLeaf{Name: "f4", Ftype: Boolean, Synonyms: []string{"f4", "f4other"}},
		&FieldObject{Name: "f5", Ftype: Object, KeepObjectSeparation: false, Fields: []Field{
			&FieldLeaf{Name: "a", Ftype: Int, Synonyms: []string{"a", "aother"}},
			&FieldLeaf{Name: "b", Ftype: String, Synonyms: []string{"b", "bother"}},
		}},
		&FieldObject{Name: "f6", Ftype: Object, KeepObjectSeparation: true, Fields: []Field{
			&FieldLeaf{Name: "a", Ftype: Int, Synonyms: []string{"a", "aother"}},
			&FieldLeaf{Name: "b", Ftype: String, Synonyms: []string{"b", "bother"}},
		}},
	},
	Synonyms: []string{"model", "other"},
	ElasticsearchOptions: ElasticsearchOptions{
		Rollmode:                  RollmodeSettings{Type: RollmodeCron},
		Rollcron:                  "0 0 * * *",
		EnablePurge:               true,
		PurgeMaxConcurrentIndices: 30,
		PatchAliasMaxIndices:      2,
		AdvancedSettings: types.IndexSettings{
			NumberOfShards:   "1",
			NumberOfReplicas: "0",
		},
	},
}

var expectedModel = strings.ReplaceAll(`{"id":1,"name":"model-1","synonyms":["model","other"],"fields":[{"name":"f1","type":"int","semantic":false,"synonyms":["f1","f1other"]},
{"name":"f2","type":"string","semantic":false,"synonyms":["f2","f2other"]},{"name":"f3","type":"datetime","semantic":false,"synonyms":["f3","f3other"]},
{"name":"f4","type":"boolean","semantic":false,"synonyms":["f4","f4other"]},{"name":"f5","type":"object","keepObjectSeparation":false,
"fields":[{"name":"a","type":"int","semantic":false,"synonyms":["a","aother"]},{"name":"b","type":"string","semantic":false,"synonyms":["b","bother"]}]},
{"name":"f6","type":"object","keepObjectSeparation":true,"fields":[{"name":"a","type":"int","semantic":false,"synonyms":["a","aother"]},
{"name":"b","type":"string","semantic":false,"synonyms":["b","bother"]}]}],"elasticsearchOptions":{"rollmode":{"type":"cron"},"rollcron":"0 0 * * *",
"enablePurge":true,"purgeMaxConcurrentIndices":30,"patchAliasMaxIndices":2,"advancedSettings":{"number_of_replicas":"0","number_of_shards":"1"}}}`, "\n", "")

func TestMarshal(t *testing.T) {
	b, err := json.Marshal(model)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if string(b) != expectedModel {
		t.Error("invalid marshal result")
		t.Log(string(b))
		t.Log(strings.TrimSpace(expectedModel))
		t.FailNow()
	}
	var newModel Model
	err = json.Unmarshal(b, &newModel)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	b2, _ := json.Marshal(model)
	newB, _ := json.Marshal(newModel)
	if string(newB) != string(b2) {
		t.Error(err)
		t.Log(string(newB))
		t.Log(string(b))
	}
}

func TestSource(t *testing.T) {

	for _, field := range model.Fields {
		_, content := field.Source()
		if len(content) == 0 {
			t.Error("expected content")
		}

		var ok bool
		var name string
		var fieldType FieldType

		switch field.(type) {
		case *FieldObject:
			obj := field.(*FieldObject)
			name, ok = content["type"].(string)
			fieldType = obj.Ftype
			break
		case *FieldLeaf:
			leaf := field.(*FieldLeaf)
			name, ok = content["type"].(string)
			fieldType = leaf.Ftype
			break
		default:
			t.Error("expected *FieldLeaf or *FieldObject")
			break
		}

		if !ok && fieldType == Object {
			continue
		} else if !ok {
			t.Error("expected type in component map")
		}

		switch fieldType {
		case Int:
			if name != "integer" {
				t.Error("expected integer")
			}
			break
		case String:
			if name != "keyword" {
				t.Error("expected keyword")
			}
			break
		case Float:
			if name != "float" {
				t.Error("expected float")
			}
			break
		case DateTime:
			if name != "date" {
				t.Error("expected date")
			}
			break
		case Boolean:
			if name != "boolean" {
				t.Error("expected boolean")
			}
			break
		}

	}

}

func TestElasticsearchOptions_IsValid(t *testing.T) {
	opt := ElasticsearchOptions{
		Rollmode: RollmodeSettings{
			Type: RollmodeCron,
		},
		Rollcron: "0 0 * * *",
	}

	valid, _ := opt.IsValid()

	expression.AssertEqual(t, valid, true, "Rollcron is valid")

	opt.Rollcron = "2 * * * *"
	valid, _ = opt.IsValid()

	expression.AssertEqual(t, valid, false, "Rollcron should be invalid, since its interval is less than every day")
}

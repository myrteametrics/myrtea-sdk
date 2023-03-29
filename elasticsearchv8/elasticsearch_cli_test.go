package elasticsearchv8

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	elasticsearchv8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/mget"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
	"github.com/myrteametrics/myrtea-sdk/v4/modeler"
)

var model = modeler.Model{
	ID:   1,
	Name: "mymodel",
	Fields: []modeler.Field{
		&modeler.FieldLeaf{Name: "f1", Ftype: modeler.Int, Synonyms: []string{"f1", "f1other"}},
		&modeler.FieldLeaf{Name: "f2", Ftype: modeler.String, Synonyms: []string{"f2", "f2other"}},
		&modeler.FieldLeaf{Name: "f3", Ftype: modeler.DateTime, Synonyms: []string{"f3", "f3other"}},
		&modeler.FieldLeaf{Name: "f4", Ftype: modeler.Boolean, Synonyms: []string{"f4", "f4other"}},
		&modeler.FieldObject{Name: "f5", Ftype: modeler.Object, KeepObjectSeparation: false, Fields: []modeler.Field{
			&modeler.FieldLeaf{Name: "a", Ftype: modeler.Int, Synonyms: []string{"a", "aother"}},
			&modeler.FieldLeaf{Name: "b", Ftype: modeler.String, Synonyms: []string{"b", "bother"}},
		}},
		&modeler.FieldObject{Name: "f6", Ftype: modeler.Object, KeepObjectSeparation: true, Fields: []modeler.Field{
			&modeler.FieldLeaf{Name: "a", Ftype: modeler.Int, Synonyms: []string{"a", "aother"}},
			&modeler.FieldLeaf{Name: "b", Ftype: modeler.String, Synonyms: []string{"b", "bother"}},
		}},
	},
	Synonyms: []string{"model", "other"},
	ElasticsearchOptions: modeler.ElasticsearchOptions{
		Rollmode:                  "cron",
		Rollcron:                  "0 0 * * *",
		EnablePurge:               true,
		PurgeMaxConcurrentIndices: 30,
		PatchAliasMaxIndices:      2,
		AdvancedSettings: modeler.ElasticsearchAdvancedSettings{
			"number_of_shards":   1,
			"number_of_replicas": 0,
		},
	},
}

// Init client
var cfgv8 = elasticsearchv8.Config{
	Addresses: []string{
		"http://localhost:9200",
	},

	RetryOnStatus: []int{502, 503, 504},
	// EnableRetryOnTimeout: true,
	MaxRetries: math.MaxInt,
	// RetryBackoff: func(attempt int) time.Duration {},

	Transport: &http.Transport{
		MaxIdleConnsPerHost:   10,
		ResponseHeaderTimeout: time.Second,
		// ...
	},
	// ...
}

func TestESv8(t *testing.T) {

	t.Fail()
	t.Run("ExistsTemplate", TestESv8ExistsTemplate)
	t.Run("PutTemplate", TestESv8PutTemplate)
	t.Run("ExistsTemplate", TestESv8ExistsTemplate)
	t.Run("DeleteTemplate", TestESv8DeleteTemplate)
	t.Run("ExistsTemplate", TestESv8ExistsTemplate)

	// t.Run("ExistsIndex", TestESv8ExistsIndex)
	// t.Run("PutIndex", TestESv8PutIndex)
	// t.Run("ExistsIndex", TestESv8ExistsIndex)
	// t.Run("DeleteIndex", TestESv8DeleteIndex)
	// t.Run("ExistsIndex", TestESv8ExistsIndex)

	t.Run("PutTemplate", TestESv8PutTemplate)
	t.Run("IndexDocument", TestESv8IndexDocument)
	t.Run("QueryDocument", TestESv8QueryDocument)
	// t.Run("DeleteIndex", TestESv8DeleteIndex)
	t.Run("DeleteTemplate", TestESv8DeleteTemplate)
}

func TestESv8Info(t *testing.T) {
	t.Fail()

	es, err := elasticsearchv8.NewClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	// Check client / Cluster info
	res, err := es.Info()
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		t.Fatalf("Error: %s", res.String())
	}
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		t.Fatalf("Error parsing the response body: %s", err)
	}
	t.Logf("Client: %s", elasticsearchv8.Version)
	t.Logf("Server: %s", r["version"].(map[string]interface{})["number"])
	t.Log(strings.Repeat("~", 37))
}

func TestESv8ExistsTemplate(t *testing.T) {
	t.Fail()

	es, err := elasticsearchv8.NewClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	// check template
	res, err := es.Indices.ExistsTemplate([]string{"mytemplate"})
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	if !res.IsError() {
		t.Error("template should be missing")
	}
	t.Log(res)

}

func TestES8CatIndices(t *testing.T) {

	es8, err := elasticsearchv8.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	res, err := es8.Cat.Indices().Index("myindex").Do(context.Background())
	if err != nil {
		t.Error(err)
	}
	defer res.Body.Close()
	var sr []struct {
		Index string `json:"index"`
	}
	err = json.NewDecoder(res.Body).Decode(&sr)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(sr)
	t.Fail()
}

func TestESv8PutTemplate(t *testing.T) {
	t.Fail()

	req := NewTemplateV8([]string{"index-*"}, model)

	es8, err := elasticsearchv8.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	res, err := es8.Indices.PutTemplate("mytemplate").Request(req).Do(context.Background())
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	if res.StatusCode >= 200 && res.StatusCode < 300 {
		t.Error(res.StatusCode)
	}
	t.Log(res)
}

func TestESv8DeleteTemplate(t *testing.T) {
	t.Fail()

	es, err := elasticsearchv8.NewClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	res, err := es.Indices.DeleteTemplate("mytemplate")
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		t.Error("delete template", res.String())
	}
	t.Log(res)
}

func TestESv8IndexDocument(t *testing.T) {

	es, err := elasticsearchv8.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}
	for _, document := range []struct {
		Id    int    `json:"id"`
		Name  string `json:"name"`
		Price int    `json:"price"`
	}{
		{
			Id:    1,
			Name:  "Foo",
			Price: 10,
		},
		{
			Id:    2,
			Name:  "Bar",
			Price: 12,
		},
		{
			Id:    3,
			Name:  "Baz",
			Price: 4,
		},
	} {
		res, err := es.Index("myindex").
			Request(document).
			Id(strconv.Itoa(document.Id)).
			Refresh(refresh.Waitfor).
			Do(context.Background())
		if err != nil {
			t.Fatalf("error indexing document: %s", err)
		}
		defer res.Body.Close()

		var sr map[string]interface{}
		err = json.NewDecoder(res.Body).Decode(&sr)
		if err != nil {
			t.Fatal(err)
		}

		t.Log(sr)
	}
	t.Fail()
}

func TestESv8QueryDocument(t *testing.T) {
	t.Fail()

	es, err := elasticsearchv8.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	// QUERY
	// var buf bytes.Buffer
	// f := engine.Fact{ID: 1, Name: "test", IsObject: false, Model: "mymodel", CalculationDepth: 1, Intent: &engine.IntentFragment{Name: "test", Operator: engine.Count, Term: "f1"}}
	// q, _ := f.ToElasticQuery(time.Now(), make(map[string]string))
	// t.Log(q)
	// query, _ := builder.BuildEsSearchSource(q)
	// t.Log(query)

	req := search.Request{
		Query: &types.Query{
			MatchAll: types.NewMatchAllQuery(),
			// Bool: &types.BoolQuery{
			// 	Must: []types.Query{
			// 		{Term: map[string]types.TermQuery{
			// 			"f2": {Value: "testing"},
			// 		}},
			// 	},
			// },
		},
		Aggregations: map[string]types.Aggregations{
			// "total_prices": {
			// 	Sum: &types.SumAggregation{
			// 		Field: some.String("price"),
			// 	},
			// },
		},
	}

	res, err := es.Search().Index("myindex").Request(&req).Do(context.Background())
	if err != nil {
		t.Fatalf("Error search: %s", err)
	}
	defer res.Body.Close()

	// var sr map[string]interface{}
	// err = json.NewDecoder(res.Body).Decode(&sr)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// t.Log(sr)
	// t.Log(sr["hits"].(map[string]interface{}))
	// t.Log(sr["hits"].(map[string]interface{})["total"].(map[string]interface{}))
	// t.Log(sr["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))

	// t.Log(sr["aggregations"].(map[string]interface{}))
	// t.Log(sr["aggregations"].(map[string]interface{})["total_prices"].(map[string]interface{}))
	// t.Log(sr["aggregations"].(map[string]interface{})["total_prices"].(map[string]interface{})["value"].(float64))

	// var sr2 = NewResponse()
	var sr2 SearchResponse
	err = json.NewDecoder(res.Body).Decode(&sr2)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(sr2.Hits.Total.Value)
	for _, hit := range sr2.Hits.Hits {
		fields, ok := hit.Source_.(map[string]interface{})
		if ok {
			t.Log(hit.Id_)
			t.Log(fields["id"])
		}
	}
	t.Log(sr2.Aggregations["total_prices"])

	// type SearchResult struct {
	// 	Hits struct {
	// 		Total struct {
	// 			Value    int
	// 			Relation string
	// 		} `json:"total"`
	// 		Hits []struct {
	// 			Index  string `json:"_index"`
	// 			Source struct {
	// 				Id   int
	// 				Name string
	// 			} `json:"_source"`
	// 		} `json:"hits"`
	// 	} `json:"hits"`
	// }

	// for name, agg := range searchResponse.Aggregations {
	// 	if name == "total_prices" {
	// 		switch aggregation := agg.(type) {
	// 		case *types.SumAggregate:
	// 			if aggregation.Value != 26. {
	// 				t.Fatalf("error in aggregation, should be 26, got: %f", aggregation.Value)
	// 			}
	// 		default:
	// 			fmt.Printf("unexpected aggregation: %#v\n", agg)
	// 		}
	// 	}
	// }
}

func TestEs8MultiGet(t *testing.T) {
	es, err := elasticsearchv8.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}
	// docs := make([]*models.Document, 0)

	docs := make([]types.MgetOperation, 0)
	docs = append(docs, types.MgetOperation{
		Id_:    "1",
		Index_: some.String("myindex"),
	})
	docs = append(docs, types.MgetOperation{
		Id_:    "3",
		Index_: some.String("myindex"),
	})
	docs = append(docs, types.MgetOperation{
		Id_:    "999",
		Index_: some.String("myindex"),
	})
	docs = append(docs, types.MgetOperation{
		Id_:    "1",
		Index_: some.String("myindexnotexists"),
	})
	req := mget.NewRequest()
	req.Docs = docs
	res, _ := es.Mget().Request(req).Do(context.Background())

	var resp MGetResponse
	_ = json.NewDecoder(res.Body).Decode(&resp)

	for _, r := range resp.Docs {
		t.Log(r.ID, r.Index, r.Found)
	}
	t.Fail()
}

func TestEs8BulkIndex(t *testing.T) {
	es, err := elasticsearchv8.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	buf := bytes.NewBuffer(make([]byte, 0))
	wr := bufio.NewWriter(buf)
	meta := BulkIndexMeta{
		Index: BulkIndexMetaDetail{
			S_Index: "myindex",
			S_Type:  "_doc",
			S_Id:    "51",
		},
	}
	bs1, _ := json.Marshal(meta)
	_, err = wr.Write(bs1)
	if err != nil {
		t.Error(err)
	}
	wr.WriteString(fmt.Sprintln())

	doc := map[string]interface{}{
		"myfield": "helloworld",
	}
	bs1, _ = json.Marshal(doc)
	_, err = wr.Write(bs1)
	if err != nil {
		t.Error(err)
	}
	wr.WriteString(fmt.Sprintln())

	wr.Flush()
	t.Log(buf.String())

	req := esapi.BulkRequest{
		Pretty: false,
		Human:  false,
		Body:   bytes.NewReader(buf.Bytes()),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res, err := req.Do(ctx, es)
	if err != nil {
		t.Error(err)
	}
	defer res.Body.Close()

	var r BulkIndexResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		t.Error(err)
	}
	if len(r.Failed()) > 0 {
		t.Error("failed bulk index")
	}
	b, _ := json.Marshal(r)
	t.Log(string(b))
	t.Fail()
}

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
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/mget"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/refresh"
	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-sdk/v4/modeler"
	"go.uber.org/zap"
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
var cfgv8 = elasticsearch.Config{
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
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

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
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

	es, err := elasticsearch.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	response, err := es.Info().Do(context.Background())
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}

	t.Logf("Client: %s", elasticsearch.Version)
	t.Logf("Server: %s", response.Version.Int)
}

func TestESv8ExistsTemplate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

	es, err := elasticsearch.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	response, err := es.Indices.ExistsTemplate("mytemplate").IsSuccess(context.Background())
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}
	t.Log(response)
}

func TestES8CatIndices(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

	es8, err := elasticsearch.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	response, err := es8.Cat.Indices().Index("myindex").Do(context.Background())
	if err != nil {
		t.Error(err)
	}
	t.Log(response)
}

func TestESv8PutTemplate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

	req := NewPutTemplateRequestV8([]string{"index-*"}, model)
	es8, err := elasticsearch.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	response, err := es8.Indices.PutTemplate("mytemplate").Timeout("15s").Request(req).Do(context.Background())
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}
	t.Log(response)
}

func TestESv8DeleteTemplate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

	es, err := elasticsearch.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

	response, err := es.Indices.DeleteTemplate("mytemplate").Timeout("15s").Do(context.Background())
	if err != nil {
		t.Errorf("Error getting response: %s", err)
	}
	t.Log(response)
}

func TestESv8IndexDocument(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

	es, err := elasticsearch.NewTypedClient(cfgv8)
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
		response, err := es.Index("myindex").
			Request(document).
			Id(strconv.Itoa(document.Id)).
			Refresh(refresh.Waitfor).
			Timeout("15s").
			Do(context.Background())
		if err != nil {
			t.Fatalf("error indexing document: %s", err)
		}
		t.Log(response)
	}
}

func TestESv8QueryDocument(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

	es, err := elasticsearch.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

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

	response, err := es.Search().Index("myindex").Request(&req).Do(context.Background())
	if err != nil {
		t.Fatalf("Error search: %s", err)
	}

	for _, hit := range response.Hits.Hits {
		data, err := jsoniter.Marshal(hit.Source_)
		if err != nil {
			zap.L().Error("update multiget unmarshal", zap.Error(err))
		}

		var source map[string]interface{}
		err = jsoniter.Unmarshal(data, &source)
		if err != nil {
			zap.L().Error("update multiget unmarshal", zap.Error(err))
		}
		t.Log(source)
	}
	t.Log(response.Aggregations["total_prices"])
}

func TestEs8MultiGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

	es, err := elasticsearch.NewTypedClient(cfgv8)
	if err != nil {
		t.Errorf("Error creating the client: %s", err)
	}

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
	response, err := es.Mget().Request(req).Do(context.Background())
	if err != nil {
		t.Error(err)
	}
	t.Log(response)
	for _, d := range response.Docs {
		switch typedDoc := d.(type) {
		case types.MultiGetError:
			t.Log(typedDoc)
		case types.GetResult:
			t.Log(typedDoc)
		}
	}

}

func TestEs8BulkIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping elasticsearch test in short mode")
	}

	es, err := elasticsearch.NewTypedClient(cfgv8)
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
		t.FailNow()
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
}

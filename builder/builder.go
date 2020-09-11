package builder

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v4/elasticsearch"
	"github.com/myrteametrics/myrtea-sdk/v4/utils"
	"github.com/olivere/elastic"
)

const placeholderLimit = "__"

// QuerySourceJSON query and agg elastic syntax ready.
type QuerySourceJSON struct {
	Size  int         `json:"size"`
	From  int         `json:"from"`
	Sort  interface{} `json:"sort,omitempty"`
	Query interface{} `json:"query,omitempty"`
	Aggs  interface{} `json:"aggs,string,omitempty"`
}

// NewQuerySourceJSON builds QuerySourceJSON.
func NewQuerySourceJSON(query interface{}, aggs interface{}, size int, from int, sort interface{}) *QuerySourceJSON {
	return &QuerySourceJSON{
		Size:  size,
		From:  from,
		Sort:  sort,
		Query: query,
		Aggs:  aggs,
	}
}

// BuildEsSearchSource returns a json representation of an elasticsearch query clause
func BuildEsSearchSource(esSearch *EsSearch) (*QuerySourceJSON, error) {
	var err error
	var queriesSource interface{}
	if esSearch.Query != nil {
		queriesSource, err = esSearch.Query.Source().Source()
		if err != nil {
			return nil, err
		}
	}
	var aggsSource = make(map[string]interface{})
	if esSearch.Aggs != nil {
		for _, agg := range esSearch.Aggs {
			aggSource, err := agg.AggSource().Source()
			if err != nil {
				return nil, err
			}
			aggsSource[agg.GetName()] = aggSource
		}
	}
	source := NewQuerySourceJSON(queriesSource, aggsSource, 0, 0, nil)
	return source, nil
}

// BuildEsSearchFromSource builds a backend elasticsearch query definition for advanced source fact definition
func BuildEsSearchFromSource(esExec *elasticsearch.EsExecutor, esSearch *EsSearch) (*elastic.SearchService, error) {
	search := esExec.Client.Search(esSearch.Indices...).Source(esSearch.Source)
	return search, nil
}

// BuildEsSearch builds a backend elasticsearch query definition
func BuildEsSearch(esExec *elasticsearch.EsExecutor, esSearch *EsSearch) (*elastic.SearchService, error) {
	if esSearch.Source != "" {
		return BuildEsSearchFromSource(esExec, esSearch)
	}

	var err error
	var queriesSource interface{}
	if esSearch.Query != nil {
		queriesSource, err = esSearch.Query.Source().Source()
		if err != nil {
			return nil, err
		}
	}

	var aggsSource = make(map[string]interface{})
	if esSearch.Aggs != nil {
		for _, agg := range esSearch.Aggs {
			aggSource, err := agg.AggSource().Source()
			if err != nil {
				return nil, err
			}
			aggsSource[agg.GetName()] = aggSource
		}
	}

	var sort interface{}
	if esSearch.Order {
		sort, err = NewSortByDoc().Source().Source()
		if err != nil {
			return nil, err
		}
	}

	source := NewQuerySourceJSON(queriesSource, aggsSource, esSearch.Size, esSearch.Offset, sort)
	data, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}

	search := esExec.Client.Search(esSearch.Indices...).Source(string(data))

	return search, nil
}

// ContextualizeTimeZonePlaceholders replaces timezone placeholder for old fact version
// @Deprecated
func ContextualizeTimeZonePlaceholders(query string, t time.Time) string {
	query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, "timezone", placeholderLimit), utils.GetTimeZone(t), -1)
	return query
}

// ContextualizeDatePlaceholders contextualize a query with the standard date placeholders (now, begin, timezone)
// @Deprecated
func ContextualizeDatePlaceholders(query string, t time.Time) string {
	// Old Version
	query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, "now", placeholderLimit), utils.GetTime(t), -1)
	query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, "begin", placeholderLimit), utils.GetBeginningOfDay(t), -1)

	// New Version
	query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, "startofday-1j", placeholderLimit), utils.GetBeginningOfDay(t.Add(-1*24*time.Hour)), -1)
	query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, "endofday-1j", placeholderLimit), utils.GetBeginningOfDay(t), -1)
	query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, "startofday", placeholderLimit), utils.GetBeginningOfDay(t), -1)
	query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, "endofday", placeholderLimit), utils.GetBeginningOfDay(t.Add(1*24*time.Hour)), -1)
	query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, "startofday+1j", placeholderLimit), utils.GetBeginningOfDay(t.Add(1*24*time.Hour)), -1)
	query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, "endofday+1j", placeholderLimit), utils.GetBeginningOfDay(t.Add(2*24*time.Hour)), -1)
	return query
}

// ContextualizePlaceholders contextualize a query with an ensemble of placeholders (__placeholder__)
// @Deprecated
func ContextualizePlaceholders(query string, placeholders map[string]string) string {
	for k, v := range placeholders {
		query = strings.Replace(query, fmt.Sprintf("%s%s%s", placeholderLimit, k, placeholderLimit), v, -1)
	}
	return query
}

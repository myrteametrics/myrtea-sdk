package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/myrteametrics/myrtea-sdk/v4/models"
	"github.com/olivere/elastic"

	zap "go.uber.org/zap"
)

// EsExecutor wraps and exposes all elasticsearch tasks
type EsExecutor struct {
	ctx    context.Context
	Client *elastic.Client
}

// SearchResult wraps elasticsearch SearchResult
type SearchResult struct {
	SResult *elastic.SearchResult
}

// SearchService wraps elasticsearch SearchService
type SearchService struct {
	SService *elastic.SearchService
}

// BulkResponse wraps elasticsearch BulkResponse
type BulkResponse struct {
	BResponse *elastic.BulkResponse
}

// MGetResponse wraps elasticsearch MGetResponse
type MGetResponse struct {
	MgetResponse *elastic.MgetResponse
}

// NewEsExecutor returns a pointer to an EsExecutor
func NewEsExecutor(ctx context.Context, urls []string) (*EsExecutor, error) {
	client, err := elastic.NewClient(elastic.SetSniff(false),
		elastic.SetHealthcheckTimeoutStartup(60*time.Second),
		elastic.SetURL(urls...),
		//elastic.SetBasicAuth()
	)
	if err != nil {
		return nil, err
	}
	return &EsExecutor{ctx, client}, nil
}

// ClientHealthCheck checks if an elasticsearch cluster is up and running
func (executor *EsExecutor) ClientHealthCheck(ctx context.Context) bool {
	health, err := executor.Client.ClusterHealth().Do(ctx)
	if err != nil && health != nil && health.Status != "red" {
		return false
	}
	return true
}

// PutTemplate initializes given template if it doesn't already exists.
func (executor *EsExecutor) PutTemplate(ctx context.Context, templateName string, templateBody *models.TemplateV6) error {
	templateExists, err := executor.Client.
		IndexTemplateExists(templateName).
		Do(ctx)
	if err != nil {
		return err
	}
	if !templateExists {
		templateBodyJSON, err := json.Marshal(templateBody)
		if err != nil {
			return err
		}
		indicesPutTemplateResponse, err := executor.Client.
			IndexPutTemplate(templateName).
			BodyString(string(templateBodyJSON)).
			Do(ctx)
		if err != nil {
			return err
		}
		if indicesPutTemplateResponse != nil && !indicesPutTemplateResponse.Acknowledged {
			return errors.New("ES API return false acknowledged")
		}
		zap.L().Info("Putting template success", zap.String("template", templateName))
		//, zap.ByteString("body", templateBodyJSON))
	}
	return nil
}

// ExecuteSearch execute a search
func (executor *EsExecutor) ExecuteSearch(ctx context.Context, search *elastic.SearchService) (*elastic.SearchResult, error) {
	response, err := search.Do(ctx)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// GetIndicesByAlias returns a slice of string containing all indices related to an alias
func (executor *EsExecutor) GetIndicesByAlias(ctx context.Context, alias string) ([]string, error) {
	aliasResult, err := executor.Client.Aliases().Alias(alias).Do(ctx)
	if err != nil {
		return nil, err
	}
	return aliasResult.IndicesByAlias(alias), nil
}

// DeleteIndices deletes an ensemble of indices
func (executor *EsExecutor) DeleteIndices(ctx context.Context, indices []string) error {
	deletedResponses, err := executor.Client.DeleteIndex(indices...).Do(ctx)
	if err != nil {
		return err
	}
	if deletedResponses.Acknowledged != true {
		return errors.New("deletedResponses.Acknowledged == false")
	}
	return nil
}

// PutAlias put a new index alias
// TODO: Must check if alias doesn't already exists
func (executor *EsExecutor) PutAlias(ctx context.Context, patternIndex string, alias string) error {
	aliasResult, err := executor.Client.Alias().Add(patternIndex, alias).Do(ctx)
	if err != nil {
		return err
	}
	if !aliasResult.Acknowledged {
		return errors.New("ES API return false acknowledged")
	}
	zap.L().Info("Putting alias success", zap.String("index", patternIndex), zap.String("alias", alias))
	return nil
}

// RollOver execute a full index rollover
func (executor *EsExecutor) RollOver(ctx context.Context, alias string, ageCondition string, docsCondition int64) (string, string, error) {
	indicesRolloverResult, err := executor.Client.RolloverIndex(alias).
		AddMaxIndexAgeCondition(ageCondition).
		AddMaxIndexDocsCondition(docsCondition).
		Do(ctx)

	if err != nil {
		return "", "", err
	}
	if !indicesRolloverResult.Acknowledged {
		return "", "", errors.New("ES API return false acknowledged")
	}
	zap.L().Info("RollOver success", zap.String("alias", alias),
		zap.String("old-index", indicesRolloverResult.OldIndex),
		zap.String("new-index", indicesRolloverResult.NewIndex))
	return indicesRolloverResult.OldIndex, indicesRolloverResult.NewIndex, nil
}

// IndexExists check if at least one index exists based on a pattern
func (executor *EsExecutor) IndexExists(ctx context.Context, patternIndexExists string) (bool, error) {
	zap.L().Info("Check if index exists", zap.String("pattern", patternIndexExists))
	indices, err := executor.Client.IndexGet(patternIndexExists).Do(ctx)
	if err != nil {
		return false, err
	}
	return (len(indices) != 0), nil
}

// PutIndex initializes a new index if it doesn't already exists.
// If no pattern is specified, the default pattern will be the index name
func (executor *EsExecutor) PutIndex(ctx context.Context, patternIndexExists string, indexName string) error {
	if patternIndexExists == "" {
		patternIndexExists = indexName
	}
	indexExists, err := executor.IndexExists(ctx, patternIndexExists)
	if err != nil {
		return err
	}
	if !indexExists {
		indicesCreateResult, err := executor.Client.CreateIndex(indexName).Do(ctx)
		if err != nil {
			return err
		}
		if !indicesCreateResult.Acknowledged {
			return errors.New("ES API return false acknowledged")
		}
		zap.L().Info("Putting index", zap.String("index", indexName))
	}
	return nil
}

// BulkIndex inserts documents in bulk into an elasticsearch index, with a specific type.
func (executor *EsExecutor) BulkIndex(ctx context.Context, docs []*models.Document) (*elastic.BulkResponse, error) {
	bulkRequest := executor.Client.Bulk()
	for _, doc := range docs {
		req := elastic.NewBulkIndexRequest().Index(doc.Index).
			Type(doc.IndexType).Id(doc.ID).Doc(doc.Source)
		bulkRequest = bulkRequest.Add(req)
	}
	bulkResponse, err := bulkRequest.Do(ctx)
	return bulkResponse, err
}

// MultiGet executes multiple get queries and return all results in a single response
func (executor *EsExecutor) MultiGet(ctx context.Context, docs []*models.Document) (*elastic.MgetResponse, error) {
	if len(docs) == 0 {
		return nil, errors.New("docs[] is empty")
	}
	var items []*elastic.MultiGetItem
	for _, doc := range docs {
		items = append(items, elastic.NewMultiGetItem().Index(doc.Index).Id(doc.ID))
	}

	s, err := elastic.NewMgetService(executor.Client).Add(items...).Do(ctx)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// MultiSearch executes multiple search queries and return all results in a single response
func (executor *EsExecutor) MultiSearch(ctx context.Context, docs []*models.Document) (*elastic.MultiSearchResult, error) {
	if len(docs) == 0 {
		return nil, errors.New("docs[] is empty")
	}
	var items []*elastic.SearchRequest
	for _, doc := range docs {
		items = append(items, elastic.NewSearchRequest().Index(doc.Index).Query(elastic.NewIdsQuery(doc.IndexType).Ids(doc.ID)))
	}

	s, err := elastic.NewMultiSearchService(executor.Client).Add(items...).Do(ctx)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// BulkUpdate insert, or partially updates documents already in an elasticsearch index
// following the provided merge function.
// Be careful, this method is not thread-safe
// TODO: A revoir la notion de merge
func (executor *EsExecutor) BulkUpdate(ctx context.Context, docs []*models.Document, fmerge func(*models.Document, *models.Document)) (*elastic.BulkResponse, error) {
	response, err := executor.MultiGet(ctx, docs)
	if err != nil {
		return nil, err
	}
	for i, d := range response.Docs {
		var doc *models.Document
		data, _ := jsoniter.Marshal(d.Source)
		err = jsoniter.Unmarshal(data, &doc)
		if err != nil {
			return nil, err
		}

		if doc != nil {
			fmerge(docs[i], doc)
		}
	}
	bulkResponse, err := executor.BulkIndex(ctx, docs)
	return bulkResponse, err
}

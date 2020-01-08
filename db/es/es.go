package es

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tingxin/go-utility/log"
	elastic "gopkg.in/olivere/elastic.v5"
)

var (
	bulkCount = 50
)

var defaultContext = context.Background()

// ConnInfo used to cache es client
type ConnInfo struct {
	client *elastic.Client
	url    string
	user   string
	pass   string
}

// BuildStoreHandler used express the function type for build store item
type BuildStoreHandler func(i interface{}) ([]string, []interface{})

// DataTupleHandler used process the raw data to target object
type DataTupleHandler func(rowIndex int, row []sql.RawBytes) ([]interface{}, error)

// NewConnInfo used create ConnInfo
func NewConnInfo(url, user, pass string) *ConnInfo {
	var conn *ConnInfo
	for i := 0; i < 3; i++ {
		log.INFO.Printf("Try to collect es[%s] %d", url, i)
		p, err := GetConn(url, user, pass)
		if err == nil {
			conn = p
			break
		}
		log.ERROR.Printf("Failed to collect es[%s] due to %v", url, err)
		time.Sleep(time.Second * 3)
	}
	return conn
}

// GetConn used to get a connection from the es server
func GetConn(url, user, pass string) (*ConnInfo, error) {
	client, err := elastic.NewClient(elastic.SetURL(url), elastic.SetBasicAuth(user, pass))
	if err != nil {
		return nil, err
	}
	conn := &ConnInfo{url: url, user: user, pass: pass, client: client}
	return conn, nil
}

// SetBulkCount used to set bulk count
func SetBulkCount(count int) {
	bulkCount = count
}

// DefaultContext used to return default es context
func DefaultContext() context.Context {
	return defaultContext
}

// FetchRawWithConn used to get doc
func FetchRawWithConn(conn *ConnInfo, index, docType, docID string) (*json.RawMessage, bool) {
	client := conn.client
	if !client.IsRunning() {
		conn = NewConnInfo(conn.url, conn.user, conn.pass)
		client = conn.client
	}
	// Get tweet with specified ID
	get1, err := client.Get().
		Index(index).
		Type(docType).
		Id(docID).
		Do(defaultContext)
	if err != nil || get1 == nil || !get1.Found {
		return nil, false
	}
	return get1.Source, true
}

// FetchWithConn used to get doc
func FetchWithConn(conn *ConnInfo, index, docType, docID string, entity interface{}) bool {
	rawMessage, ok := FetchRawWithConn(conn, index, docType, docID)
	if ok {
		json.Unmarshal(*rawMessage, &entity)
	}
	return ok
}

// Push used to insert data to es
func Push(index, docType, docID string) error {
	return nil
}

// PushBulk used to insert bulk data to es
func PushBulk(conn *ConnInfo, index, docType string, docIDs []string, docs []interface{}) error {
	client := conn.client
	if !client.IsRunning() {
		conn = NewConnInfo(conn.url, conn.user, conn.pass)
		client = conn.client
	}
	docCount := len(docs)
	bulkRequest := client.Bulk()
	for i, doc := range docs {
		docID := docIDs[i]
		indexReq := elastic.NewBulkIndexRequest().Index(index).Type(docType).Id(docID).Doc(doc)
		bulkRequest = bulkRequest.Add(indexReq)
	}

	// NumberOfActions contains the number of requests in a bulk
	if bulkRequest.NumberOfActions() != docCount {

		return fmt.Errorf("Error: failed to insert docs %v", docIDs)
	}

	// Do sends the bulk requests to Elasticsearch
	bulkResponse, err := bulkRequest.Do(context.Background())
	if err != nil {
		return err
	}

	// Bulk request actions get cleared
	if bulkRequest.NumberOfActions() != 0 {
		return err
	}

	indexed := bulkResponse.Indexed()

	if len(indexed) != docCount {
		return fmt.Errorf("Error: failed to insert docs %v", docIDs)
	}

	fail := bulkResponse.Failed()
	for _, item := range fail {
		return fmt.Errorf("%s", item.Error.Reason)
	}
	return nil
}

// PushBulkPro used to push data by PushBulk
func PushBulkPro(conn *ConnInfo, index, doc string, input []interface{}, converter BuildStoreHandler) (successCount int, err error) {
	bulkCache := make([]interface{}, bulkCount, bulkCount)
	bulkKeyCache := make([]string, bulkCount, bulkCount)
	var cursor int
	successCount = 0
	for _, item := range input {
		docIDs, storeItems := converter(item)
		for i := 0; i < len(docIDs); i++ {
			bulkKeyCache[cursor] = docIDs[i]
			bulkCache[cursor] = storeItems[i]
			cursor++

			if cursor >= bulkCount {
				err = PushBulk(conn, index, doc, bulkKeyCache, bulkCache)
				if err == nil {
					successCount += len(bulkKeyCache)
				}
				cursor = 0
			}
		}
	}
	if cursor > 0 {
		bulkKeyCache = bulkKeyCache[0 : cursor+1]
		bulkCache = bulkCache[0 : cursor+1]

		p := PushBulk(conn, index, doc, bulkKeyCache, bulkCache)
		if err == nil {
			err = p
		}
		if p == nil {
			successCount += cursor
		}
	}
	return
}

// CreateIndex used to create index in es
func CreateIndex(conn *ConnInfo, name, mappingContent string) error {
	client := conn.client
	if !client.IsRunning() {
		conn = NewConnInfo(conn.url, conn.user, conn.pass)
		client = conn.client
	}
	_, err := client.CreateIndex(name).BodyString(mappingContent).Do(defaultContext)
	if err != nil {
		// Handle error
		return err
	}
	return nil
}

// DeleteIndex used to delete index in es
func DeleteIndex(conn *ConnInfo, name string) error {
	// Delete an index
	client := conn.client
	if !client.IsRunning() {
		conn = NewConnInfo(conn.url, conn.user, conn.pass)
		client = conn.client
	}
	_, err := client.DeleteIndex(name).Do(defaultContext)
	if err != nil {
		// Handle error
		return err
	}
	return nil
}

// DoMaxAggreation used to max aggreation
func DoMaxAggreation(conn *ConnInfo, index, filed, timeFiled string, begin, end int64) []interface{} {
	maxAggregation := elastic.NewMaxAggregation()
	maxAggregation.Field(filed)
	client := conn.client
	if !client.IsRunning() {
		conn = NewConnInfo(conn.url, conn.user, conn.pass)
		client = conn.client
	}
	query := elastic.NewRangeQuery(timeFiled).From(begin).To(end)
	searchResult, err := client.Search().
		Index(index).
		Query(query).
		Aggregation("Max", maxAggregation).
		Pretty(true). // pretty print request and response JSON
		Do(defaultContext)

	if err != nil {
		// Handle error
		panic(err)
	}

	length := len(searchResult.Aggregations)
	result := make([]interface{}, length, length)

	var cursor int
	for _, item := range searchResult.Aggregations {
		var out ValueObject
		data := []byte(*item)
		json.Unmarshal(data, &out)
		result[cursor] = out.Value
		cursor++
	}
	return result
}

// MakeESTimeStamp used to make es timestamp
func MakeESTimeStamp(timeObj time.Time) int64 {
	return timeObj.UnixNano() / int64(time.Millisecond)
}

// ValueObject used
type ValueObject struct {
	Value interface{} `json:"value"`
}

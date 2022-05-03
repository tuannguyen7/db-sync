package streaming

import (
	"context"
	bigqueryclient "db-sync/clients/bigquery"
	"db-sync/clients/webdatabases"
	"db-sync/config"
	"db-sync/helpers"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
)

type webDBToBQStreaming struct {
	bqClient    *bigqueryclient.Client
	webDBClient *webdatabases.Client
	batchSize   int64
	tables      []string
	guardSize   int
}

func NewWebDBToBQStreaming(bqClient *bigqueryclient.Client, webDBClient *webdatabases.Client, batchSize int64, tables []string) *webDBToBQStreaming {
	return &webDBToBQStreaming{
		bqClient:    bqClient,
		webDBClient: webDBClient,
		batchSize:   batchSize,
		tables:      tables,
		guardSize:   5,
	}
}

func (s *webDBToBQStreaming) Stream(ctx context.Context) error {
	for _, tableName := range s.tables {
		err := s.streamTable(ctx, tableName)
		if err != nil {
			log.WithFields(log.Fields{
				"tableName": tableName,
			}).Errorln("error streaming table into presync dataset in BQ")
			continue
		}

		err = s.mergeTable(ctx, tableName)
		if err != nil {
			log.WithFields(log.Fields{
				"tableName": tableName,
			}).Errorln("error merging table from presync dataset to web-sync in BQ")
		}
	}

	return nil
}

func (s *webDBToBQStreaming) mergeTable(ctx context.Context, tableName string) error {
	log.WithFields(log.Fields{
		"tableName": tableName,
	}).Infoln("start merging table from presync dataset to web-sync dataset in Biqquery")
	query, err := s.generateMergeQuery(ctx, tableName)
	if err != nil {
		return err
	}

	q := s.bqClient.Query(query)
	q.Location = "US"
	job, err := q.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if err := status.Err(); err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"tableName": tableName,
	}).Infoln("done merging table from presync dataset to web-sync dataset in Biqquery")
	return nil
}

func (s *webDBToBQStreaming) generateMergeQuery(ctx context.Context, tableName string) (string, error) {
	temp := `
		MERGE {{.tableName}} T
			USING (SELECT
			agg.table.*
			FROM (
			SELECT
			id,
			ARRAY_AGG(STRUCT(table)
			ORDER BY
			_created_at DESC)[SAFE_OFFSET(0)] agg
			FROM
			{{.presyncTableName}} table
			WHERE
			{{.whereClause}}
			GROUP BY
			id, _date)) S
			ON T.id = S.id and T._date = T._date
			WHEN MATCHED THEN
		UPDATE SET {{.updateClause}}
			WHEN NOT MATCHED THEN
		INSERT {{.insertFieldClause}} VALUES {{.insertValueClause}}
		`

	columns, err := s.webDBClient.GetTableInfo(ctx, tableName)
	if err != nil {
		return "", err
	}
	var updateColumnNames []string
	var updateItems []string
	for _, c := range columns {
		updateColumnNames = append(updateColumnNames, c.Name)
	}
	for _, c := range updateColumnNames {
		updateItems = append(updateItems, fmt.Sprintf("%s = S.%s", c, c))
	}

	insertColumnName := append(updateColumnNames, "_date")
	insertFieldClause := fmt.Sprintf("(%s)", strings.Join(insertColumnName, ","))
	insertValueClause := insertFieldClause
	updateClause := strings.Join(updateItems, ",")

	bqTableName := strings.ToLower(tableName)
	variables := map[string]interface{}{
		"tableName":         fmt.Sprintf("`%s.%s`", config.BqWebsyncDataset, bqTableName),
		"presyncTableName":  fmt.Sprintf("`%s.%s`", config.BqPresyncDataset, bqTableName),
		"updateClause":      updateClause,
		"insertFieldClause": insertFieldClause,
		"insertValueClause": insertValueClause,
		"whereClause":       `_date = CURRENT_DATE("+7")`,
	}

	return helpers.MakeTemplateFile(temp, variables)
}

func (s *webDBToBQStreaming) streamTable(ctx context.Context, originalTableName string) error {
	var wg sync.WaitGroup
	totalRows, err := s.webDBClient.GetTotalRows(ctx, originalTableName)
	if err != nil {
		return err
	}
	bqTableName := strings.ToLower(originalTableName)
	maxGoroutines := 5
	guard := make(chan struct{}, maxGoroutines)

	loopCount := int(totalRows/s.batchSize) + 1
	for i := 0; i < loopCount; i++ {
		guard <- struct{}{}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			log.WithFields(log.Fields{
				"tableName":   bqTableName,
				"batchNumber": n,
			}).Infoln("start inserting a batch rows into Bigquery...")
			offset := s.batchSize * int64(n)
			rows, err := s.webDBClient.GetRows(ctx, originalTableName, s.batchSize, offset)
			if err != nil {
				log.WithFields(log.Fields{
					"tableName": originalTableName,
					"offset":    offset,
					"error":     err,
				}).Errorln("error getting rows from database")
				return
			}

			err = s.bqClient.InsertOrUpdate(ctx, config.BqPresyncDataset, bqTableName, rows)
			if err != nil {
				log.WithFields(log.Fields{
					"tableName": bqTableName,
					"offset":    offset,
					"error":     err,
				}).Errorln("error inserting rows into Bigquery")
				return
			}

			log.WithFields(log.Fields{
				"tableName":   bqTableName,
				"offset":      offset,
				"batchNumber": n,
			}).Infoln("done inserting a batch rows into Bigquery")
			<-guard
		}(i)
	}

	wg.Wait()
	log.WithFields(log.Fields{
		"BQTableName":  fmt.Sprintf("%s.%s", config.BqPresyncDataset, bqTableName),
		"WebTableName": originalTableName,
	}).Infoln("successfully streaming table from web databases to BigQuery")
	return nil
}

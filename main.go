package main

import (
	"context"
	bigqueryclient "db-sync/clients/bigquery"
	"db-sync/clients/webdatabases"
	"db-sync/config"
	"db-sync/streaming"
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

func startUp() {
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		ForceColors:     true,
		FullTimestamp:   true,
	})
}

func main() {
	ctx := context.Background()
	startUp()
	dbClient, err := webdatabases.NewClient()
	if err != nil {
		log.Errorln(err)
		return
	}
	defer dbClient.Close()

	bqClient, err := bigqueryclient.NewClient()
	if err != nil {
		log.Errorln(err)
		return
	}
	defer bqClient.Close()

	tables := strings.Split(config.StreamingDbTables, config.Separator)
	batchSize, err := strconv.ParseInt(config.StreamingBatchSize, 10, 64)
	if err != nil {
		log.Errorln(err)
		return
	}

	log.WithFields(log.Fields{
		"tables": tables,
	}).Infoln("streaming tables to BQ")
	streamingService := streaming.NewWebDBToBQStreaming(bqClient, dbClient, batchSize, tables)

	err = streamingService.Stream(ctx)
	if err != nil {
		log.Errorln(err)
		return
	}
}

func createTableWrapper() {
	err := creatTable()
	if err != nil {
		log.Errorln(err)
	}
}

func creatTable() error {
	dbClient, err := webdatabases.NewClient()
	if err != nil {
		return err
	}
	defer dbClient.Close()

	tableName := "Products"
	ctx := context.Background()
	columnDef, err := dbClient.GetTableInfo(ctx, tableName)
	if err != nil {
		return err
	}

	bqClient, err := bigqueryclient.NewClient()
	if err != nil {
		return err
	}
	defer bqClient.Close()

	return bqClient.CreateSyncTimePartitionTable(ctx, config.BqWebsyncDataset, config.BqPresyncDataset, strings.ToLower(tableName), columnDef)
}

func getRowDb() error {
	dbClient, err := webdatabases.NewClient()
	if err != nil {
		return err
	}
	defer dbClient.Close()

	ctx := context.Background()
	rows, err := dbClient.GetRows(ctx, "Products", 10, 0)
	if err != nil {
		return err
	}

	bqClient, err := bigqueryclient.NewClient()
	if err != nil {
		return err
	}
	defer bqClient.Close()

	return bqClient.InsertOrUpdate(ctx, config.BqPresyncDataset, "products", rows)
}

func sync() error {
	ctx := context.Background()
	bqClient, err := bigqueryclient.NewClient()
	if err != nil {
		return err
	}
	q := bqClient.Query("SELECT * FROM dwh.aging_121online LIMIT 1")
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
	it, err := job.Read(ctx)
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Println(row)
	}
	return nil
}

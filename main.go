package main

import (
	"context"
	bigqueryclient "db-sync/clients/bigquery"
	"db-sync/clients/kiotviet"
	"db-sync/clients/webdatabases"
	"db-sync/config"
	"db-sync/data"
	"db-sync/streaming"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
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
	kiotvietClient, err := kiotviet.NewClient()
	if err != nil {
		log.Errorln(err)
		return
	}

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
	kiotvietService := streaming.NewKiotVietStreaming(bqClient, kiotvietClient)

	err = streamingService.Stream(ctx)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Errorln("error streaming tables from web database into BQ")
	} else {
		log.Infoln("done streaming tables from web database into BQ")
	}

	err = kiotvietService.StreamTransfers(ctx)
	if err != nil {
		log.Errorln(err)
	}
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Errorln("error streaming from KiotViet into BQ")
	} else {
		log.Infoln("done streaming from KiotViet into BQ")
	}

	return
}

func createTableWrapper() {
	err := creatTable()
	if err != nil {
		log.Errorln(err)
	}
}

func createTransferTable() {
	bqClient, err := bigqueryclient.NewClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer bqClient.Close()

	ctx := context.Background()
	tableName := "kiotviet_transfers"
	columnDef := []data.Column{
		{
			Name:     "id",
			DataType: "int",
			NullAble: false,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "code",
			DataType: "string",
			NullAble: false,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "from_branch_id",
			DataType: "int",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "to_branch_id",
			DataType: "int",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "status",
			DataType: "int",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "transfer_date",
			DataType: "time",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "received_date",
			DataType: "time",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "retailer_id",
			DataType: "int",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "sent_note",
			DataType: "string",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "received_note",
			DataType: "string",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "product_id",
			DataType: "int",
			NullAble: false,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "product_code",
			DataType: "string",
			NullAble: false,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "product_name",
			DataType: "string",
			NullAble: false,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "sent_quantity",
			DataType: "int",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "received_quantity",
			DataType: "int",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "sent_price",
			DataType: "float64",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "received_price",
			DataType: "float64",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "price",
			DataType: "float64",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "sent_imei_serials",
			DataType: "string",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "received_imei_serials",
			DataType: "string",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "created_user_name",
			DataType: "string",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
		{
			Name:     "barcode",
			DataType: "string",
			NullAble: true,
			From:     data.ColumnFromKiotViet,
		},
	}

	err = bqClient.CreateSyncTimePartitionTable(ctx, config.BqWebsyncDataset, config.BqPresyncDataset, strings.ToLower(tableName), columnDef)
	if err != nil {
		fmt.Println(err)
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

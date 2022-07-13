package streaming

import (
	"context"
	biqueryclient "db-sync/clients/bigquery"
	"db-sync/clients/kiotviet"
	"db-sync/config"
	"db-sync/data"
	"db-sync/helpers"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type KiotVietStreaming struct {
	kiotVietClient *kiotviet.Client
	bqClient       *biqueryclient.Client
}

const (
	ListTransfersLimit     = 100
	ListTransfersBatchSize = 500
)

const (
	KiotvietTransferTable = "kiotviet_transfers"
)

var transfersUpdateColumnNames = []string{"id", "_sub_id", "code", "from_branch_id", "to_branch_id", "status", "transfer_date", "received_date", "retailer_id", "sent_note", "received_note", "product_id", "product_code", "product_name", "sent_quantity", "received_quantity", "sent_price", "received_price", "price", "sent_imei_serials", "received_imei_serials", "created_user_name", "barcode"}

func NewKiotVietStreaming(bqClient *biqueryclient.Client, kiotvietClient *kiotviet.Client) *KiotVietStreaming {
	return &KiotVietStreaming{
		kiotVietClient: kiotvietClient,
		bqClient:       bqClient,
	}
}

func (s *KiotVietStreaming) StreamTransfers(ctx context.Context) error {
	offset := 0
	twoMonthAgo := time.Now().Add(-(time.Hour * 24 * 60))
	for true {
		var transfers []kiotviet.TransferBasicInfo
		for true {
			page, err := s.kiotVietClient.ListTransfers(ctx, ListTransfersLimit, offset)
			if err != nil {
				return err
			}
			offset += ListTransfersLimit
			transfers = append(transfers, page.Transfers...)
			if page.PageSize == 0 || len(transfers) >= ListTransfersBatchSize {
				break
			}
		}

		if len(transfers) == 0 {
			break
		}

		if transfers[0].TransferDate != nil && transfers[0].TransferDate.ToTime().Before(twoMonthAgo) {
			log.WithFields(log.Fields{
				"lastTransferID": transfers[0].ID,
			}).Infoln("done pushing 2 month transfer records into presync dataset")
			break
		}

		err := s.StreamTransfersPage(ctx, transfers)
		if err != nil {
			log.WithFields(log.Fields{
				"batchSize": ListTransfersBatchSize,
				"offset":    offset,
				"error":     err,
			}).Errorln("error streaming transfers page")
			break
		} else {
			log.WithFields(log.Fields{
				"batchSize": ListTransfersBatchSize,
				"offset":    offset,
			}).Infoln("done streaming transfers page")
		}
	}

	query, err := s.generateMergeQuery(ctx, KiotvietTransferTable, transfersUpdateColumnNames)
	if err != nil {
		return err
	}

	q := s.bqClient.Query(query)
	q.Location = "US"
	err = helpers.RunQuery(ctx, q)

	if err != nil {
		log.WithFields(log.Fields{
			"tableName": KiotvietTransferTable,
			"error":     err,
		}).Errorln("error merging table from presync dataset to web-sync dataset in Biqquery")
	} else {
		log.WithFields(log.Fields{
			"tableName": KiotvietTransferTable,
		}).Infoln("done merging table from presync dataset to web-sync dataset in Biqquery")
	}
	return err
}

func (s *KiotVietStreaming) StreamTransfersPage(ctx context.Context, transfers []kiotviet.TransferBasicInfo) error {
	function := "StreamTransfersPage"
	var rows []data.Row
	var wg sync.WaitGroup
	var lock = sync.RWMutex{}

	logEntry := log.WithFields(log.Fields{
		"function": function,
	})
	defer helpers.Elapsed(logEntry)()

	maxGettingDetailRoutines := 50
	guard := make(chan struct{}, maxGettingDetailRoutines)
	transferIDToTransferDetail := make(map[int64][]*kiotviet.WebTransferDetail)

	for _, t := range transfers {
		guard <- struct{}{}
		wg.Add(1)
		go func(transfer kiotviet.TransferBasicInfo) {
			webTransferResp, err := s.kiotVietClient.GetTransferDetailWeb(ctx, transfer.ID)
			if err != nil {
				log.WithFields(log.Fields{
					"TransferID": transfer.ID,
					"error":      err,
				}).Errorln("error getting web detail")
			}

			lock.Lock()
			transferIDToTransferDetail[transfer.ID] = webTransferResp.TransferDetail
			lock.Unlock()
			<-guard
			wg.Done()
		}(t)
	}

	wg.Wait()
	for _, t := range transfers {
		convertedRows := s.basicTransferToRows(t, transferIDToTransferDetail[t.ID])
		rows = append(rows, convertedRows...)
	}

	err := s.bqClient.InsertOrUpdate(ctx, config.BqPresyncDataset, KiotvietTransferTable, &data.Rows{Rows: rows})
	return err
}

func (s *KiotVietStreaming) generateMergeQuery(ctx context.Context, tableName string, updateColumnNames []string) (string, error) {
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
			id, _sub_id, _date)) S
			ON T.id = S.id and T._sub_id = S._sub_id and T._date = S._date
			WHEN MATCHED THEN
		UPDATE SET {{.updateClause}}
			WHEN NOT MATCHED THEN
		INSERT {{.insertFieldClause}} VALUES {{.insertValueClause}}
		`

	var updateItems []string
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

func (s *KiotVietStreaming) basicTransferToRows(basicTransfer kiotviet.TransferBasicInfo, webTransferDetails []*kiotviet.WebTransferDetail) []data.Row {
	var rows []data.Row
	for i, detail := range basicTransfer.TransferDetail {
		transfer := make(map[string]interface{})
		// Transfer detail ID is a pair (id, _sub_id)
		transfer["_sub_id"] = i + 1
		transfer["id"] = basicTransfer.ID

		transfer["code"] = basicTransfer.Code
		transfer["from_branch_id"] = basicTransfer.FromBranchID
		transfer["to_branch_id"] = basicTransfer.ToBranchID
		if basicTransfer.TransferDate != nil {
			transfer["transfer_date"] = basicTransfer.TransferDate.ToTime()
		}
		if basicTransfer.ReceivedDate != nil {
			transfer["received_date"] = basicTransfer.ReceivedDate.ToTime()
		}
		transfer["retailer_id"] = basicTransfer.RetailerID
		transfer["sent_note"] = basicTransfer.Description
		transfer["status"] = basicTransfer.Status

		// detail
		transfer["product_id"] = detail.ProductID
		transfer["product_code"] = detail.ProductCode
		transfer["product_name"] = detail.ProductName
		transfer["sent_quantity"] = detail.SentQuantity
		transfer["received_quantity"] = detail.ReceivedQuantity
		transfer["sent_price"] = detail.SentPrice
		transfer["received_price"] = detail.ReceivedPrice
		transfer["price"] = detail.Price
		if webTransferDetails != nil {
			webDetail := webTransferDetails[i]
			transfer["sent_imei_serials"] = webDetail.SerialNumbers
			transfer["received_imei_serials"] = webDetail.ReceivedSerialNumbers
			transfer["barcode"] = webDetail.Product.BarCode
		}
		//transfer["created_user_name"] = webTransferDetail.SerialNumbers
		rows = append(rows, data.Row{Values: transfer})
	}

	return rows
}

package kiotviet

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"time"
)

const KiotTimeLayout = "2006-01-02T15:04:05"

type KiotTime struct {
	t time.Time
}

// Structs returned from public APIs

type AccessToken struct {
	AccessToken string `json:"access_token"`
	ExpiredIn   int64  `json:"expired_id"`
	TokenType   string `json:"token_type"`
}

type TransferPage struct {
	Total     int64               `json:"total"`
	PageSize  int64               `json:"pageSize"`
	Transfers []TransferBasicInfo `json:"data"`
}

// Miss:
// Khong co ghi chu nhan
type TransferBasicInfo struct {
	ID             int64            `json:"id"`
	Code           string           `json:"code"`
	FromBranchID   int64            `json:"fromBranchId"`
	ToBranchID     int64            `json:"toBranchId"`
	Status         int              `json:"status"`
	TransferDate   *KiotTime        `json:"dispatchedDate"`
	ReceivedDate   *KiotTime        `json:"receivedDate"`
	RetailerID     int64            `json:"retailerId"`
	Description    string           `json:"description"` // Ghi chu chuyen
	TransferDetail []TransferDetail `json:"transferDetails"`
}

type TransferDetail struct {
	ProductID        int64   `json:"productId"`
	ProductCode      string  `json:"productCode"`
	ProductName      string  `json:"productName"`
	SentQuantity     int64   `json:"sendQuantity"`
	ReceivedQuantity int64   `json:"receiveQuantity"`
	SentPrice        float64 `json:"sendPrice"`
	ReceivedPrice    float64 `json:"receivePrice"`
	Price            float64 `json:"price"`
}

// Structs returned from Web APIs

type WebAccessToken struct {
	Token   string         `json:"token"`
	Cookies []*http.Cookie `json:"-"`
}

// In-progress
type WebTransferBasicInfo struct {
}

type WebTransferDetailResp struct {
	TransferDetail []*WebTransferDetail `json:"Data"`
}

type WebTransferDetail struct {
	ProductId             int64      `json:"ProductId"`
	ProductCode           string     `json:"ProductCode"`
	ProductName           string     `json:"ProductName"`
	SerialNumbers         *string    `json:"SerialNumbers"`
	ReceivedSerialNumbers *string    `json:"ReceiveSerialNumbers"`
	Product               WebProduct `json:"Product"`
}

type WebProduct struct {
	BarCode *string `json:"Barcode"`
}

func (t KiotTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", t.t.Format(KiotTimeLayout))
	return []byte(stamp), nil
}

func (t *KiotTime) UnmarshalJSON(data []byte) error {
	var datetime interface{}
	err := json.Unmarshal(data, &datetime)
	if err != nil {
		return err
	}

	items := reflect.ValueOf(datetime)
	switch items.Kind() {
	case reflect.String:
		parsedTime, err := time.Parse(KiotTimeLayout, items.String())
		if err != nil {
			return err
		}
		t.t = parsedTime
		return nil
	}

	return fmt.Errorf("must be a datetime in string format, got %s", string(data))
}

func (t *KiotTime) ToTime() time.Time {
	return t.t
}

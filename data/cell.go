package data

import (
	"encoding/json"
	"errors"
)

type Source string

const SourcePostgres Source = "Postgres"
const SourceBQ Source = "BigQuery"

type Cell struct {
	Source Source
	Value  interface{}
}

type Row struct {
	Values map[string]interface{}
}

type Rows struct {
	Rows []Row
}

type Column struct {
	Name      string
	DataType  string
	NullAble  bool
	IsPrimary bool
}

type MyType struct {
	Val interface{}
}

func (t *MyType) Scan(src interface{}) (err error) {
	var skills []string
	switch src.(type) {
	case string:
		err = json.Unmarshal([]byte(src.(string)), &skills)
	case []byte:
		err = json.Unmarshal(src.([]byte), &skills)
	default:
		return errors.New("incompatible type for Skills")
	}
	if err != nil {
		return
	}
	return nil
}

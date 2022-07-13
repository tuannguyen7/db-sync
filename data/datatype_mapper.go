package data

import (
	"cloud.google.com/go/bigquery"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
)

func PostgresDataTypeToBQ(dataType string) (bigquery.FieldType, error) {
	found, ok := postgresDataTypeToBQ[dataType]
	if ok {
		return found, nil
	}

	if strings.HasPrefix(dataType, "INT") {
		return bigquery.IntegerFieldType, nil
	}
	if strings.HasPrefix(dataType, "VARCHAR") {
		return bigquery.StringFieldType, nil
	}

	if strings.HasPrefix(dataType, "FLOAT") {
		return bigquery.FloatFieldType, nil
	}

	return bigquery.StringFieldType, fmt.Errorf("cannot map datatype %s from Postgre to BQ type", dataType)
}

func PostgresDataTypeToGo(dataType string) (driver.Valuer, error) {
	_, ok := postGresDataTypeToNullString[dataType]
	if ok {
		return new(sql.NullString), nil
	}

	_, ok = postGresDataTypeToNullBool[dataType]
	if ok {
		return new(sql.NullBool), nil
	}

	_, ok = postGresDataTypeToNullTime[dataType]
	if ok {
		return new(sql.NullTime), nil
	}

	if strings.HasPrefix(dataType, "INT") {
		return new(sql.NullInt64), nil
	}
	if strings.HasPrefix(dataType, "VARCHAR") {
		return new(sql.NullString), nil
	}

	if strings.HasPrefix(dataType, "FLOAT") {
		return new(sql.NullFloat64), nil
	}

	if strings.HasPrefix(dataType, "BOOL") {
		return new(sql.NullBool), nil
	}
	return nil, fmt.Errorf("cannot map datatype %s from Postgre to BQ type", dataType)
}

func KiotVietDataTypeToBQ(dataType string) (bigquery.FieldType, error) {
	found, ok := goDataTypeToBQ[dataType]
	if ok {
		return found, nil
	}

	if strings.HasPrefix(dataType, "int") {
		return bigquery.IntegerFieldType, nil
	}
	if strings.HasPrefix(dataType, "float") {
		return bigquery.FloatFieldType, nil
	}

	return bigquery.StringFieldType, fmt.Errorf("cannot map datatype %s from Postgre to BQ type", dataType)
}

var postgresDataTypeToBQ = map[string]bigquery.FieldType{
	"TEXT":        bigquery.StringFieldType,
	"UUID":        bigquery.StringFieldType,
	"TSVECTOR":    bigquery.StringFieldType,
	"_VARCHAR":    bigquery.StringFieldType,
	"JSONB":       bigquery.StringFieldType,
	"BOOL":        bigquery.BooleanFieldType,
	"TIMESTAMPTZ": bigquery.TimestampFieldType,
}

var postGresDataTypeToNullString = map[string]interface{}{
	"TEXT":     struct{}{},
	"UUID":     struct{}{},
	"TSVECTOR": struct{}{},
	"_VARCHAR": struct{}{},
	"JSONB":    struct{}{},
}

var postGresDataTypeToNullBool = map[string]interface{}{
	"BOOL": struct{}{},
}

var postGresDataTypeToNullTime = map[string]interface{}{
	"TIMESTAMPTZ": struct{}{},
}

var goDataTypeToBQ = map[string]bigquery.FieldType{
	"string": bigquery.StringFieldType,
	"int64":  bigquery.IntegerFieldType,
	"bool":   bigquery.BooleanFieldType,
	"float":  bigquery.FloatFieldType,
	"time":   bigquery.TimestampFieldType,
}

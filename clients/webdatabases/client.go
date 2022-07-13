package webdatabases

import (
	"context"
	"database/sql/driver"
	"db-sync/config"
	"db-sync/data"
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type Client struct {
	*sqlx.DB
}

func NewClient() (*Client, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		config.GiakhoPostgresUser, config.GiakhoPostgresPassword, config.GiakhoPostgresHost,
		config.GiakhoPostgresPort, config.GiakhoPostgresDb)

	db, err := sqlx.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return &Client{db}, nil
}

func (c *Client) GetTableInfo(ctx context.Context, tableName string) ([]data.Column, error) {
	var columns []data.Column
	cursor, err := c.QueryxContext(ctx, fmt.Sprintf(`SELECT * FROM "%s" LIMIT 1`, tableName))
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	columnNames, err := cursor.Columns()
	if err != nil {
		return nil, err
	}

	columnTypes, err := cursor.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for i, columnName := range columnNames {
		nullAble, ok := columnTypes[i].Nullable()
		if !ok {
			nullAble = true
		}
		c := data.Column{
			Name:     columnName,
			DataType: columnTypes[i].DatabaseTypeName(),
			NullAble: nullAble,
			From:     data.ColumnFromBQ,
		}
		columns = append(columns, c)
	}

	return columns, nil
}

func (c *Client) GetTotalRows(ctx context.Context, tableName string) (int64, error) {
	cursor, err := c.QueryxContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName))
	if err != nil {
		return 0, err
	}
	defer cursor.Close()

	var totalRows int64 = 0
	for cursor.Next() {
		if err := cursor.Scan(&totalRows); err != nil {
			return 0, err
		}
	}

	return totalRows, nil
}

func (c *Client) GetRows(ctx context.Context, tableName string, limit int64, offset int64) (*data.Rows, error) {
	cursor, err := c.QueryxContext(ctx, fmt.Sprintf(`SELECT * FROM "%s" LIMIT %d OFFSET %d`, tableName, limit, offset))
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	var rowValues []data.Row

	columnNames, err := cursor.Columns()
	if err != nil {
		return nil, err
	}

	columnTypes, err := cursor.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for cursor.Next() {
		var values []interface{}

		for _, colType := range columnTypes {
			v, err := data.PostgresDataTypeToGo(colType.DatabaseTypeName())
			if err != nil {
				return nil, err
			}

			values = append(values, v)
		}

		curRow := make(map[string]interface{})
		if err := cursor.Scan(values...); err != nil {
			return nil, err
		}

		for i, name := range columnNames {
			val, err := (values[i].(driver.Valuer)).Value()
			if err != nil {
				return nil, err
			}
			curRow[name] = val
		}
		rowValues = append(rowValues, data.Row{Values: curRow})
	}

	rows := &data.Rows{
		Rows: rowValues,
	}
	return rows, nil
}

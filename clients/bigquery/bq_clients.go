package biqueryclient

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"context"
	"db-sync/config"
	"db-sync/data"
	"time"
)

type Client struct {
	*bigquery.Client
}

func NewClient() (*Client, error) {
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, config.BqProjectId)
	if err != nil {
		return nil, err
	}

	return &Client{bqClient}, nil
}

// CreateSyncTimePartitionTable creates 2 table into mainDataset and preSyncDataset
// Tables in mainDataset will have _date field for partitioning
// Tables in preDataset will have _date field and _created at field. Records are daily streamed into tables in the preDataset
// then will be merged into table in the mainDataset
func (c *Client) CreateSyncTimePartitionTable(ctx context.Context, mainDataset string, preSyncDataset string, tableName string, columns []data.Column) error {
	schema := bigquery.Schema{&bigquery.FieldSchema{Name: "_date", Type: bigquery.DateFieldType}}
	coreSchema, err := c.convertColumnToSchema(columns)
	if err != nil {
		return err
	}

	schema = append(schema, coreSchema...)
	schema = append(schema)
	metadata := &bigquery.TableMetadata{
		TimePartitioning: &bigquery.TimePartitioning{
			Field: "_date",
		},
		Schema: schema,
	}

	tableRef := c.Dataset(mainDataset).Table(tableName)
	if err := tableRef.Create(ctx, metadata); err != nil {
		return err
	}

	// create table in the presync dataset
	schema = append(schema, &bigquery.FieldSchema{Name: "_created_at", Type: bigquery.TimestampFieldType})
	metadata = &bigquery.TableMetadata{
		TimePartitioning: &bigquery.TimePartitioning{
			Field:      "_date",
			Expiration: 7 * 24 * time.Hour, // expire after 7 days
		},
		Schema: schema,
	}
	preSyncTableRef := c.Dataset(preSyncDataset).Table(tableName)
	if err := preSyncTableRef.Create(ctx, metadata); err != nil {
		return err
	}

	return nil
}

func (c *Client) InsertOrUpdate(ctx context.Context, dataset string, tableName string, rows *data.Rows) error {
	table := c.Dataset(dataset).Table(tableName)
	metadata, err := table.Metadata(ctx)
	if err != nil {
		return err
	}

	schema := metadata.Schema
	inserter := table.Inserter()
	var vss []*bigquery.ValuesSaver

	for _, r := range rows.Rows {
		vss = append(vss, &bigquery.ValuesSaver{
			Schema:   schema,
			InsertID: bigquery.NoDedupeID,
			Row:      c.convertToValue(r.Values, schema),
		})
	}
	for _, vs := range vss {
		if err := inserter.Put(ctx, vs); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) convertToValue(row map[string]interface{}, schema bigquery.Schema) []bigquery.Value {
	var values []bigquery.Value
	for _, field := range schema {
		if field.Name == "_date" {
			values = append(values, civil.DateOf(time.Now()))
		} else if field.Name == "_created_at" {
			values = append(values, civil.DateTimeOf(time.Now()))
		} else {
			values = append(values, row[field.Name])
		}
	}
	return values
}

func (c *Client) convertColumnToSchema(columns []data.Column) (bigquery.Schema, error) {
	var schema bigquery.Schema
	for _, c := range columns {
		bqType, err := data.PostgresDataTypeToBQ(c.DataType)
		if err != nil {
			return nil, err
		}

		field := &bigquery.FieldSchema{
			Name:     c.Name,
			Required: !c.NullAble,
			Type:     bqType,
		}
		schema = append(schema, field)
	}
	return schema, nil
}

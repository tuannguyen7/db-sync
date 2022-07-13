package data

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
	From      ColumnFrom
}

type ColumnFrom string

const (
	ColumnFromBQ       ColumnFrom = "BigQuery"
	ColumnFromKiotViet ColumnFrom = "KiotViet"
)

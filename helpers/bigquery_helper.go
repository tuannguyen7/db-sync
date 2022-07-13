package helpers

import (
	"context"

	"cloud.google.com/go/bigquery"
)

func RunQuery(ctx context.Context, query *bigquery.Query) error {
	job, err := query.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	return status.Err()
}

package streaming

import (
	"context"
	bigqueryclient "db-sync/clients/bigquery"
	"db-sync/clients/webdatabases"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateQuery(t *testing.T) {
	assertor := assert.New(t)
	ctx := context.Background()
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

	streamingService := NewWebDBToBQStreaming(bqClient, dbClient, 0, nil)
	query, err := streamingService.generateMergeQuery(ctx, "POptions")
	assertor.Nil(err)
	fmt.Println(query)
}

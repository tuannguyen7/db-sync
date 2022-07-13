package kiotviet

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGetAccessToken(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c, err := NewClient()
	assert.Nil(err)

	token, err := c.getAccessToken(ctx)

	assert.Nil(err)
	assert.True(token.AccessToken != "")
}

func TestListTransfers(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c, err := NewClient()
	assert.Nil(err)

	page, err := c.ListTransfers(ctx, 100, 5)

	assert.Nil(err)
	assert.True(page.Total > 0)
}

func TestParseTime(t *testing.T) {
	//var ti time.Time
	timeStr := `2022-07-01T18:40:40.3530000`
	//timeBytes := []byte(timeStr)
	layout := "2006-01-02T15:04:05"
	parsedTime, err := time.Parse(layout, timeStr)
	//err := json.Unmarshal(timeBytes, &ti)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(parsedTime.String())
}

func TestGetTransferDetail(t *testing.T) {
	assert := assert.New(t)
	c, err := NewClient()
	ctx := context.Background()
	assert.Nil(err)
	transfer, err := c.GetTransferDetailWeb(ctx, 2583754)
	assert.Nil(err)
	assert.NotNil(transfer)
}

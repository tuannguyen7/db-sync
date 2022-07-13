package streaming

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateQueryKiotViet(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	s := NewKiotVietStreaming(nil, nil)
	query, err := s.generateMergeQuery(ctx, "kiotviet_transfers", transfersUpdateColumnNames)
	assert.Nil(err)
	fmt.Println(query)
}

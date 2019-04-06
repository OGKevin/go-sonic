package sonic

import (
	"context"
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSearchService_Query(t *testing.T) {
	type args struct {
		data   *Data
		offset int
		limit  int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		result int
	}{
		{
			name: "query no result",
			args: args{
				data: NewDataBuilder().Collection(uuid.NewV4().String()).Bucket(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
			},
			result: 0,
		},
		{
			name: "query 4 or 8 more result",
			// need to flush buckets before running tests
			args: args{
				data: NewDataBuilder().Collection("col1").Bucket("buc1").Text("magical").Build(),
			},
			result: 8,
		},
		{
			name: "query 1  result",
			// need to flush buckets before running tests
			args: args{
				data: NewDataBuilder().Collection("col1").Bucket("buc2").Text("magical").Build(),
			},
			result: 1,
		},
	}

	c, err := NewClientWithPassword("localhost:1491", "SecretPassword", context.Background())
	if !assert.NoError(t, err) {
		return
	}

	if !assert.True(t, beforePush4(t, c)) {
		return
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.SearchService.Query(tt.args.data, tt.args.offset, tt.args.limit)
			if !tt.wantErr && !assert.NoError(t, err) {
				return
			}

			if tt.wantErr && !assert.Error(t, err) {
				return
			}

			c := 0
			for range got {
				c++
			}

			if !assert.True(t, tt.result >= c, fmt.Sprintf("want %d got %d", tt.result, c)) {
				return
			}
		})
	}
}

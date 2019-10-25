package sonic

import (
	"context"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
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
		result  int
	}{
		{
			name: "query no result",
			args: args{
				data: NewDataBuilder().Collection(uuid.NewV4().String()).Bucket(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
			},
			result: 0,
		},
		{
			name: "query more then 1 result",
			// need to flush buckets before running tests
			args: args{
				data: NewDataBuilder().Collection("col2").Bucket("buc1").Text("magical").Build(),
			},
			result: 2,
		},
		{
			name: "query 1  result",
			// need to flush buckets before running tests
			args: args{
				data: NewDataBuilder().Collection("col2").Bucket("buc2").Text("magical").Build(),
			},
			result: 1,
		},
	}

	c, err := NewClientWithPassword("localhost:1491", PASSWORD, context.Background())
	if !assert.NoError(t, err) {
		return
	}

	if !assert.True(t, beforePush4(t, c)) {
		return
	}

	t.Parallel()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logrus.Debug("attempting reconnect")
			if !assert.NoError(t, c.reconnect(context.Background())) {
				return
			}
			logrus.Debug("reconnect finished")

			logrus.Debug("sending ping")
			if !assert.NoError(t, c.SearchService.Ping(context.Background())) {
				return
			}
			logrus.Debug("ping sent")

			ctx := context.Background()
			got, err := c.SearchService.Query(ctx, tt.args.data, tt.args.offset, tt.args.limit)
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

			if !assert.True(t, tt.result <= c, fmt.Sprintf("want %d got %d", tt.result, c)) {
				return
			}
		})
	}
}

func TestSearchService_Suggest(t *testing.T) {
	type args struct {
		data  *Data
		limit int
	}
	tests := []struct {
		name    string
		args    args
		want    chan string
		wantErr bool
	}{
		{
			name: "suggest with result",
			args: args{
				data:  NewDataBuilder().Collection("col2").Bucket("buc1").Text("som").Build(),
				limit: 0,
			},
		},
		{
			name: "suggest in empty bucket",
			args: args{
				data:  NewDataBuilder().Collection("test").Bucket(uuid.NewV4().String()).Text("awe").Build(),
				limit: 0,
			},
		},
	}

	c, err := NewClientWithPassword("localhost:1491", PASSWORD, context.Background())
	if !assert.NoError(t, err) {
		return
	}

	if !assert.True(t, beforePush4(t, c)) {
		return
	}

	t.Parallel()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !assert.NoError(t, c.reconnect(context.Background())) {
				return
			}

			if !assert.NoError(t, c.SearchService.Ping(context.Background())) {
				return
			}

			got, err := c.SearchService.Suggest(context.Background(), tt.args.data, tt.args.limit)
			if tt.wantErr && !assert.NoError(t, err) {
				return
			}

			for e := range got {
				if !assert.NotEqual(t, "", e) {
					return
				}
			}
		})
	}
}

package sonic

import (
	"context"
	"fmt"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

const (
	PASSWORD = "SecretPassword"
)

func TestIngestService_Count(t *testing.T) {
	c, err := NewClientWithPassword("localhost:1491", PASSWORD, context.Background())

	if !assert.NoError(t, err) {
		return
	}

	type fields struct {
		c *Client
	}
	type args struct {
		data []*Data
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			name:   "main",
			fields: fields{c: c},
			args: args{
				data: []*Data{
					NewDataBuilder().Collection("c").Bucket("b").Object(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
				},
			},
		},
		{
			name:   "2 objects",
			fields: fields{c: c},
			args: args{
				data: []*Data{
					NewDataBuilder().Collection("c").Bucket("b").Object(uuid.FromStringOrNil("568A42A3-7EBF-4243-94D7-7057122CDAF8").String()).Text(uuid.NewV4().String()).Build(),
					NewDataBuilder().Collection("c").Bucket("b").Object(uuid.FromStringOrNil("568A42A3-7EBF-4243-94D7-7057122CDAF8").String()).Text(uuid.NewV4().String()).Build(),
				},
			},
		},
	}

	t.Parallel()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			for _, x := range tt.args.data {
				ok, err := c.IngestService.Push(x)
				if !assert.NoError(t, err) {
					return
				}
				if !assert.True(t, ok) {
					return
				}
			}

			got, err := c.IngestService.Count(tt.args.data[0])
			if (err != nil) != tt.wantErr {
				assert.NoError(t, err)
				return
			}
			if !assert.True(t, len(tt.args.data)*5 <= got) {
				return
			}
		})
	}
}

func TestIngestService_Push(t *testing.T) {
	c, err := NewClientWithPassword("localhost:1491", PASSWORD, context.Background())
	if !assert.NoError(t, err) {
		return
	}

	type fields struct {
		c *Client
	}
	type args struct {
		data *Data
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "main",
			args: args{
				data: NewDataBuilder().Collection(uuid.NewV4().String()).Bucket(uuid.NewV4().String()).Object(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
			},
			fields: fields{
				c: c,
			},
			want: true,
		},
	}

	t.Parallel()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.c.IngestService.Push(tt.args.data)
			if !tt.wantErr && !assert.NoError(t, err) {
				return
			}

			if !assert.Equal(t, tt.want, got) {
				return
			}
		})
	}
}

func TestIngestService_Pop(t *testing.T) {
	c, err := NewClientWithPassword("localhost:1491", PASSWORD, context.Background())
	if !assert.NoError(t, err) {
		return
	}

	type fields struct {
		c *Client
	}
	type args struct {
		data *Data
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "nothing to pop",
			fields: fields{
				c: c,
			},
			args: args{
				data: NewDataBuilder().Collection(uuid.NewV4().String()).Bucket(uuid.NewV4().String()).Object(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
			},
			want: 0,
		},
	}

	t.Parallel()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.c.IngestService.Pop(tt.args.data)
			if !tt.wantErr && !assert.NoError(t, err) {
				return
			}

			if !assert.Equal(t, tt.want, got) {
				return
			}
		})
	}
}

func TestIngestService_Flush(t *testing.T) {
	c, err := NewClientWithPassword("localhost:1491", PASSWORD, context.Background())
	if !assert.NoError(t, err) {
		return
	}

	type fields struct {
		c *Client
	}
	type args struct {
		data *Data
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        int
		wantErr     bool
		beforeFunc  func(t *testing.T, c *Client) bool
		flushMethod func(data *Data) (int, error)
	}{
		{
			name: "flushc with unknown collection",
			fields: fields{
				c: c,
			},
			args: args{
				data: NewDataBuilder().Collection(uuid.NewV4().String()).Bucket(uuid.NewV4().String()).Object(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
			},
			want:        0,
			flushMethod: c.IngestService.Flushc,
		},
		{
			name: "flushc with 4 pushed items to 1 col",
			fields: fields{
				c: c,
			},
			args: args{
				data: NewDataBuilder().Collection("col1").Bucket(uuid.NewV4().String()).Object(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
			},
			want:        1,
			beforeFunc:  beforePush4,
			flushMethod: c.IngestService.Flushc,
		},
		{
			name: "flushb with unknown collection",
			fields: fields{
				c: c,
			},
			args: args{
				data: NewDataBuilder().Collection("col1").Bucket(uuid.NewV4().String()).Object(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
			},
			want:        0,
			flushMethod: c.IngestService.Flushb,
		},
		{
			name: "flushb with 4 pushed items to 1 collection and bucket",
			fields: fields{
				c: c,
			},
			args: args{
				data: NewDataBuilder().Collection("col1").Bucket("buc1").Object(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
			},
			want:        34,
			beforeFunc:  beforePush4,
			flushMethod: c.IngestService.Flushb,
		},
		{
			name: "flusho with unknown collection",
			fields: fields{
				c: c,
			},
			args: args{
				data: NewDataBuilder().Collection(uuid.NewV4().String()).Bucket(uuid.NewV4().String()).Object(uuid.NewV4().String()).Text(uuid.NewV4().String()).Build(),
			},
			want:        0,
			flushMethod: c.IngestService.Flusho,
		},
		{
			name: "flusho with 4 pushed items to 1 collection, bucket and object",
			fields: fields{
				c: c,
			},
			args: args{
				data: NewDataBuilder().Collection("col1").Bucket("buc2").Object("obj1").Text(uuid.NewV4().String()).Build(),
			},
			want:        41,
			beforeFunc:  beforePush4,
			flushMethod: c.IngestService.Flusho,
		},
	}

	t.Parallel()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.beforeFunc != nil {
				if !assert.True(t, tt.beforeFunc(t, tt.fields.c)) {
					return
				}
			}

			got, err := tt.flushMethod(tt.args.data)
			if !assert.NoError(t, err) != tt.wantErr {
				return
			}
			if !assert.True(t, tt.want+5 > got, fmt.Sprintf("want %d, got %d", tt.want+5, got)) {
				return
			}
		})
	}
}

func beforePush4(t *testing.T, c *Client) bool {
	for i := 0; i < 4; i++ {
		_, err := c.IngestService.Push(NewDataBuilder().Collection("col1").Bucket("buc1").Object(uuid.NewV4().String()).Text(fmt.Sprintf("some magical text -- %s", uuid.NewV4())).Build())
		if !assert.NoError(t, err) {
			return false
		}
		_, err = c.IngestService.Push(NewDataBuilder().Collection("col2").Bucket("buc1").Object(uuid.NewV4().String()).Text(fmt.Sprintf("some magical text -- %s", uuid.NewV4())).Build())
		if !assert.NoError(t, err) {
			return false
		}
		_, err = c.IngestService.Push(NewDataBuilder().Collection("col1").Bucket("buc2").Object("obj1").Text(fmt.Sprintf("some magical text -- %s", uuid.NewV4())).Build())
		if !assert.NoError(t, err) {
			return false
		}
		_, err = c.IngestService.Push(NewDataBuilder().Collection("col2").Bucket("buc2").Object("obj1").Text(fmt.Sprintf("some magical text -- %s", uuid.NewV4())).Build())
		if !assert.NoError(t, err) {
			return false
		}
	}

	return true
}

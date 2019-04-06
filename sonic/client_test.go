package sonic

import (
	"context"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestClient_Close(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "main",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewClientWithPassword("localhost:1491", "SecretPassword", context.Background())
			if !assert.NoError(t, err) {
				return
			}

			err = c.Close()

			if !tt.wantErr && !assert.NoError(t, err) {
				return
			}

			if tt.wantErr && !assert.Error(t, err) {
				return
			}
		})
	}
}

func ExampleNewClientWithPassword() {
	c, err := NewClientWithPassword("localhost:1491", "SecretPassword", context.Background())
	if err != nil {
		panic(err)
	}

	// Ingest
	_, err = c.IngestService.Push(NewDataBuilder().Text("my string").Bucket("my bucket").Collection("my collection").Object("my object").Build())
	if err != nil {
		panic(err)
	}

	// Search
	ch, err := c.SearchService.Query(NewDataBuilder().Collection("my collection").Bucket("my bucket").Text("my string").Build(), 0, 0)
	for e := range ch {
		log.Println(e)
	}
}

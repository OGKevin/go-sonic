package sonic

import (
	"context"
	"github.com/stretchr/testify/assert"
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

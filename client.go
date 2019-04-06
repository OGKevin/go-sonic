package sonic

import (
	"context"
	"github.com/pkg/errors"
	"io"
	"net"
	"time"
)

type Data struct {
	Collection string
	Bucket     string
	Object     string
	Text       string
}

type Client struct {
	s net.Conn
	i net.Conn

	IngestService *IngestService
	SearchService *SearchService
}

func (c *Client) SetDeadline(t time.Time) error {
	if err := c.s.SetDeadline(t); err != nil {
		return errors.Wrap(err, "could not set deadline for search connection")
	}

	if err := c.i.SetDeadline(t); err != nil {
		return errors.Wrap(err, "could not set deadline for ingest connection")
	}

	return nil
}

func (c *Client) SetReadDeadline(t time.Time) error {
	if err := c.s.SetReadDeadline(t); err != nil {
		return errors.Wrap(err, "could not set read deadline for search connection")
	}

	if err := c.i.SetReadDeadline(t); err != nil {
		return errors.Wrap(err, "could not set read deadline for ingest connection")
	}

	return nil
}

func (c *Client) SetWriteDeadline(t time.Time) error {
	if err := c.s.SetWriteDeadline(t); err != nil {
		return errors.Wrap(err, "could not set write deadline for search connection")
	}

	if err := c.i.SetWriteDeadline(t); err != nil {
		return errors.Wrap(err, "could not set write deadline for ingest connection")
	}

	return nil
}

func (c *Client) Close() error {
	_, err := io.WriteString(c.s, "QUIT\n")
	if err != nil {
		return errors.Wrap(err, "could not signal server to close search connection")
	}

	err = c.s.Close()
	if err != nil {
		return errors.Wrap(err, "could not close search connection")
	}

	_, err = io.WriteString(c.i, "QUIT\n")
	if err != nil {
		return errors.Wrap(err, "could not signal server to close ingest connection")
	}

	err = c.i.Close()
	if err != nil {
		return errors.Wrap(err, "could not close ingest connection")
	}

	return nil
}

func NewClientWithPassword(address, password string, ctx context.Context) (*Client, error) {
	i, err := net.Dial("tcp", address)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open connection to %q", address)
	}

	s, err := net.Dial("tcp", address)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open connection to %q", address)
	}

	client := Client{i: i, s: s}
	client.IngestService, err = newIngestService(&client, password)
	if err != nil {
		return nil, errors.Wrap(err, "could not create ingest service")
	}

	client.SearchService, err = newSearchService(&client, password, ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not create search service")
	}

	return &client, nil
}

// Data builder pattern code
type DataBuilder struct {
	data *Data
}

func NewDataBuilder() *DataBuilder {
	data := &Data{}
	b := &DataBuilder{data: data}
	return b
}

func (b *DataBuilder) Collection(collection string) *DataBuilder {
	b.data.Collection = collection
	return b
}

func (b *DataBuilder) Bucket(bucket string) *DataBuilder {
	b.data.Bucket = bucket
	return b
}

func (b *DataBuilder) Object(object string) *DataBuilder {
	b.data.Object = object
	return b
}

func (b *DataBuilder) Text(text string) *DataBuilder {
	b.data.Text = text
	return b
}

func (b *DataBuilder) Build() *Data {
	return b.data
}


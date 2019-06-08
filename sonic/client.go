package sonic

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"io"
	"net"
	"sync"
	"time"
)

// Data is a generic struct that can be used for all the "queries"
type Data struct {
	Collection string
	Bucket     string
	Object     string
	Text       string
}

type Client struct {
	s net.Conn
	i net.Conn

	ctx context.Context

	address  string
	password string

	IngestService IngestService
	SearchService SearchService
}

// SetDeadline sets the read and write deadlines associated
// with the connection. It is equivalent to calling both
// SetReadDeadline and SetWriteDeadline.
//
// A deadline is an absolute time after which I/O operations
// fail with a timeout (see type Error) instead of
// blocking. The deadline applies to all future and pending
// I/O, not just the immediately following call to Read or
// Write. After a deadline has been exceeded, the connection
// can be refreshed by setting a deadline in the future.
//
// An idle timeout can be implemented by repeatedly extending
// the deadline after successful Read or Write calls.
//
// A zero value for t means I/O operations will not time out.
func (c *Client) SetDeadline(t time.Time) error {
	if err := c.s.SetDeadline(t); err != nil {
		return errors.Wrap(err, "could not set deadline for search connection")
	}

	if err := c.i.SetDeadline(t); err != nil {
		return errors.Wrap(err, "could not set deadline for ingest connection")
	}

	return nil
}

// SetReadDeadline sets the deadline for future Read calls
// and any currently-blocked Read call.
// A zero value for t means Read will not time out.
func (c *Client) SetReadDeadline(t time.Time) error {
	if err := c.s.SetReadDeadline(t); err != nil {
		return errors.Wrap(err, "could not set read deadline for search connection")
	}

	if err := c.i.SetReadDeadline(t); err != nil {
		return errors.Wrap(err, "could not set read deadline for ingest connection")
	}

	return nil
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
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

	client.password = password
	client.address = address
	client.ctx = ctx

	client.IngestService, err = newIngestService(ctx, &client)
	if err != nil {
		return nil, errors.Wrap(err, "could not create ingest service")
	}

	client.SearchService, err = newSearchService(ctx, &client)
	if err != nil {
		return nil, errors.Wrap(err, "could not create search service")
	}

	return &client, nil
}

func NewNoOpsClient(ctx context.Context) *Client {
	return &Client{
		ctx: ctx,
		IngestService: &NoOpsIngestService{},
		SearchService: &NoOpsSearchService{},
	}
}

func (c *Client) reconnect(ctx context.Context) error {
	sp, ctx := opentracing.StartSpanFromContext(ctx, "sonic-reconnect")
	defer sp.Finish()

	var err error

	c.i, err = net.Dial("tcp", c.address)
	if err != nil {
		return errors.Wrapf(err, "could not open connection to %q", c.address)
	}

	c.s, err = net.Dial("tcp", c.address)
	if err != nil {
		return errors.Wrapf(err, "could not open connection to %q", c.address)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)

	go func() {
		defer wg.Done()

		err := c.IngestService.connect(ctx)
		if err != nil {
			errCh <- errors.Wrap(err, "could not create ingest service")
		}
	}()

	go func() {
		defer wg.Done()

		err := c.SearchService.connect(ctx)
		if err != nil {
			errCh <- errors.Wrap(err, "could not create search service")
		}
	}()

	wg.Wait()
	close(errCh)

	err = <- errCh
	if err != nil {
		return errors.Wrap(err, "could not reconnect to sonic")
	}

	return nil
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

// Collection see https://github.com/valeriansaliou/sonic/blob/master/PROTOCOL.md for terminology explanation
func (b *DataBuilder) Collection(collection string) *DataBuilder {
	b.data.Collection = collection
	return b
}

// Bucket see https://github.com/valeriansaliou/sonic/blob/master/PROTOCOL.md for terminology explanation
func (b *DataBuilder) Bucket(bucket string) *DataBuilder {
	b.data.Bucket = bucket
	return b
}

// Object see https://github.com/valeriansaliou/sonic/blob/master/PROTOCOL.md for terminology explanation
func (b *DataBuilder) Object(object string) *DataBuilder {
	b.data.Object = object
	return b
}

// Text or Word see https://github.com/valeriansaliou/sonic/blob/master/PROTOCOL.md for terminology explanation
func (b *DataBuilder) Text(text string) *DataBuilder {
	b.data.Text = text
	return b
}

func (b *DataBuilder) Build() *Data {
	return b.data
}

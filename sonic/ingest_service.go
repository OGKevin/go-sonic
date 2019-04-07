package sonic

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"strconv"
	"sync"
)

// IngestService exposes the ingest mode of sonic
type IngestService struct {
	c *Client

	l sync.Mutex
	s *bufio.Scanner
}

func newIngestService(c *Client, password string) (*IngestService, error) {
	s := bufio.NewScanner(c.i)

	_, err := io.WriteString(c.i, fmt.Sprintf("START ingest %s\n", password))
	if err != nil {
		return nil, errors.Wrap(err, "could not start ingest connection")
	}

	s.Scan()

parse:
	w := bufio.NewScanner(bytes.NewBuffer(s.Bytes()))
	w.Split(bufio.ScanWords)
	w.Scan()

	switch w.Text() {
	case "STARTED":
	case "CONNECTED":
		s.Scan()
		goto parse
	case "ENDED":
		return nil, errors.Errorf("failed to start ingest session: %q", s.Text())
	default:
		return nil, errors.Errorf("could not determine how to interpret %q response", s.Text())
	}

	return &IngestService{c: c, s: s}, nil
}

// Push search data in the index
func (i *IngestService) Push(data *Data) (bool, error) {
	if data.Collection == "" || data.Bucket == "" || data.Object == "" || data.Text == "" {
		return false, errors.New("all ingest data are required for pushing")
	}

	i.l.Lock()
	defer i.l.Unlock()
	_, err := io.WriteString(i.c.i, fmt.Sprintf("PUSH %s %s %s %q\n", data.Collection, data.Bucket, data.Object, data.Text))
	if err != nil {
		return false, errors.Wrap(err, "pushing data failed")
	}

	i.s.Scan()

	w := bufio.NewScanner(bytes.NewBuffer(i.s.Bytes()))
	w.Split(bufio.ScanWords)
	w.Scan()

	switch w.Text() {
	case "OK":
	default:
		return false, errors.Errorf("could not determine how to interpret response: %q", i.s.Text())
	}

	return true, nil
}

// Pop search data from the index
func (i *IngestService) Pop(data *Data) (int, error) {
	if data.Collection == "" || data.Bucket == "" || data.Object == "" || data.Text == "" {
		return 0, errors.New("all ingest data are required for pushing")
	}

	i.l.Lock()
	defer i.l.Unlock()
	_, err := io.WriteString(i.c.i, fmt.Sprintf("POP %s %s %s %q\n", data.Collection, data.Bucket, data.Object, data.Text))
	if err != nil {
		return 0, errors.Wrap(err, "popping data failed")
	}

	i.s.Scan()
	w := bufio.NewScanner(bytes.NewBuffer(i.s.Bytes()))
	w.Split(bufio.ScanWords)
	w.Scan()

	switch w.Text() {
	case "RESULT":
	default:
		return 0, errors.Errorf("could not determine how to interpret %q", i.s.Text())
	}

	w.Scan()
	c, err := strconv.Atoi(w.Text())
	if err != nil {
		return 0, errors.Wrapf(err, "could not parse count result to int: %q", i.s.Text())
	}

	return c, nil
}

// Count indexed search data
func (i *IngestService) Count(data *Data) (int, error) {
	if data.Collection == "" {
		return 0, errors.New("collection can not be an empty string")
	}

	args := []interface{}{data.Collection}
	sfmt := "COUNT %s"

	if data.Bucket != "" {
		sfmt += " %s"
		args = append(args, data.Bucket)
	}

	if data.Object != "" {
		sfmt += " %s"
		args = append(args, data.Object)
	}

	i.l.Lock()
	defer i.l.Unlock()
	_, err := io.WriteString(i.c.i, fmt.Sprintf(fmt.Sprintf("%s\n", sfmt), args...))
	if err != nil {
		return 0, errors.Wrap(err, "popping data failed")
	}

	if !i.s.Scan() {
		return 0, errors.Wrap(i.s.Err(), "could not scan count response from server")
	}

	s := bufio.NewScanner(bytes.NewBuffer(i.s.Bytes()))
	s.Split(bufio.ScanWords)
	if !s.Scan() {
		return 0, errors.New("could not scan result")
	}

	switch s.Text() {
	case "RESULT":
	default:
		return 0, errors.Errorf("could not determine how to interpret %q", i.s.Text())
	}

	s.Scan()

	c, err := strconv.Atoi(s.Text())
	if err != nil {
		return 0, errors.Wrap(err, "could not parse count result to int")
	}

	return c, nil
}

func (i *IngestService) flush(query string) (int, error) {
	i.l.Lock()
	defer i.l.Unlock()
	_, err := io.WriteString(i.c.i, query)
	if err != nil {
		return 0, errors.Wrap(err, "flushing collection failed")
	}

	if !i.s.Scan() {
		return 0, errors.Wrap(i.s.Err(), "could not scan count response from server")
	}

	w := bufio.NewScanner(bytes.NewBuffer(i.s.Bytes()))
	w.Split(bufio.ScanWords)
	w.Scan()

	switch w.Text() {
	case "RESULT":
	default:
		return 0, errors.Errorf("could not determine how to interpret %q", i.s.Text())
	}

	w.Scan()

	c, err := strconv.Atoi(w.Text())
	if err != nil {
		return 0, errors.Wrap(err, "could not parse count result to int")
	}

	return c, nil
}

// Flushc Flush all indexed data from a collection
func (i *IngestService) Flushc(data *Data) (int, error) {
	if data.Collection == "" {
		return 0, errors.New("collection can not be an empty string")
	}

	return i.flush(fmt.Sprintf("FLUSHC %s\n", data.Collection))
}

// Flushb Flush all indexed data from a bucket in a collection
func (i *IngestService) Flushb(data *Data) (int, error) {
	if data.Collection == "" || data.Bucket == "" {
		return 0, errors.New("collection and bucket can not be an empty strings")
	}

	return i.flush(fmt.Sprintf("FLUSHB %s %s\n", data.Collection, data.Bucket))
}

// Flusho Flush all indexed data from an object in a bucket in collection
func (i *IngestService) Flusho(data *Data) (int, error) {
	if data.Collection == "" || data.Bucket == "" || data.Object == "" {
		return 0, errors.New("collection, bucket and object can not be an empty strings")
	}

	return i.flush(fmt.Sprintf("FLUSHO %s %s %s\n", data.Collection, data.Bucket, data.Object))
}

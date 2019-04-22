package sonic

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"net"
	"sync"
	"syscall"
	"time"
)

// SearchService exposes the search mode of sonic
type SearchService struct {
	c *Client

	sl sync.RWMutex
	s  *bufio.Scanner

	pl      sync.RWMutex
	pending map[string]chan string

	onePoll sync.Once

	ctx context.Context
}

func newSearchService(c *Client) (*SearchService, error) {
	ss := &SearchService{c: c, pending: make(map[string]chan string), ctx: c.ctx}

	err := ss.connect()
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to search service")
	}

	go ss.keepAlive()

	return ss, nil
}

func (s *SearchService) connect() error {
	s.sl.Lock()
	defer s.sl.Unlock()

	scanner := bufio.NewScanner(s.c.s)
	s.s = scanner

	_, err := io.WriteString(s.c.s, fmt.Sprintf("START search %s\n", s.c.password))
	if err != nil {
		return errors.Wrap(err, "could not start search connection")
	}

	s.s.Scan()

parse:
	w := bufio.NewScanner(bytes.NewBuffer(s.s.Bytes()))
	w.Split(bufio.ScanWords)
	w.Scan()

	switch w.Text() {
	case "STARTED":
	case "CONNECTED":
		s.s.Scan()
		goto parse
	case "ENDED":
		return errors.Errorf("failed to start search session: %q", s.s.Text())
	default:
		return errors.Errorf("could not determine how to interpret %q response", s.s.Text())
	}

	return nil
}

func (s *SearchService) keepAlive() {
	ticker := time.Tick(time.Second * 5)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker:
			err := s.Ping()
			s.pollForEvents()
			if err != nil {
				log.Print(err.Error())
			}
		}
	}
}

func (s *SearchService) pollForEvents() {
	s.onePoll.Do(func() {
		t := time.NewTicker(time.Millisecond * 100)
		go func() {
			defer t.Stop()
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-t.C:
					s.sl.RLock()
					if !s.s.Scan() {
						s.sl.RUnlock()
						continue
					}

					w := bufio.NewScanner(bytes.NewBuffer(s.s.Bytes()))
					w.Split(bufio.ScanWords)
					w.Scan()

					switch w.Text() {
					case "EVENT":
						go s.handleEvent(s.s.Text())
					case "", "PONG":
						// do nothing
					default:
						log.Panicf("event poller managed to get/intercept a non event response: %q", s.s.Text())
					}

					s.sl.RUnlock()
				}
			}
		}()
	})
}

// Suggest  auto-completes word
func (s *SearchService) Suggest(data *Data, limit int) (chan string, error) {
	if data.Collection == "" || data.Bucket == "" {
		return nil, errors.New("collection and bucket should not be empty for suggest")
	}

	query := fmt.Sprintf("SUGGEST %s %s %q", data.Collection, data.Bucket, data.Text)

	if limit != 0 {
		query += fmt.Sprintf(" LIMIT(%d)", limit)
	}

	s.sl.Lock()
	defer s.sl.Unlock()
	_, err := io.WriteString(s.c.s, fmt.Sprintf("%s\n", query))
	if err != nil {
		return nil, errors.Wrap(err, "querying data for suggestion failed")
	}

	ch, err := s.parseResponse()
	if err != nil {
		return nil, errors.Wrap(err, "could not parse response for suggest")
	}

	return ch, nil
}

func (s *SearchService) Ping() error {
	reconnect := false

	s.sl.Lock()
ping:

	_, err := io.WriteString(s.c.s, fmt.Sprintf("%s\n", "PING"))
	if err != nil {
		s.sl.Unlock()
		if err, ok := err.(*net.OpError); ok && err.Err == syscall.EPIPE && !reconnect {
			reconnect = true
			err := s.c.reconnect()
			if err != nil {
				return errors.Wrap(err, "could not reconnect to sonic")
			}
			goto ping
		}
		return errors.Wrap(err, "pinging sonic failed")
	}
	s.sl.Unlock()

	return nil
}

// Query query database
func (s *SearchService) Query(data *Data, offset, limit int) (chan string, error) {
	if data.Collection == "" || data.Bucket == "" {
		return nil, errors.New("collection and bucket should not be empty for query")
	}

	query := fmt.Sprintf("QUERY %s %s %q", data.Collection, data.Bucket, data.Text)

	if offset != 0 {
		query += fmt.Sprintf(" OFFSET(%d)", offset)
	}

	if limit != 0 {
		query += fmt.Sprintf(" LIMIT(%d)", limit)
	}

	s.sl.Lock()
	defer s.sl.Unlock()
	_, err := io.WriteString(s.c.s, fmt.Sprintf("%s\n", query))
	if err != nil {
		return nil, errors.Wrap(err, "querying data failed")
	}

	ch, err := s.parseResponse()
	if err != nil {
		return nil, errors.Wrap(err, "could not parse response for query")
	}

	return ch, nil
}

func (s *SearchService) parseResponse() (chan string, error) {
scan:
	s.s.Scan()

	w := bufio.NewScanner(bytes.NewBuffer(s.s.Bytes()))
	w.Split(bufio.ScanWords)
	w.Scan()

	switch w.Text() {
	case "PENDING":
		ch := make(chan string)

		w.Scan()

		s.pl.Lock()
		defer s.pl.Unlock()
		s.pending[w.Text()] = ch

		s.pollForEvents()

		return ch, nil
	case "EVENT":
		// in case we intercept an event
		go s.handleEvent(s.s.Text())
		fallthrough
	case "", "PONG":
		goto scan
	default:
		return nil, errors.Errorf("could not determine how to interpret response: %q", s.s.Text())
	}
}

func (s *SearchService) handleEvent(event string) {
	w := bufio.NewScanner(bytes.NewBufferString(event))
	w.Split(bufio.ScanWords)
	w.Scan()
	w.Scan()

	switch w.Text() {
	case "QUERY", "SUGGEST":
		w.Scan()
		s.pl.RLock()
		defer s.pl.RUnlock()
		ch := s.pending[w.Text()]
		defer close(ch)

		for w.Scan() {
			ch <- w.Text()
		}
	default:
		log.Panicf("could not determine how to interpret event: %q", event)
	}
}

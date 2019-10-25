package sonic

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"sync"
	"time"
)

type pendingQuery struct {
	ids chan string
}

type SearchService interface {
	Suggest(ctx context.Context, data *Data, limit int) (chan string, error)
	Ping(ctx context.Context) error
	Query(ctx context.Context, data *Data, offset, limit int) (chan string, error)

	connect(ctx context.Context) error
}

// NoOpsSearchService Is a search service that preforms no operations.
type NoOpsSearchService struct {
}

func (*NoOpsSearchService) connect(ctx context.Context) error {
	return nil
}

func (*NoOpsSearchService) Suggest(ctx context.Context, data *Data, limit int) (chan string, error) {
	ch := make(chan string)
	close(ch)

	return ch, nil
}

func (*NoOpsSearchService) Ping(ctx context.Context) error {
	return nil
}

func (*NoOpsSearchService) Query(ctx context.Context, data *Data, offset, limit int) (chan string, error) {
	ch := make(chan string)
	close(ch)

	return ch, nil
}

// searchService exposes the search mode of sonic
type searchService struct {
	c *Client

	sl sync.Mutex
	s  *bufio.Scanner

	pl      sync.RWMutex
	pending map[string]*pendingQuery

	onePoll sync.Once

	ctx context.Context
}

func newSearchService(ctx context.Context, c *Client) (SearchService, error) {
	ss := &searchService{c: c, pending: make(map[string]*pendingQuery), ctx: c.ctx}

	err := ss.connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to search service")
	}

	ss.keepAlive()

	return ss, nil
}

func (s *searchService) connect(ctx context.Context) error {
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

func (s *searchService) keepAlive() {
	go func() {
		ticker := time.Tick(time.Second * 5)
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker:
				func() {
					logrus.Debugf("starting span sonic-keepAlive")
					ctx := context.Background()
					logrus.Debugf("sending ping from keep alive")
					err := s.Ping(ctx)
					logrus.Debugf("ping sent")

					s.pollForEvents()
					if err != nil {
						logrus.WithError(err).Error("error while pinging sonic")
					}
				}()
			}
		}
	}()
}

func (s *searchService) pollForEvents() {
	s.onePoll.Do(func() {
		t := time.NewTicker(time.Millisecond * 100)
		go func() {
			defer t.Stop()
			for {
				select {
				case <-s.ctx.Done():
					return
				case <-t.C:
					func() {
						s.pl.RLock()
						shouldPoll := len(s.pending) > 0
						s.pl.RUnlock()
						if !shouldPoll {
							return
						}

						ctx := context.Background()

						logrus.Debug("event poller getting lock")
						s.sl.Lock()
						logrus.Debug("event poller got lock")
						defer func() {
							defer s.sl.Unlock()
							logrus.Debug("event poller releasing lock")
						}()

						scanned := s.s.Scan()
						if !scanned {
							return
						}

						w := bufio.NewScanner(bytes.NewBuffer(s.s.Bytes()))
						w.Split(bufio.ScanWords)
						w.Scan()

						switch w.Text() {
						case "EVENT":
							go s.handleEvent(ctx, s.s.Text())
						case "", "PONG":
							// do nothing
						default:
							log.Panicf("event poller managed to get/intercept a non event response: %q", s.s.Text())
						}
					}()
				}
			}
		}()
	})
}

// Suggest  auto-completes word
func (s *searchService) Suggest(ctx context.Context, data *Data, limit int) (chan string, error) {
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

	ch, err := s.parseResponse(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse response for suggest")
	}

	return ch, nil
}

func (s *searchService) Ping(ctx context.Context) error {
	reconnect := false
	s.sl.Lock()
	defer s.sl.Unlock()

ping:
	_, err := io.WriteString(s.c.s, fmt.Sprintf("%s\n", "PING"))
	if err != nil {
		if err, ok := err.(*net.OpError); ok && !reconnect {
			if _, ok := err.Err.(*os.SyscallError); ok && !reconnect {
				reconnect = true
				err := s.c.reconnect(ctx)
				if err != nil {
					return errors.Wrap(err, "could not reconnect to sonic")
				}
				goto ping
			}
		}
		return errors.Wrap(err, "pinging sonic failed")
	}

	return nil
}

// Query query database
func (s *searchService) Query(ctx context.Context, data *Data, offset, limit int) (chan string, error) {
	logrus.Debug("preforming query")
	defer logrus.Debug("done performing query")

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

	logrus.Debug("getting lock")
	s.sl.Lock()
	logrus.Debug("got lock")
	defer func() {
		defer s.sl.Unlock()
		logrus.Debug("releasing lock")
	}()

	logrus.Debug("writing to tcp connection")
	n, err := io.WriteString(s.c.s, fmt.Sprintf("%s\n", query))
	logrus.Debugf("written %d bytes", n)
	if err != nil {
		return nil, errors.Wrap(err, "querying data failed")
	}

	ch, err := s.parseResponse(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse response for query")
	}

	return ch, nil
}

func (s *searchService) parseResponse(ctx context.Context) (chan string, error) {
	logrus.Debug("parsing response")
	defer logrus.Debug("done parsing response")

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

		s.pollForEvents()

		s.pending[w.Text()] = &pendingQuery{ids: ch}

		return ch, nil
	case "EVENT":
		// in case we intercept an event
		go s.handleEvent(ctx, s.s.Text())
		fallthrough
	case "", "PONG":
		goto scan
	default:
		return nil, errors.Errorf("could not determine how to interpret response: %q", s.s.Text())
	}
}

func (s *searchService) handleEvent(ctx context.Context, event string) {
	w := bufio.NewScanner(bytes.NewBufferString(event))
	w.Split(bufio.ScanWords)
	w.Scan()
	w.Scan()

	switch w.Text() {
	case "QUERY", "SUGGEST":
		w.Scan()
		s.pl.RLock()
		defer s.pl.RUnlock()
		pending := s.pending[w.Text()]
		defer close(pending.ids)

		for w.Scan() {
			pending.ids <- w.Text()
		}
	default:
		log.Panicf("could not determine how to interpret event: %q", event)
	}
}

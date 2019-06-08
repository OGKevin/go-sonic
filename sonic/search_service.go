package sonic

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/opentracing/opentracing-go"
	opLog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"net"
	"os"

	"sync"
	"time"
)

type pendingQuery struct {
	ids chan string
	sp  opentracing.Span
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
	sp, ctx := opentracing.StartSpanFromContext(ctx, "sonic-newSearchService")
	defer sp.Finish()

	ss := &searchService{c: c, pending: make(map[string]*pendingQuery), ctx: c.ctx}

	err := ss.connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to search service")
	}

	ss.keepAlive()

	return ss, nil
}

func (s *searchService) connect(ctx context.Context) error {
	sp, ctx := opentracing.StartSpanFromContext(ctx, "sonic-search-connect")
	defer sp.Finish()

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
					sp, ctx := opentracing.StartSpanFromContext(context.Background(), "sonic-keepAlive")
					defer sp.Finish()

					logrus.Debugf("sending ping from keep alive")
					err := s.Ping(ctx)
					logrus.Debugf("ping sent")

					s.pollForEvents()
					if err != nil {
						sp.LogFields(opLog.Error(err))
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

						sp, ctx := opentracing.StartSpanFromContext(context.Background(), "sonic-pollForEvents")
						defer sp.Finish()

						lsp, _ := opentracing.StartSpanFromContext(ctx, "acquiring lock")
						lsp.SetTag("line", "sonic/search_service.go:115")
						logrus.Debug("event poller getting lock")
						s.sl.Lock()
						logrus.Debug("event poller got lock")
						lsp.Finish()
						defer func() {
							defer s.sl.Unlock()
							logrus.Debug("event poller releasing lock")
						}()

						ssp, _ := opentracing.StartSpanFromContext(ctx, "scanning main scanner")
						scanned := s.s.Scan()
						ssp.LogFields(opLog.Bool("scanned", scanned))
						ssp.Finish()

						sp.SetTag("scanned", scanned)

						if !scanned {
							return
						}

						sp.LogFields(opLog.String("scanned value", s.s.Text()))

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
	sp, ctx := opentracing.StartSpanFromContext(ctx, "sonic-Suggest")
	defer sp.Finish()

	if data.Collection == "" || data.Bucket == "" {
		return nil, errors.New("collection and bucket should not be empty for suggest")
	}

	query := fmt.Sprintf("SUGGEST %s %s %q", data.Collection, data.Bucket, data.Text)

	if limit != 0 {
		query += fmt.Sprintf(" LIMIT(%d)", limit)
	}

	lsp := sp.Tracer().StartSpan("acquiring lock", opentracing.ChildOf(sp.Context()))
	s.sl.Lock()
	lsp.Finish()
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
	sp, ctx := opentracing.StartSpanFromContext(ctx, "sonic-Ping")
	defer sp.Finish()

	reconnect := false
	lsp, _ := opentracing.StartSpanFromContext(ctx, "acquiring lock")
	s.sl.Lock()
	defer s.sl.Unlock()
	lsp.Finish()

ping:
	_, err := io.WriteString(s.c.s, fmt.Sprintf("%s\n", "PING"))
	if err != nil {
		if err, ok := err.(*net.OpError); ok && !reconnect {
			if _, ok := err.Err.(*os.SyscallError); ok && !reconnect {
				sp.LogFields(opLog.Bool("reconnect", true))
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

	ssp, _ := opentracing.StartSpanFromContext(ctx, "scanning main scanner")
	ssp.SetTag("scanned", s.s.Scan())
	ssp.LogFields(opLog.String("scanned", s.s.Text()))
	ssp.Finish()
	sp.LogFields(opLog.Bool("reconnect", false))

	return nil
}

// Query query database
func (s *searchService) Query(ctx context.Context, data *Data, offset, limit int) (chan string, error) {
	logrus.Debug("preforming query")
	defer logrus.Debug("done performing query")

	sp, ctx := opentracing.StartSpanFromContext(ctx, "sonic-Query")
	defer sp.Finish()

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

	lsp := sp.Tracer().StartSpan("acquiring lock", opentracing.ChildOf(sp.Context()))
	lsp.SetTag("line", "sonic/search_service.go:222")
	logrus.Debug("getting lock")
	s.sl.Lock()
	logrus.Debug("got lock")
	lsp.Finish()
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

	sp, ctx := opentracing.StartSpanFromContext(ctx, "sonic-parseResponse")
	defer sp.Finish()
scan:
	ssp, _ := opentracing.StartSpanFromContext(ctx, "scanning main scanner")
	scanned := s.s.Scan()
	ssp.LogFields(opLog.Bool("scanned", scanned))
	ssp.Finish()

	sp.SetTag("scanned", scanned)
	sp.LogFields(opLog.String("scanned value", s.s.Text()))

	w := bufio.NewScanner(bytes.NewBuffer(s.s.Bytes()))
	w.Split(bufio.ScanWords)
	w.Scan()

	switch w.Text() {
	case "PENDING":
		psp, _ := opentracing.StartSpanFromContext(ctx, "sonic-parseResponse-pending")
		defer psp.Finish()

		ch := make(chan string)

		w.Scan()

		s.pl.Lock()
		defer s.pl.Unlock()
		s.pending[w.Text()] = &pendingQuery{ids: ch, sp: opentracing.StartSpan("sonic-parseResponse-pending-waiting-for-response")}

		s.pollForEvents()

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
	sp, ctx := opentracing.StartSpanFromContext(ctx, "sonic-handleEvent")
	defer sp.Finish()

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
		defer pending.sp.Finish()
		defer close(pending.ids)

		for w.Scan() {
			chSp := opentracing.StartSpan("sending-result-to-chan", opentracing.ChildOf(pending.sp.Context()))
			pending.ids <- w.Text()
			chSp.Finish()
		}
	default:
		log.Panicf("could not determine how to interpret event: %q", event)
	}
}

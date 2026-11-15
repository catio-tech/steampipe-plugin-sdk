package plugin

// Regression tests for scan errors that are lost at the end of a scan.
//
// streamRows and execute each wait in one select for an error or for the end of
// the rows. The error is sent before the end of the rows, but when the receiver
// is busy (it sends an earlier row), both channels are ready when it selects
// again. Go then picks a case at random, so a failed scan could end as a
// success with only part of its rows.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	sdkgrpc "github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin/transform"
	"github.com/turbot/steampipe-plugin-sdk/v6/query_cache"
	"google.golang.org/grpc"
)

// Both select cases are ready: an error is pending and rowChan is closed. The
// error must come back every time. Without the check at the nil row, each
// attempt returns nil half of the time.
func TestStreamRowsReturnsErrorPendingAtEndOfRows(t *testing.T) {
	want := errors.New("list failed")
	for i := 0; i < 100; i++ {
		d := &QueryData{
			Table:     &Table{Name: "error_fixture", Plugin: &Plugin{Name: "error_fixture"}},
			plugin:    &Plugin{queryCache: &query_cache.QueryCache{}},
			errorChan: make(chan error, 1),
		}
		d.errorChan <- want
		rowChan := make(chan *proto.Row)
		close(rowChan)
		if err := d.streamRows(context.Background(), rowChan, make(chan bool)); !errors.Is(err, want) {
			t.Fatalf("attempt %d: streamRows returned %v, want %v", i, err, want)
		}
	}
}

// scanSignals orders one scan of the fixture table. It is in the stream's
// context, which the list call and the column transform get.
type scanSignals struct {
	// listed is closed when the list call has streamed all its items. Rows are
	// built only after it, so the reads of the query counters in streamRow come
	// after all their writes.
	listed chan struct{}
	// firstRow is closed when the stream gets its first row.
	firstRow chan struct{}
}

type scanSignalsKey struct{}

func signalsOf(ctx context.Context) *scanSignals {
	s, _ := ctx.Value(scanSignalsKey{}).(*scanSignals)
	return s
}

// slowStream is the server side of an Execute stream. Each Send waits, so the
// receivers in streamRows and execute are busy when the scan fails.
type slowStream struct {
	grpc.ServerStream
	ctx     context.Context
	cancel  context.CancelFunc
	signals *scanSignals
	delay   time.Duration
	rows    int
}

// newSlowStream gives a stream whose context ends after 10 s, so a scan that
// waits for a signal that never comes fails the test instead of hanging it.
func newSlowStream(delay time.Duration) *slowStream {
	s := &slowStream{delay: delay, signals: &scanSignals{listed: make(chan struct{}), firstRow: make(chan struct{})}}
	ctx := context.WithValue(context.Background(), scanSignalsKey{}, s.signals)
	s.ctx, s.cancel = context.WithTimeout(ctx, 10*time.Second)
	return s
}

func (s *slowStream) Context() context.Context { return s.ctx }

func (s *slowStream) Send(r *proto.ExecuteResponse) error {
	if r.GetRow() != nil {
		if s.rows++; s.rows == 1 {
			close(s.signals.firstRow)
		}
	}
	time.Sleep(s.delay)
	return nil
}

// errorFixturePlugin has one table whose list call streams 3 items and then
// fails, while the receiver sends the first row (an error on a later page).
func errorFixturePlugin(context.Context) *Plugin {
	return &Plugin{
		Name: "steampipe-plugin-error-fixture",
		TableMap: map[string]*Table{
			"list_fails": {
				Name: "list_fails",
				List: &ListConfig{Hydrate: func(ctx context.Context, d *QueryData, _ *HydrateData) (any, error) {
					for i := 0; i < 3; i++ {
						d.StreamListItem(ctx, map[string]any{"id": i})
					}
					if s := signalsOf(ctx); s != nil {
						close(s.listed)
						select {
						case <-s.firstRow:
						case <-ctx.Done():
						}
					}
					return nil, errors.New("list failed after 3 rows")
				}},
				Columns: []*Column{{
					Name: "id",
					Type: proto.ColumnType_INT,
					Transform: transform.From(func(ctx context.Context, td *transform.TransformData) (any, error) {
						if s := signalsOf(ctx); s != nil {
							select {
							case <-s.listed:
							case <-ctx.Done():
							}
						}
						return td.HydrateItem.(map[string]any)["id"], nil
					}),
				}},
			},
		},
	}
}

// A list call that fails after its first rows reach the receiver must fail the
// Execute call, also when the receiver is slow.
func TestExecuteReturnsListErrorWithSlowReceiver(t *testing.T) {
	const connection = "error_fixture"
	server := Server(&ServeOpts{PluginFunc: errorFixturePlugin})
	res, err := server.SetAllConnectionConfigs(&proto.SetAllConnectionConfigsRequest{
		Configs: []*proto.ConnectionConfig{{
			Connection:      connection,
			Plugin:          "steampipe-plugin-error-fixture",
			PluginShortName: "error_fixture",
			PluginInstance:  "error_fixture",
		}},
		MaxCacheSizeMb: -1,
	})
	if err != nil {
		t.Fatalf("SetAllConnectionConfigs: %v", err)
	}
	if msg, failed := res.FailedConnections[connection]; failed {
		t.Fatalf("connection failed: %s", msg)
	}
	if _, err := server.SetCacheOptions(&proto.SetCacheOptionsRequest{Enabled: false, Ttl: 1, MaxSizeMb: 1}); err != nil {
		t.Fatalf("SetCacheOptions: %v", err)
	}

	for i := 0; i < 50; i++ {
		req := &proto.ExecuteRequest{
			Table:        "list_fails",
			QueryContext: proto.NewQueryContext([]string{"id"}, nil, -1, nil),
			Connection:   connection,
			CallId:       sdkgrpc.BuildCallId(),
			ExecuteConnectionData: map[string]*proto.ExecuteConnectionData{
				connection: {CacheEnabled: false},
			},
		}
		stream := newSlowStream(2 * time.Millisecond)
		err := server.Execute(req, stream)
		timedOut := stream.ctx.Err() != nil
		stream.cancel()
		if timedOut {
			t.Fatalf("attempt %d: the scan did not end within 10 s", i)
		}
		if err == nil {
			t.Fatalf("attempt %d: Execute returned no error after %d rows", i, stream.rows)
		}
		if !strings.Contains(err.Error(), "list failed after 3 rows") {
			t.Fatalf("attempt %d: Execute returned %v, want the list error", i, err)
		}
	}
}

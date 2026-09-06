package repairnzb

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Tensai75/nzbparser"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/kipsilabs/nzb-repair/internal/config"
	"github.com/kipsilabs/nzb-repair/internal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// errTransient mimics the wrapped "all providers exhausted" 502 error that is
// NOT the ErrServiceUnavailable sentinel.
var errTransient = errors.New("nntp: all providers exhausted: host: nntp auth: unexpected response to AUTHINFO PASS: 502 Too many connections")

func TestIsRetryableDownloadErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"canceled", context.Canceled, false},
		{"article not found", nntppool.ErrArticleNotFound, false},
		{"quota exceeded", nntppool.ErrQuotaExceeded, false},
		{"generic 502 too many connections", errTransient, true},
		{"service unavailable sentinel", nntppool.ErrServiceUnavailable, true},
		{"wrapped canceled", errors.New("boom: " + context.Canceled.Error()), true}, // not wrapped via %w
		{"unknown", errors.New("some other error"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRetryableDownloadErr(tt.err))
		})
	}
}

func TestBackoffDelay(t *testing.T) {
	base := 2 * time.Second
	maxDelay := 60 * time.Second

	for attempt := range 64 {
		d := backoffDelay(attempt, base, maxDelay)
		require.GreaterOrEqual(t, d, time.Duration(0), "delay must be non-negative")
		require.LessOrEqual(t, d, maxDelay, "delay must never exceed max (attempt %d)", attempt)
	}

	// Full-jitter lower bound: attempt 0 -> capped=base, delay in [base/2, base].
	got := backoffDelay(0, base, maxDelay)
	assert.GreaterOrEqual(t, got, base/2)
	assert.LessOrEqual(t, got, base)
}

func TestRetryDownload_TransientThenSuccess(t *testing.T) {
	cfg := fastRetryConfig()
	calls := 0
	err := retryDownload(context.Background(), cfg, "seg@test", func() error {
		calls++
		if calls < 3 {
			return errTransient
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls)
}

func TestRetryDownload_Exhaustion(t *testing.T) {
	cfg := fastRetryConfig()
	cfg.DownloadRetries = 2
	calls := 0
	err := retryDownload(context.Background(), cfg, "seg@test", func() error {
		calls++
		return errTransient
	})
	require.ErrorIs(t, err, errTransient)
	// 1 initial attempt + DownloadRetries retries.
	assert.Equal(t, 3, calls)
}

func TestRetryDownload_NonRetryableReturnsImmediately(t *testing.T) {
	cfg := fastRetryConfig()
	calls := 0
	err := retryDownload(context.Background(), cfg, "seg@test", func() error {
		calls++
		return nntppool.ErrArticleNotFound
	})
	require.ErrorIs(t, err, nntppool.ErrArticleNotFound)
	assert.Equal(t, 1, calls, "non-retryable errors must not retry")
}

func TestRetryDownload_CancelDuringBackoff(t *testing.T) {
	cfg := fastRetryConfig()
	cfg.DownloadRetryBaseDelay = time.Hour // force a long wait so cancel wins
	cfg.DownloadRetryMaxDelay = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := retryDownload(ctx, cfg, "seg@test", func() error {
		return errTransient
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second, "must return promptly on cancel, not wait the full backoff")
}

func TestDownloadAll_RetriesTransientThenSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := fastRetryConfig()
	cfg.DownloadWorkers = 1

	mockPool := mocks.NewMockDownloader(ctrl)
	tmpDir := t.TempDir()

	segID := "seg1@test"
	content := []byte("segment payload")
	file := nzbparser.NzbFile{
		Filename:      "movie.bin",
		Bytes:         int64(len(content)),
		TotalSegments: 1,
		Segments:      []nzbparser.NzbSegment{{Id: segID, Number: 1, Bytes: len(content)}},
	}

	gomock.InOrder(
		mockPool.EXPECT().BodyStream(gomock.Any(), segID, gomock.Any(), gomock.Any()).Return(nil, errTransient),
		mockPool.EXPECT().BodyStream(gomock.Any(), segID, gomock.Any(), gomock.Any()).Return(nil, errTransient),
		mockPool.EXPECT().BodyStream(gomock.Any(), segID, gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, w io.Writer, onMeta ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
				for _, fn := range onMeta {
					fn(nntppool.YEncMeta{Part: 1, PartSize: int64(len(content))})
				}
				_, _ = w.Write(content)
				return &nntppool.ArticleBody{}, nil
			}),
	)

	brokenCh := make(chan brokenSegment, 1)
	err := downloadAll(context.Background(), cfg, mockPool, []nzbparser.NzbFile{file}, brokenCh, tmpDir)
	require.NoError(t, err)
	assert.Len(t, brokenCh, 0, "successful download must not queue a broken segment")

	written, err := os.ReadFile(filepath.Join(tmpDir, file.Filename))
	require.NoError(t, err)
	assert.Equal(t, content, written)
}

func TestDownloadAll_RetryExhaustionReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := fastRetryConfig()
	cfg.DownloadWorkers = 1
	cfg.DownloadRetries = 2

	mockPool := mocks.NewMockDownloader(ctrl)
	tmpDir := t.TempDir()

	segID := "seg1@test"
	file := nzbparser.NzbFile{
		Filename:      "movie.bin",
		Bytes:         10,
		TotalSegments: 1,
		Segments:      []nzbparser.NzbSegment{{Id: segID, Number: 1, Bytes: 10}},
	}

	// 1 initial + 2 retries = 3 attempts, all failing.
	mockPool.EXPECT().BodyStream(gomock.Any(), segID, gomock.Any(), gomock.Any()).
		Return(nil, errTransient).Times(3)

	err := downloadAll(context.Background(), cfg, mockPool, []nzbparser.NzbFile{file}, nil, tmpDir)
	require.Error(t, err)
	assert.ErrorIs(t, err, errTransient)
}

func TestDownloadAll_ArticleNotFoundNoRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := fastRetryConfig()
	cfg.DownloadWorkers = 1

	mockPool := mocks.NewMockDownloader(ctrl)
	tmpDir := t.TempDir()

	segID := "missing@test"
	file := nzbparser.NzbFile{
		Filename:      "movie.bin",
		Bytes:         10,
		TotalSegments: 1,
		Segments:      []nzbparser.NzbSegment{{Id: segID, Number: 1, Bytes: 10}},
	}

	// Must be called exactly once — no retries for a genuinely missing article.
	mockPool.EXPECT().BodyStream(gomock.Any(), segID, gomock.Any(), gomock.Any()).
		Return(nil, nntppool.ErrArticleNotFound).Times(1)

	brokenCh := make(chan brokenSegment, 1)
	err := downloadAll(context.Background(), cfg, mockPool, []nzbparser.NzbFile{file}, brokenCh, tmpDir)
	require.NoError(t, err)
	require.Len(t, brokenCh, 1, "missing segment must be routed for repair")
	bs := <-brokenCh
	assert.Equal(t, segID, bs.segment.Id)
}

func TestDownloadAll_QuotaExceededNoRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cfg := fastRetryConfig()
	cfg.DownloadWorkers = 1

	mockPool := mocks.NewMockDownloader(ctrl)
	tmpDir := t.TempDir()

	segID := "seg1@test"
	file := nzbparser.NzbFile{
		Filename:      "movie.bin",
		Bytes:         10,
		TotalSegments: 1,
		Segments:      []nzbparser.NzbSegment{{Id: segID, Number: 1, Bytes: 10}},
	}

	// Quota won't recover this run — no retries, terminal error.
	mockPool.EXPECT().BodyStream(gomock.Any(), segID, gomock.Any(), gomock.Any()).
		Return(nil, nntppool.ErrQuotaExceeded).Times(1)

	err := downloadAll(context.Background(), cfg, mockPool, []nzbparser.NzbFile{file}, nil, tmpDir)
	require.Error(t, err)
}

// fastRetryConfig returns a config with near-instant backoff so retry tests run fast.
func fastRetryConfig() config.Config {
	return config.Config{
		DownloadWorkers:        1,
		DownloadRetries:        5,
		DownloadRetryBaseDelay: time.Microsecond,
		DownloadRetryMaxDelay:  time.Millisecond,
	}
}

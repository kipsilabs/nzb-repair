package repairnzb

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
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

// fakeArticle is one segment as a provider would serve it.
type fakeArticle struct {
	partBegin int64
	body      []byte
}

// serveArticles makes the mock answer BodyStream from a message-ID map, driving
// the onMeta callback with the yEnc part header first, exactly as the pool does.
func serveArticles(mockPool *mocks.MockDownloader, articles map[string]fakeArticle) {
	mockPool.EXPECT().
		BodyStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, id string, w io.Writer, onMeta ...func(nntppool.YEncMeta),
		) (*nntppool.ArticleBody, error) {
			a, ok := articles[id]
			if !ok {
				return nil, nntppool.ErrArticleNotFound
			}

			for _, fn := range onMeta {
				fn(nntppool.YEncMeta{PartBegin: a.partBegin, PartSize: int64(len(a.body))})
			}

			if _, err := w.Write(a.body); err != nil {
				return nil, err
			}

			return &nntppool.ArticleBody{}, nil
		}).AnyTimes()
}

func downloadTestConfig(workers int) config.Config {
	return config.Config{
		DownloadWorkers:        workers,
		DownloadRetries:        1,
		DownloadRetryBaseDelay: time.Microsecond,
		DownloadRetryMaxDelay:  time.Millisecond,
		StatConcurrency:        4,
	}
}

// TestDownloadAllShortFinalSegment is the regression test for the file-tail
// corruption: the previous implementation placed each segment at
// (number-1)*<that segment's own length>, so a short trailing segment landed
// before its true offset and overwrote real data.
func TestDownloadAllShortFinalSegment(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mocks.NewMockDownloader(ctrl)
	tmpDir := t.TempDir()

	// Three segments with a 10-byte stride; the last one holds only 4 bytes.
	serveArticles(mockPool, map[string]fakeArticle{
		"s1@t": {partBegin: 0, body: []byte("AAAAAAAAAA")},
		"s2@t": {partBegin: 10, body: []byte("BBBBBBBBBB")},
		"s3@t": {partBegin: 20, body: []byte("CCCC")},
	})

	file := nzbparser.NzbFile{
		Filename:      "movie.bin",
		Bytes:         24,
		TotalSegments: 3,
		Segments: []nzbparser.NzbSegment{
			{Id: "s1@t", Number: 1, Bytes: 10},
			{Id: "s2@t", Number: 2, Bytes: 10},
			{Id: "s3@t", Number: 3, Bytes: 4},
		},
	}

	err := downloadAll(context.Background(), downloadTestConfig(3), mockPool,
		[]nzbparser.NzbFile{file}, nil, tmpDir)
	require.NoError(t, err)

	got, err := os.ReadFile(filepath.Join(tmpDir, file.Filename))
	require.NoError(t, err)
	assert.Equal(t, "AAAAAAAAAABBBBBBBBBBCCCC", string(got),
		"the short final segment must land at its true offset")
}

// TestDownloadAllAcrossFiles covers the cross-file scheduler: every segment of
// every file is fetched through one pool and lands in the right file.
func TestDownloadAllAcrossFiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mocks.NewMockDownloader(ctrl)
	tmpDir := t.TempDir()

	serveArticles(mockPool, map[string]fakeArticle{
		"a1@t": {partBegin: 0, body: []byte("alpha")},
		"a2@t": {partBegin: 5, body: []byte("ALPHA")},
		"b1@t": {partBegin: 0, body: []byte("bravo")},
	})

	files := []nzbparser.NzbFile{
		{
			Filename: "a.bin", Bytes: 10, TotalSegments: 2,
			Segments: []nzbparser.NzbSegment{
				{Id: "a1@t", Number: 1, Bytes: 5},
				{Id: "a2@t", Number: 2, Bytes: 5},
			},
		},
		{
			Filename: "b.bin", Bytes: 5, TotalSegments: 1,
			Segments: []nzbparser.NzbSegment{{Id: "b1@t", Number: 1, Bytes: 5}},
		},
	}

	err := downloadAll(context.Background(), downloadTestConfig(4), mockPool, files, nil, tmpDir)
	require.NoError(t, err)

	for name, want := range map[string]string{"a.bin": "alphaALPHA", "b.bin": "bravo"} {
		got, err := os.ReadFile(filepath.Join(tmpDir, name))
		require.NoError(t, err, name)
		assert.Equal(t, want, string(got), name)
	}
}

// TestDownloadAllRoutesMissingSegmentsPerFile checks that a missing segment is
// attributed to the file it belongs to, which matters now that segments from
// different files are interleaved on one pool.
func TestDownloadAllRoutesMissingSegmentsPerFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mocks.NewMockDownloader(ctrl)
	tmpDir := t.TempDir()

	// b1@t is absent from the map, so the mock reports it as not found.
	serveArticles(mockPool, map[string]fakeArticle{
		"a1@t": {partBegin: 0, body: []byte("alpha")},
	})

	files := []nzbparser.NzbFile{
		{
			Filename: "a.bin", Bytes: 5, TotalSegments: 1,
			Segments: []nzbparser.NzbSegment{{Id: "a1@t", Number: 1, Bytes: 5}},
		},
		{
			Filename: "b.bin", Bytes: 5, TotalSegments: 1,
			Segments: []nzbparser.NzbSegment{{Id: "b1@t", Number: 1, Bytes: 5}},
		},
	}

	brokenCh := make(chan brokenSegment, 4)

	var (
		wg     sync.WaitGroup
		broken []brokenSegment
	)

	wg.Add(1)

	go func() {
		defer wg.Done()

		for bs := range brokenCh {
			broken = append(broken, bs)
		}
	}()

	err := downloadAll(context.Background(), downloadTestConfig(2), mockPool, files, brokenCh, tmpDir)
	require.NoError(t, err)
	close(brokenCh)
	wg.Wait()

	require.Len(t, broken, 1)
	assert.Equal(t, "b1@t", broken[0].segment.Id)
	assert.Equal(t, "b.bin", broken[0].file.Filename, "broken segment must point at its own file")
}

// TestDownloadAllMissingSegmentIsFatalWithoutChannel covers the par2 download
// path, where a missing article has nowhere to be routed.
func TestDownloadAllMissingSegmentIsFatalWithoutChannel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mocks.NewMockDownloader(ctrl)
	serveArticles(mockPool, map[string]fakeArticle{})

	file := nzbparser.NzbFile{
		Filename: "p.par2", Bytes: 5, TotalSegments: 1,
		Segments: []nzbparser.NzbSegment{{Id: "gone@t", Number: 1, Bytes: 5}},
	}

	err := downloadAll(context.Background(), downloadTestConfig(1), mockPool,
		[]nzbparser.NzbFile{file}, nil, t.TempDir())
	require.Error(t, err)
}

// TestDownloadAllSkipsExistingFiles preserves the resume behaviour.
func TestDownloadAllSkipsExistingFiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mocks.NewMockDownloader(ctrl)
	tmpDir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "done.bin"), []byte("cached"), 0o600))

	// No BodyStream expectation is registered, so any fetch fails the test.
	file := nzbparser.NzbFile{
		Filename: "done.bin", Bytes: 6, TotalSegments: 1,
		Segments: []nzbparser.NzbSegment{{Id: "x@t", Number: 1, Bytes: 6}},
	}

	err := downloadAll(context.Background(), downloadTestConfig(1), mockPool,
		[]nzbparser.NzbFile{file}, nil, tmpDir)
	require.NoError(t, err)

	got, err := os.ReadFile(filepath.Join(tmpDir, "done.bin"))
	require.NoError(t, err)
	assert.Equal(t, "cached", string(got))
}

func TestCountMissingParSegments(t *testing.T) {
	parFiles := []nzbparser.NzbFile{
		{Segments: []nzbparser.NzbSegment{{Id: "p1@t"}, {Id: "p2@t"}}},
		{Segments: []nzbparser.NzbSegment{{Id: "p3@t"}}},
	}

	tests := []struct {
		name        string
		results     []nntppool.StatManyResult
		wantMissing int64
		wantTotal   int64
		wantErr     bool
	}{
		{
			name: "all present",
			results: []nntppool.StatManyResult{
				{MessageID: "p1@t", Result: &nntppool.StatResult{}},
				{MessageID: "p2@t", Result: &nntppool.StatResult{}},
				{MessageID: "p3@t", Result: &nntppool.StatResult{}},
			},
			wantMissing: 0,
			wantTotal:   3,
		},
		{
			name: "some missing",
			results: []nntppool.StatManyResult{
				{MessageID: "p1@t", Result: &nntppool.StatResult{}},
				{MessageID: "p2@t", Err: nntppool.ErrArticleNotFound},
				{MessageID: "p3@t", Err: nntppool.ErrArticleNotFound},
			},
			wantMissing: 2,
			wantTotal:   3,
		},
		{
			name: "transport error surfaces",
			results: []nntppool.StatManyResult{
				{MessageID: "p1@t", Err: errors.New("connection reset")},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockPool := mocks.NewMockDownloader(ctrl)
			mockPool.EXPECT().
				StatMany(gomock.Any(), []string{"p1@t", "p2@t", "p3@t"}, gomock.Any()).
				Return(statResults(tt.results...)).Times(1)

			missing, total, err := countMissingParSegments(
				context.Background(), downloadTestConfig(1), mockPool, parFiles)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantMissing, missing, "missing")
			assert.Equal(t, tt.wantTotal, total, "total")
		})
	}
}

// TestCountMissingParSegmentsUsesStatNotBody pins the whole point of the sweep:
// availability is probed without transferring any article bodies.
func TestCountMissingParSegmentsUsesStatNotBody(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mocks.NewMockDownloader(ctrl)
	mockPool.EXPECT().BodyStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	mockPool.EXPECT().StatMany(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(statResults(nntppool.StatManyResult{MessageID: "p1@t", Result: &nntppool.StatResult{}})).
		Times(1)

	_, total, err := countMissingParSegments(context.Background(), downloadTestConfig(1), mockPool,
		[]nzbparser.NzbFile{{Segments: []nzbparser.NzbSegment{{Id: "p1@t"}}}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), total)
}

func TestCountMissingParSegmentsNoParFiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mocks.NewMockDownloader(ctrl)
	mockPool.EXPECT().StatMany(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	missing, total, err := countMissingParSegments(
		context.Background(), downloadTestConfig(1), mockPool, nil)
	require.NoError(t, err)
	assert.Zero(t, missing)
	assert.Zero(t, total)
}

// TestDownloadAllRejectsMultipartWithoutPartHeader guards the offset source:
// without a yEnc part header there is no way to place a segment inside a
// multi-part file, and guessing from the segment number silently corrupts it.
func TestDownloadAllRejectsMultipartWithoutPartHeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mocks.NewMockDownloader(ctrl)

	// DoAndReturn writes a body but never invokes onMeta.
	mockPool.EXPECT().
		BodyStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, _ string, w io.Writer, _ ...func(nntppool.YEncMeta),
		) (*nntppool.ArticleBody, error) {
			_, _ = w.Write([]byte("orphan"))

			return &nntppool.ArticleBody{}, nil
		}).AnyTimes()

	file := nzbparser.NzbFile{
		Filename: "multi.bin", Bytes: 12, TotalSegments: 2,
		Segments: []nzbparser.NzbSegment{
			{Id: "m1@t", Number: 1, Bytes: 6},
			{Id: "m2@t", Number: 2, Bytes: 6},
		},
	}

	err := downloadAll(context.Background(), downloadTestConfig(1), mockPool,
		[]nzbparser.NzbFile{file}, nil, t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no yEnc part header")
}

// TestDownloadAllSinglePartWithoutHeader is the companion case: a one-segment
// file always starts at offset 0, so a missing part header is harmless.
func TestDownloadAllSinglePartWithoutHeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPool := mocks.NewMockDownloader(ctrl)
	tmpDir := t.TempDir()

	mockPool.EXPECT().
		BodyStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, _ string, w io.Writer, _ ...func(nntppool.YEncMeta),
		) (*nntppool.ArticleBody, error) {
			_, _ = w.Write([]byte("whole"))

			return &nntppool.ArticleBody{}, nil
		}).Times(1)

	file := nzbparser.NzbFile{
		Filename: "single.bin", Bytes: 5, TotalSegments: 1,
		Segments: []nzbparser.NzbSegment{{Id: "only@t", Number: 1, Bytes: 5}},
	}

	err := downloadAll(context.Background(), downloadTestConfig(1), mockPool,
		[]nzbparser.NzbFile{file}, nil, tmpDir)
	require.NoError(t, err)

	got, err := os.ReadFile(filepath.Join(tmpDir, file.Filename))
	require.NoError(t, err)
	assert.Equal(t, "whole", string(got))
}

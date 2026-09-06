package repairnzb

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Tensai75/nzbparser"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/kipsilabs/nzb-repair/internal/config"
	"github.com/kipsilabs/nzb-repair/internal/mocks"
	"github.com/mnightingale/rapidyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// postedSegment is what the fake upload pool observed for one article.
type postedSegment struct {
	messageID string
	meta      rapidyenc.Meta
	body      []byte
}

// TestUploadPar2FilesStreamsSegmentsFromDisk pins the segmentation of par2
// uploads: each posted article must carry the exact bytes of its slice of the
// file, at the right offset, including a short trailing segment.
func TestUploadPar2FilesStreamsSegmentsFromDisk(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := t.TempDir()

	// Two and a bit segments, so the last one is short.
	contentA := make([]byte, defaultSegmentSize*2+1234)
	for i := range contentA {
		contentA[i] = byte(i % 251)
	}

	contentB := []byte("small par2 volume")

	pathA := filepath.Join(dir, "rel.vol000+01.par2")
	pathB := filepath.Join(dir, "rel.par2")
	require.NoError(t, os.WriteFile(pathA, contentA, 0o600))
	require.NoError(t, os.WriteFile(pathB, contentB, 0o600))

	var (
		mu     sync.Mutex
		posted = map[string][]postedSegment{}
	)

	mockUpload := mocks.NewMockUploader(ctrl)
	mockUpload.EXPECT().
		PostYenc(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, h nntppool.PostHeaders, body io.Reader, meta rapidyenc.Meta,
		) (*nntppool.PostResult, error) {
			b, err := io.ReadAll(body)
			if err != nil {
				return nil, err
			}

			mu.Lock()
			defer mu.Unlock()

			posted[meta.FileName] = append(posted[meta.FileName], postedSegment{
				messageID: h.MessageID,
				meta:      meta,
				body:      b,
			})

			return &nntppool.PostResult{StatusCode: 240}, nil
		}).AnyTimes()

	cfg := config.Config{
		UploadWorkers: 4,
		Upload:        config.UploadConfig{ObfuscationPolicy: config.ObfuscationPolicyNone},
	}
	nzb := &nzbparser.Nzb{Files: []nzbparser.NzbFile{{Groups: []string{"alt.binaries.test"}}}}

	newFiles, err := uploadPar2Files(context.Background(), []string{pathA, pathB}, cfg, mockUpload, nzb)
	require.NoError(t, err)
	require.Len(t, newFiles, 2)

	for _, tc := range []struct {
		name    string
		content []byte
	}{
		{"rel.vol000+01.par2", contentA},
		{"rel.par2", contentB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			segs := posted[tc.name]
			wantSegments := (len(tc.content) + defaultSegmentSize - 1) / defaultSegmentSize
			require.Len(t, segs, wantSegments)

			// Reassemble from the posted parts and compare with the file.
			assembled := make([]byte, len(tc.content))
			seen := map[int64]bool{}

			for _, s := range segs {
				assert.Equal(t, int64(len(tc.content)), s.meta.FileSize, "FileSize")
				assert.Equal(t, int64(wantSegments), s.meta.TotalParts, "TotalParts")
				assert.Equal(t, int64(len(s.body)), s.meta.PartSize, "PartSize must match the bytes posted")
				assert.False(t, seen[s.meta.PartNumber], "duplicate part number %d", s.meta.PartNumber)
				seen[s.meta.PartNumber] = true

				copy(assembled[s.meta.Offset:], s.body)
			}

			assert.Equal(t, tc.content, assembled, "posted segments must reassemble into the file")
		})
	}

	// The NZB entries must describe what was actually posted.
	for i, f := range newFiles {
		require.NotEmpty(t, f.Segments, "file %d has no segments", i)

		total := 0
		ids := map[string]bool{}

		for _, s := range f.Segments {
			require.NotEmpty(t, s.Id, "segment %d of %s has no message ID", s.Number, f.Filename)
			assert.False(t, ids[s.Id], "duplicate message ID %s", s.Id)
			ids[s.Id] = true
			total += s.Bytes
		}

		assert.Equal(t, f.Bytes, int64(total), "segment bytes must sum to the file size")
	}
}

// TestUploadPar2FilesMissingFileDoesNotLeak covers the early-return path: the
// shared pool must be drained before the deferred close reaches open handles.
func TestUploadPar2FilesMissingFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dir := t.TempDir()
	good := filepath.Join(dir, "good.par2")
	require.NoError(t, os.WriteFile(good, []byte("data"), 0o600))

	mockUpload := mocks.NewMockUploader(ctrl)
	mockUpload.EXPECT().
		PostYenc(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context, _ nntppool.PostHeaders, body io.Reader, _ rapidyenc.Meta,
		) (*nntppool.PostResult, error) {
			_, _ = io.ReadAll(body)

			return &nntppool.PostResult{StatusCode: 240}, nil
		}).AnyTimes()

	cfg := config.Config{UploadWorkers: 2}
	nzb := &nzbparser.Nzb{}

	_, err := uploadPar2Files(context.Background(),
		[]string{good, filepath.Join(dir, "does-not-exist.par2")}, cfg, mockUpload, nzb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open par2 file")
}

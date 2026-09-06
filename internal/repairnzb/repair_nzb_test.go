package repairnzb

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Tensai75/nzbparser"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/kipsilabs/nzb-repair/internal/config"
	"github.com/kipsilabs/nzb-repair/internal/mocks" // Import the generated mocks
	"github.com/mnightingale/rapidyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestRepairNzb(t *testing.T) {
	// Setup
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	cfg := config.Config{
		DownloadWorkers: 1, // Use 1 worker for easier expectation setting
		UploadWorkers:   1,
		// Par2Exe is not used directly here as we mock the executor
		Upload: config.UploadConfig{
			ObfuscationPolicy: config.ObfuscationPolicyNone,
		},
	}

	mockDownloadPool := mocks.NewMockDownloader(ctrl)
	mockUploadPool := mocks.NewMockUploader(ctrl)
	mockPar2Executor := mocks.NewMockPar2Executor(ctrl) // Instantiate the mock executor

	// Create a temporary directory for testing
	inputDir := t.TempDir()
	tmpDir := t.TempDir()
	outputDir := t.TempDir()
	outputFile := filepath.Join(outputDir, "output.nzb")
	nzbFile := filepath.Join(inputDir, "input.nzb")

	// Define file/segment names for clarity
	dataFileName := "test.mkv"
	par2FileName := "test.mkv.par2"
	brokenSegmentID := "segment1@test"
	goodSegmentID := "segment2@test"
	parSegmentID := "parSegment1@test"
	repairedDataContent := "repaired data for segment 1 and 2 combined"
	originalDataFileContentSegment2 := "test data segment 2"

	// Create a dummy NZB file for testing
	nzbContent := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <head>
  <meta type="category">TV > HD</meta>
  <meta type="name">Test Release</meta>
 </head>
 <file poster="test@example.com" date="1678886400" subject="[1/2] %s - &quot;test.mkv&quot; yEnc (1/2)">
  <groups>
   <group>alt.binaries.test</group>
  </groups>
  <segments>
   <segment bytes="%d" number="1">%s</segment>
   <segment bytes="%d" number="2">%s</segment>
  </segments>
 </file>
 <file poster="test@example.com" date="1678886400" subject="[2/2] %s - &quot;test.mkv.par2&quot; yEnc (1/1)">
  <groups>
   <group>alt.binaries.test</group>
  </groups>
  <segments>
   <segment bytes="50" number="1">%s</segment>
  </segments>
 </file>
</nzb>`, dataFileName, len(repairedDataContent)/2, brokenSegmentID, len(originalDataFileContentSegment2), goodSegmentID, par2FileName, parSegmentID)
	err := os.WriteFile(nzbFile, []byte(nzbContent), 0644)
	require.NoError(t, err)

	// --- Mock Expectations ---

	// Download Expectations:
	// Segment 1 (broken) - Not Found
	mockDownloadPool.EXPECT().BodyStream(gomock.Any(), brokenSegmentID, gomock.Any(), gomock.Any()).
		Return(nil, nntppool.ErrArticleNotFound)
	// Segment 2 (good) - Found & Written
	mockDownloadPool.EXPECT().BodyStream(gomock.Any(), goodSegmentID, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, writer io.Writer, _ ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
			if writer != nil {
				// Simulate writing segment 2 content to the correct offset in the temp file
				// Note: This write happens *before* par2 repair in the actual code flow.
				// We assume downloadWorker creates the file.
				filePath := filepath.Join(tmpDir, dataFileName)
				file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
				require.NoError(t, err) // Test fails if we can't open file
				defer func() {
					_ = file.Close()
				}()
				// Write at offset (segment number - 1) * segment size
				// Segment size is tricky here, use fixed size from NZB for segment 2
				_, err = file.WriteAt([]byte(originalDataFileContentSegment2), int64(len(repairedDataContent)/2))
				require.NoError(t, err)
				// Simulate the io.Copy happening in the actual BodyStream call
				_, err = writer.Write([]byte(originalDataFileContentSegment2))
				require.NoError(t, err)
			}

			return &nntppool.ArticleBody{}, nil
		}).Times(1)
	// Par2 Segment - Found & Written
	mockDownloadPool.EXPECT().BodyStream(gomock.Any(), parSegmentID, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, writer io.Writer, _ ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
			// Simulate writing the par2 file content
			parFilePath := filepath.Join(tmpDir, par2FileName)
			parContent := []byte("dummy par2 data")
			err := os.WriteFile(parFilePath, parContent, 0644)
			require.NoError(t, err)
			// Simulate io.Copy
			if writer != nil {
				_, err = writer.Write(parContent)
				require.NoError(t, err)
			}

			return &nntppool.ArticleBody{}, nil
		}).Times(1)

	// Par2 Repair Expectation:
	mockPar2Executor.EXPECT().Repair(gomock.Any(), tmpDir).
		DoAndReturn(func(ctx context.Context, path string) error {
			// Simulate the outcome of par2 repair: the broken file is now complete.
			fullFilePath := filepath.Join(path, dataFileName)
			// Write the complete, "repaired" content.
			err := os.WriteFile(fullFilePath, []byte(repairedDataContent), 0644)
			require.NoError(t, err) // Ensure simulation is successful

			return nil // Simulate successful repair
		}).Times(1)

	// Upload Expectation:
	// Expect PostYenc to be called once for the repaired segment (segment 1)
	var postedArticle bytes.Buffer
	mockUploadPool.EXPECT().PostYenc(gomock.Any(), gomock.Any(), gomock.Any(), gomock.AssignableToTypeOf(rapidyenc.Meta{})).
		DoAndReturn(func(ctx context.Context, headers nntppool.PostHeaders, body io.Reader, meta rapidyenc.Meta) (*nntppool.PostResult, error) {
			// Capture the posted article content if needed for assertion
			_, err := io.Copy(&postedArticle, body)
			assert.NoError(t, err)
			return &nntppool.PostResult{}, nil // Simulate successful upload
		}).Times(1)

	// --- Call the function ---
	err = RepairNzb(ctx, cfg, mockDownloadPool, mockUploadPool, mockPar2Executor, nzbFile, outputFile, tmpDir)
	require.NoError(t, err)

	// --- Assertions ---

	// 1. Check if the output NZB file exists
	_, err = os.Stat(outputFile)
	assert.NoError(t, err, "Output NZB file should exist")

	// 2. Check the content of the output NZB file
	outputNzbBytes, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	outputNzb, err := nzbparser.Parse(bytes.NewReader(outputNzbBytes))
	require.NoError(t, err)

	// Find the data file in the output NZB
	var foundDataFile *nzbparser.NzbFile
	for i := range outputNzb.Files {
		if outputNzb.Files[i].Filename == dataFileName {
			foundDataFile = &outputNzb.Files[i]
			break
		}
	}
	require.NotNil(t, foundDataFile, "Data file should be present in output NZB")

	// Assert that segment 1's ID has changed (was brokenSegmentID)
	require.Len(t, foundDataFile.Segments, 2, "Should still have 2 segments")
	assert.NotEqual(t, brokenSegmentID, foundDataFile.Segments[0].Id, "Segment 1 ID should have changed after repair and upload")
	// Assert that segment 2's ID is unchanged (was goodSegmentID)
	assert.Equal(t, goodSegmentID, foundDataFile.Segments[1].Id, "Segment 2 ID should remain unchanged")

	// 3. Check tmp directory state (e.g., par files removed if -p was simulated)
	_, err = os.Stat(filepath.Join(tmpDir, par2FileName))
	assert.True(t, os.IsNotExist(err), "Par2 file should have been deleted by repair process (-p flag simulation)")
}

func TestRepairNzb_Par2ThresholdTriggersRecreation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	cfg := config.Config{
		DownloadWorkers:        1,
		UploadWorkers:          1,
		Par2RecreateThreshold:  1.0, // 100% — 1/1 missing triggers recreation
		Par2RecreateRedundancy: 10,
		Upload:                 config.UploadConfig{ObfuscationPolicy: config.ObfuscationPolicyNone},
	}

	mockDownloadPool := mocks.NewMockDownloader(ctrl)
	mockUploadPool := mocks.NewMockUploader(ctrl)
	mockPar2Executor := mocks.NewMockPar2Executor(ctrl)

	inputDir := t.TempDir()
	tmpDir := t.TempDir()
	outputDir := t.TempDir()
	outputFile := filepath.Join(outputDir, "out.nzb")
	nzbFile := filepath.Join(inputDir, "input.nzb")

	dataFileName := "video.mkv"
	par2FileName := "video.mkv.par2"
	dataSegID := "dataSeg1@test"
	par2SegID := "par2Seg1@test"

	nzbContent := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <file poster="test@example.com" date="1678886400" subject="[1/2] %s yEnc (1/1)">
  <groups><group>alt.binaries.test</group></groups>
  <segments><segment bytes="20" number="1">%s</segment></segments>
 </file>
 <file poster="test@example.com" date="1678886400" subject="[2/2] %s yEnc (1/1)">
  <groups><group>alt.binaries.test</group></groups>
  <segments><segment bytes="50" number="1">%s</segment></segments>
 </file>
</nzb>`, dataFileName, dataSegID, par2FileName, par2SegID)
	require.NoError(t, os.WriteFile(nzbFile, []byte(nzbContent), 0644))

	// Data segment found (no broken data segments)
	mockDownloadPool.EXPECT().BodyStream(gomock.Any(), dataSegID, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, w io.Writer, _ ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
			_, _ = w.Write([]byte("videodata"))
			return &nntppool.ArticleBody{}, nil
		}).Times(1)

	// Par2 segment is missing → threshold triggered
	mockDownloadPool.EXPECT().StatMany(gomock.Any(), []string{par2SegID}, gomock.Any()).
		Return(statResults(nntppool.StatManyResult{
			MessageID: par2SegID,
			Err:       nntppool.ErrArticleNotFound,
		})).Times(1)

	// Expect Create (threshold exceeded); Repair must NOT be called
	mockPar2Executor.EXPECT().Create(gomock.Any(), gomock.Any(), 10).
		Return([]string{}, nil).Times(1)

	err := RepairNzb(ctx, cfg, mockDownloadPool, mockUploadPool, mockPar2Executor, nzbFile, outputFile, tmpDir)
	require.NoError(t, err)
}

func TestRepairNzb_Par2ThresholdNotReached(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	cfg := config.Config{
		DownloadWorkers:       1,
		Par2RecreateThreshold: 0.5, // 50% — 0/1 missing, threshold not reached
	}

	mockDownloadPool := mocks.NewMockDownloader(ctrl)
	mockPar2Executor := mocks.NewMockPar2Executor(ctrl)

	inputDir := t.TempDir()
	tmpDir := t.TempDir()
	nzbFile := filepath.Join(inputDir, "input.nzb")

	dataSegID := "dataSeg@test"
	par2SegID := "par2Seg@test"

	nzbContent := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <file poster="test@example.com" date="1678886400" subject="[1/2] data.mkv yEnc (1/1)">
  <groups><group>alt.binaries.test</group></groups>
  <segments><segment bytes="20" number="1">%s</segment></segments>
 </file>
 <file poster="test@example.com" date="1678886400" subject="[2/2] data.mkv.par2 yEnc (1/1)">
  <groups><group>alt.binaries.test</group></groups>
  <segments><segment bytes="50" number="1">%s</segment></segments>
 </file>
</nzb>`, dataSegID, par2SegID)
	require.NoError(t, os.WriteFile(nzbFile, []byte(nzbContent), 0644))

	// Data segment found
	mockDownloadPool.EXPECT().BodyStream(gomock.Any(), dataSegID, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, w io.Writer, _ ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
			_, _ = w.Write([]byte("data"))
			return &nntppool.ArticleBody{}, nil
		}).Times(1)

	// Par2 segment found (0% missing, threshold not reached)
	mockDownloadPool.EXPECT().StatMany(gomock.Any(), []string{par2SegID}, gomock.Any()).
		Return(statResults(nntppool.StatManyResult{
			MessageID: par2SegID,
			Result:    &nntppool.StatResult{MessageID: par2SegID},
		})).Times(1)

	// No Create, no Repair
	mockPar2Executor.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	mockPar2Executor.EXPECT().Repair(gomock.Any(), gomock.Any()).Times(0)

	err := RepairNzb(ctx, cfg, mockDownloadPool, nil, mockPar2Executor, nzbFile, "", tmpDir)
	require.NoError(t, err)
}

func TestRepairNzb_Par2ThresholdDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	cfg := config.Config{
		DownloadWorkers:       1,
		Par2RecreateThreshold: 0, // disabled
	}

	mockDownloadPool := mocks.NewMockDownloader(ctrl)
	mockPar2Executor := mocks.NewMockPar2Executor(ctrl)

	inputDir := t.TempDir()
	tmpDir := t.TempDir()
	nzbFile := filepath.Join(inputDir, "input.nzb")

	dataSegID := "dataSeg@test"
	par2SegID := "par2SegDisabled@test"

	nzbContent := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <file poster="test@example.com" date="1678886400" subject="[1/2] data.mkv yEnc (1/1)">
  <groups><group>alt.binaries.test</group></groups>
  <segments><segment bytes="20" number="1">%s</segment></segments>
 </file>
 <file poster="test@example.com" date="1678886400" subject="[2/2] data.mkv.par2 yEnc (1/1)">
  <groups><group>alt.binaries.test</group></groups>
  <segments><segment bytes="50" number="1">%s</segment></segments>
 </file>
</nzb>`, dataSegID, par2SegID)
	require.NoError(t, os.WriteFile(nzbFile, []byte(nzbContent), 0644))

	// Data segment found — threshold disabled so par2 NOT checked
	mockDownloadPool.EXPECT().BodyStream(gomock.Any(), dataSegID, gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, w io.Writer, _ ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error) {
			_, _ = w.Write([]byte("data"))
			return &nntppool.ArticleBody{}, nil
		}).Times(1)

	// Par2 must NOT be probed at all when the threshold check is disabled
	mockDownloadPool.EXPECT().BodyStream(gomock.Any(), par2SegID, gomock.Any(), gomock.Any()).Times(0)
	mockDownloadPool.EXPECT().StatMany(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	// No Create, no Repair
	mockPar2Executor.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	mockPar2Executor.EXPECT().Repair(gomock.Any(), gomock.Any()).Times(0)

	err := RepairNzb(ctx, cfg, mockDownloadPool, nil, mockPar2Executor, nzbFile, "", tmpDir)
	require.NoError(t, err)
}

func TestRepairNzb_NoPar2Files(t *testing.T) {
	// Setup
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	cfg := config.Config{
		DownloadWorkers: 1,
		UploadWorkers:   1,
	}

	mockDownloadPool := mocks.NewMockDownloader(ctrl)
	mockUploadPool := mocks.NewMockUploader(ctrl)
	mockPar2Executor := mocks.NewMockPar2Executor(ctrl)

	inputDir := t.TempDir()
	tmpDir := t.TempDir()
	outputDir := t.TempDir()
	outputFile := filepath.Join(outputDir, "output.nzb")
	nzbFile := filepath.Join(inputDir, "input_no_par2.nzb")

	dataFileName := "test_data.mkv"
	segmentID := "dataSegment@test"

	// Create an NZB file with only a data file
	nzbContent := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
 <head>
  <meta type="category">Misc</meta>
  <meta type="name">Test Release No Par2</meta>
 </head>
 <file poster="test@example.com" date="1678886400" subject="[1/1] %s - &quot;test_data.mkv&quot; yEnc (1/1)">
  <groups>
   <group>alt.binaries.test</group>
  </groups>
  <segments>
   <segment bytes="100" number="1">%s</segment>
  </segments>
 </file>
</nzb>`, dataFileName, segmentID)
	err := os.WriteFile(nzbFile, []byte(nzbContent), 0644)
	require.NoError(t, err)

	// --- Mock Expectations ---
	// No downloads, repairs, or uploads should be attempted as there are no par files.
	// We expect the function to return early.
	mockDownloadPool.EXPECT().BodyStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0) // No downloads expected
	mockPar2Executor.EXPECT().Repair(gomock.Any(), gomock.Any()).Times(0)                                 // No repair expected
	mockUploadPool.EXPECT().PostYenc(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)     // No uploads expected

	// --- Call the function ---
	err = RepairNzb(ctx, cfg, mockDownloadPool, mockUploadPool, mockPar2Executor, nzbFile, outputFile, tmpDir)
	require.NoError(t, err) // Expecting graceful exit with no error

	// --- Assertions ---
	// 1. Check that the output NZB file was NOT created
	_, err = os.Stat(outputFile)
	assert.True(t, os.IsNotExist(err), "Output NZB file should NOT exist when no par2 files are present")
}

// statResults returns a closed channel carrying the given STAT outcomes, as the
// pool's StatMany does once a sweep completes.
func statResults(results ...nntppool.StatManyResult) <-chan nntppool.StatManyResult {
	ch := make(chan nntppool.StatManyResult, len(results))
	for _, r := range results {
		ch <- r
	}
	close(ch)

	return ch
}

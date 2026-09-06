package repairnzb

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Tensai75/nzbparser"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/kipsilabs/nzb-repair/internal/config"
	"github.com/mnightingale/rapidyenc"
	"github.com/sourcegraph/conc/pool"
)

// Downloader fetches articles and checks their availability.
// *nntppool.Client satisfies this interface.
type Downloader interface {
	BodyStream(ctx context.Context, messageID string, w io.Writer, onMeta ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error)
	StatMany(ctx context.Context, messageIDs []string, opts nntppool.StatManyOptions) <-chan nntppool.StatManyResult
}

// Uploader posts articles.
// *nntppool.Client satisfies this interface.
type Uploader interface {
	PostYenc(ctx context.Context, headers nntppool.PostHeaders, body io.Reader, meta rapidyenc.Meta) (*nntppool.PostResult, error)
}

const defaultSegmentSize = 750_000 // bytes per uploaded segment for recreated par2 files

// isRetryableDownloadErr reports whether a segment download error is transient
// and worth retrying. Terminal cases are handled by the caller:
//   - context.Canceled: the whole repair is being torn down.
//   - ErrArticleNotFound: the segment is genuinely missing and routed for repair.
//   - ErrQuotaExceeded: the provider quota won't recover within this run.
//
// Everything else (including the generic wrapped "all providers exhausted: ...
// 502 too many connections" error, which is NOT the ErrServiceUnavailable
// sentinel) is treated as transient.
func isRetryableDownloadErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, nntppool.ErrArticleNotFound) {
		return false
	}
	if errors.Is(err, nntppool.ErrQuotaExceeded) {
		return false
	}
	return true
}

// backoffDelay returns an exponential backoff with full jitter for the given
// attempt, capped at max. The result is in the range [capped/2, capped] where
// capped = min(base*2^attempt, max).
func backoffDelay(attempt int, base, max time.Duration) time.Duration {
	capped := base << attempt // base * 2^attempt
	if capped <= 0 || capped > max {
		// Guard against shift overflow and cap at max.
		capped = max
	}
	half := capped / 2
	return half + time.Duration(mrand.Int64N(int64(half)+1)) // [half, capped]
}

// retryDownload runs op, retrying transient failures with backoff. Non-retryable
// errors are returned immediately for the caller to classify. After cfg.DownloadRetries
// failed retries the last error is returned. ctx cancellation aborts the wait.
func retryDownload(ctx context.Context, cfg config.Config, label string, op func() error) error {
	var lastErr error
	for attempt := 0; ; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		if !isRetryableDownloadErr(err) {
			return err
		}
		lastErr = err
		if int64(attempt) >= cfg.DownloadRetries {
			return lastErr
		}
		delay := backoffDelay(attempt, cfg.DownloadRetryBaseDelay, cfg.DownloadRetryMaxDelay)
		slog.WarnContext(ctx, fmt.Sprintf("transient error downloading %s, retry %d/%d in %s: %v",
			label, attempt+1, cfg.DownloadRetries, delay, err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// countMissingParSegments reports how many par2 segments are unavailable.
//
// Availability is probed with STAT rather than by fetching bodies: STAT carries
// no payload, so the whole set can be swept concurrently at round-trip cost
// instead of transferring — and discarding — every par2 segment. The sweep runs
// on the background lane so it cannot starve foreground downloads.
func countMissingParSegments(
	ctx context.Context,
	cfg config.Config,
	downloadPool Downloader,
	parFiles []nzbparser.NzbFile,
) (missing, total int64, err error) {
	ids := make([]string, 0)
	for _, f := range parFiles {
		for _, s := range f.Segments {
			ids = append(ids, s.Id)
		}
	}

	if len(ids) == 0 {
		return 0, 0, nil
	}

	results := downloadPool.StatMany(ctx, ids, nntppool.StatManyOptions{
		Concurrency: cfg.StatConcurrency,
		Background:  true,
	})

	for r := range results {
		total++

		switch {
		case r.Err == nil:
		case errors.Is(r.Err, nntppool.ErrArticleNotFound):
			missing++
		case errors.Is(r.Err, context.Canceled):
		default:
			// Drain the channel so the sweep's goroutines are not left blocked.
			go func() {
				for range results { //nolint:revive // drain only
				}
			}()

			return missing, total, fmt.Errorf("error checking par2 segment %s: %w", r.MessageID, r.Err)
		}
	}

	if ctx.Err() != nil {
		return missing, total, ctx.Err()
	}

	return missing, total, nil
}

// uploadPar2Files uploads generated par2 files and returns new NzbFile entries.
func uploadPar2Files(
	ctx context.Context,
	par2FilePaths []string,
	cfg config.Config,
	uploadPool Uploader,
	nzb *nzbparser.Nzb,
) (newFiles []nzbparser.NzbFile, err error) {
	groups := []string{}
	if len(nzb.Files) > 0 {
		groups = nzb.Files[0].Groups
	}

	// One pool spans every par2 file: uploading a file at a time would drain
	// the connection pool at each file boundary.
	p := pool.New().WithContext(ctx).
		WithMaxGoroutines(workerCount(cfg.UploadWorkers)).
		WithCancelOnError()

	var openFiles []*os.File

	defer func() {
		for _, f := range openFiles {
			_ = f.Close()
		}
	}()

	newFiles = make([]nzbparser.NzbFile, len(par2FilePaths))

	for fileIdx, path := range par2FilePaths {
		// Segments are read straight off disk through per-segment section
		// readers. Slurping each file into memory would hold every par2 file
		// resident at once, which at the default redundancy is a sizeable
		// fraction of the release.
		fh, openErr := os.Open(path)
		if openErr != nil {
			// The shared pool already has segments in flight for earlier files;
			// drain it before the deferred close reaches their handles.
			_ = p.Wait()

			return nil, fmt.Errorf("failed to open par2 file %s: %w", path, openErr)
		}

		openFiles = append(openFiles, fh)

		info, statErr := fh.Stat()
		if statErr != nil {
			_ = p.Wait()

			return nil, fmt.Errorf("failed to stat par2 file %s: %w", path, statErr)
		}

		filename := filepath.Base(path)
		fileSize := info.Size()
		segSize := int64(defaultSegmentSize)
		totalSegments := int((fileSize + segSize - 1) / segSize)

		segments := make([]nzbparser.NzbSegment, totalSegments)
		newFiles[fileIdx] = nzbparser.NzbFile{
			Filename:      filename,
			Basefilename:  filename,
			Poster:        "nzb-repair",
			Date:          int(time.Now().Unix()),
			TotalSegments: totalSegments,
			Bytes:         fileSize,
			Groups:        groups,
			Segments:      segments,
		}

		for i := range totalSegments {
			segNum := i + 1
			offset := int64(i) * segSize
			partSize := min(segSize, fileSize-offset)

			p.Go(func(ctx context.Context) error {
				msgId := generateRandomMessageID()
				subject := fmt.Sprintf("[1/1] \"%s\" yEnc (%d/%d)", filename, segNum, totalSegments)
				fName := filename

				if cfg.Upload.ObfuscationPolicy != config.ObfuscationPolicyNone {
					fName = rand.Text()
					subject = rand.Text()
				}

				headers := nntppool.PostHeaders{
					From:       "nzb-repair",
					Subject:    subject,
					Newsgroups: groups,
					MessageID:  fmt.Sprintf("<%s>", msgId),
				}
				meta := rapidyenc.Meta{
					FileName:   fName,
					FileSize:   fileSize,
					PartSize:   partSize,
					PartNumber: int64(segNum),
					Offset:     offset,
					TotalParts: int64(totalSegments),
				}

				// The pool consumes the body exactly once, so a single-pass
				// section reader over the open file is enough.
				body := io.NewSectionReader(fh, offset, partSize)
				if _, err := uploadPool.PostYenc(ctx, headers, body, meta); err != nil {
					return fmt.Errorf("failed to upload par2 segment: %w", err)
				}

				segments[i] = nzbparser.NzbSegment{
					Bytes:  int(partSize),
					Number: segNum,
					Id:     msgId,
				}

				return nil
			})
		}

		slog.InfoContext(ctx, "Queued par2 file for upload", "filename", filename, "segments", totalSegments)
	}

	if err := p.Wait(); err != nil {
		return nil, err
	}

	return newFiles, nil
}

func RepairNzb(
	ctx context.Context,
	cfg config.Config,
	downloadPool Downloader,
	uploadPool Uploader,
	par2Executor Par2Executor,
	nzbFile string,
	outputFile string,
	tmpDir string,
) error {
	content, err := os.Open(nzbFile)
	if err != nil {
		return err
	}

	nzb, err := nzbparser.Parse(content)
	if err != nil {
		_ = content.Close()

		return err
	}

	_ = content.Close()

	parFiles, restFiles := splitParWithRest(nzb)
	if len(parFiles) == 0 {
		slog.InfoContext(ctx, "No par2 files found in NZB, stopping repair.")
		return nil
	}

	brokenSegments := make(map[*nzbparser.NzbFile][]brokenSegment, 0)
	brokenSegmentCh := make(chan brokenSegment, 100)

	bswg := &sync.WaitGroup{}
	// goroutine to listen for broken segments
	bswg.Add(1)
	go func() {
		defer bswg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-brokenSegmentCh:
				if !ok {
					return
				}

				if _, ok := brokenSegments[s.file]; !ok {
					brokenSegments[s.file] = make([]brokenSegment, 0)
				}

				brokenSegments[s.file] = append(brokenSegments[s.file], s)
			}
		}
	}()

	if len(restFiles) == 0 {
		slog.InfoContext(ctx, "No files to repair, stopping repair.")

		return nil
	}

	firstFile := restFiles[0]
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		if !errors.Is(err, os.ErrExist) {
			slog.With("err", err).ErrorContext(ctx, "failed to ensure temp folder exists")
			return err
		}
	}

	defer func() {
		slog.InfoContext(ctx, "Cleaning up temporary directory", "path", tmpDir)
		if err := os.RemoveAll(tmpDir); err != nil {
			slog.ErrorContext(ctx, "Failed to clean up temporary directory", "path", tmpDir, "error", err)
		}
	}()

	// Download every segment of every file through one pool, so connections
	// stay saturated across file boundaries.
	startTime := time.Now()
	if err := downloadAll(ctx, cfg, downloadPool, restFiles, brokenSegmentCh, tmpDir); err != nil {
		slog.With("err", err).ErrorContext(ctx, "failed to download files")
	}

	close(brokenSegmentCh)
	bswg.Wait()

	if ctx.Err() != nil {
		slog.With("err", err).ErrorContext(ctx, "repair canceled")

		return nil
	}

	elapsed := time.Since(startTime)

	slog.InfoContext(ctx, fmt.Sprintf("%d files downloaded in %s", len(restFiles), elapsed))

	// Check par2 threshold (if configured)
	needsParRecreation := false
	if cfg.Par2RecreateThreshold > 0 && len(parFiles) > 0 {
		missing, total, countErr := countMissingParSegments(ctx, cfg, downloadPool, parFiles)
		if countErr != nil {
			slog.With("err", countErr).WarnContext(ctx, "failed to count missing par2 segments, skipping threshold check")
		} else if total > 0 {
			ratio := float64(missing) / float64(total)
			slog.InfoContext(ctx, fmt.Sprintf("par2 segments: %d/%d missing (%.1f%%)", missing, total, ratio*100))
			if ratio >= cfg.Par2RecreateThreshold {
				slog.InfoContext(ctx, "par2 missing threshold exceeded, will recreate par2 set")
				needsParRecreation = true
			}
		}
	}

	if len(brokenSegments) == 0 && !needsParRecreation {
		slog.InfoContext(ctx, "No broken segments and par2 is healthy, stopping repair.")

		return nil
	}

	// Repair broken data segments (if any)
	if len(brokenSegments) > 0 {
		slog.InfoContext(ctx, fmt.Sprintf("%d broken segments found. Downloading par2 files", len(brokenSegments)))
		if err := downloadAll(ctx, cfg, downloadPool, parFiles, nil, tmpDir); err != nil {
			slog.With("err", err).InfoContext(ctx, "failed to download par2 files, cancelling repair")
		}

		if err := par2Executor.Repair(ctx, tmpDir); err != nil {
			slog.With("err", err).ErrorContext(ctx, "failed to repair files")
		}

		startTime = time.Now()
		if err := replaceBrokenSegments(ctx, brokenSegments, tmpDir, cfg, uploadPool, nzb); err != nil {
			slog.With("err", err).ErrorContext(ctx, "failed to upload repaired files")
			return err
		}
		slog.InfoContext(ctx, fmt.Sprintf("%d broken segments uploaded in %s", len(brokenSegments), time.Since(startTime)))
	}

	// Recreate par2 set (if threshold exceeded)
	if needsParRecreation {
		slog.InfoContext(ctx, "Recreating par2 set")
		newPar2Paths, createErr := par2Executor.Create(ctx, tmpDir, cfg.Par2RecreateRedundancy)
		if createErr != nil {
			slog.With("err", createErr).ErrorContext(ctx, "failed to create new par2 set")
			return createErr
		}

		if len(newPar2Paths) > 0 {
			newPar2Files, uploadErr := uploadPar2Files(ctx, newPar2Paths, cfg, uploadPool, nzb)
			if uploadErr != nil {
				slog.With("err", uploadErr).ErrorContext(ctx, "failed to upload new par2 files")
				return uploadErr
			}

			// Replace par2 entries in NZB: remove old, add new
			filtered := nzb.Files[:0]
			for _, f := range nzb.Files {
				if !parregexp.MatchString(f.Filename) {
					filtered = append(filtered, f)
				}
			}
			nzb.Files = append(filtered, newPar2Files...)
			slog.InfoContext(ctx, fmt.Sprintf("Replaced par2 set with %d new files", len(newPar2Files)))
		}
	}

	// write the repaired nzb file
	var nzbFileName string
	if outputFile != "" {
		nzbFileName = outputFile
	} else {
		inputFileFolder := filepath.Dir(nzbFile)
		nzbFileName = filepath.Join(inputFileFolder, fmt.Sprintf("%s.repaired.nzb", firstFile.Basefilename))
	}

	// Ensure output directory exists
	outputDirPath := filepath.Dir(nzbFileName)
	if err := os.MkdirAll(outputDirPath, 0755); err != nil {
		if !errors.Is(err, os.ErrExist) {
			slog.With("err", err).ErrorContext(ctx, "failed to create output directory")
			return err
		}
	}

	b, err := nzbparser.Write(nzb)
	if err != nil {
		slog.With("err", err).ErrorContext(ctx, "failed to write repaired nzb file")

		return err
	}

	nzbFileHandle, err := os.Create(nzbFileName)
	if err != nil {
		slog.With("err", err).ErrorContext(ctx, "failed to create repaired nzb file")

		return err
	}

	defer func() {
		_ = nzbFileHandle.Close()
	}()

	if _, err := nzbFileHandle.Write(b); err != nil {
		slog.With("err", err).ErrorContext(ctx, "failed to write repaired nzb file")

		return err
	}

	slog.InfoContext(ctx, fmt.Sprintf("Repaired nzb file written to %s", nzbFileName))
	slog.InfoContext(ctx, fmt.Sprintf("%d broken segments uploaded in %s", len(brokenSegments), time.Since(startTime)))
	slog.InfoContext(ctx, "Repair completed successfully")

	return nil
}

func replaceBrokenSegments(
	ctx context.Context,
	brokenSegments map[*nzbparser.NzbFile][]brokenSegment,
	tmpFolder string,
	cfg config.Config,
	uploadPool Uploader,
	nzb *nzbparser.Nzb,
) error {
	// One pool spans every repaired file: a pool per file drains to zero at
	// each file boundary and pays the ramp-up again for the next one.
	p := pool.New().WithContext(ctx).
		WithMaxGoroutines(workerCount(cfg.UploadWorkers)).
		WithCancelOnError()

	var openFiles []*os.File

	defer func() {
		for _, f := range openFiles {
			_ = f.Close()
		}
	}()

	for nzbFile, bs := range brokenSegments {
		if ctx.Err() != nil {
			slog.ErrorContext(ctx, "repair canceled")

			return nil
		}

		tmpFile, openErr := os.Open(filepath.Join(tmpFolder, nzbFile.Filename))
		if openErr != nil {
			slog.With("err", openErr).ErrorContext(ctx, "failed to open file")
			// Uploads for earlier files are already running against handles in
			// openFiles; drain the pool before the deferred close reaches them.
			_ = p.Wait()

			return openErr
		}

		openFiles = append(openFiles, tmpFile)

		fs, statErr := tmpFile.Stat()
		if statErr != nil {
			slog.With("err", statErr).ErrorContext(ctx, "failed to get file info")
			_ = p.Wait()

			return statErr
		}

		fileSize := fs.Size()
		totalSegments := int64(nzbFile.TotalSegments)
		// s.segment.Bytes is the yEnc-encoded article size (~10% larger than decoded binary).
		// The repaired file contains decoded binary data, so compute offsets from actual file size.
		decodedSegSize := (fileSize + totalSegments - 1) / totalSegments

		for _, s := range bs {
			p.Go(func(ctx context.Context) error {
				if ctx.Err() != nil {
					return nil
				}

				// Get the segment from the file using decoded segment boundaries.
				segNum := int64(s.segment.Number)
				readOffset := (segNum - 1) * decodedSegSize
				readSize := decodedSegSize
				if segNum >= totalSegments {
					readSize = fileSize - readOffset
				}

				buff := make([]byte, readSize)
				_, err := tmpFile.ReadAt(buff, readOffset)
				if err != nil {
					slog.With("err", err).ErrorContext(ctx, "failed to read segment")

					return err
				}

				partSize := readSize
				date := time.Unix(int64(nzbFile.Date), 0)

				subject := fmt.Sprintf("[%v/%v] %v - \"\" yEnc (%v/%v)", s.file.Number, nzb.TotalFiles, s.file.Filename, int64(s.segment.Number), s.file.TotalSegments)

				var fName string

				if cfg.Upload.ObfuscationPolicy == config.ObfuscationPolicyNone {
					fName = s.file.Filename
				} else {
					fName = rand.Text()
					subject = rand.Text()
				}

				msgId := generateRandomMessageID()

				headers := nntppool.PostHeaders{
					From:       nzbFile.Poster,
					Subject:    subject,
					Newsgroups: nzbFile.Groups,
					MessageID:  fmt.Sprintf("<%s>", msgId),
					Date:       date.UTC(),
				}

				meta := rapidyenc.Meta{
					FileName:   fName,
					FileSize:   fileSize,
					PartSize:   partSize,
					PartNumber: int64(s.segment.Number),
					TotalParts: int64(s.file.TotalSegments),
				}

				// Upload the segment
				_, err = uploadPool.PostYenc(ctx, headers, bytes.NewReader(buff), meta)
				if err != nil {
					slog.With("err", err).ErrorContext(ctx, "failed to upload segment")

					return err
				}

				slog.InfoContext(ctx, fmt.Sprintf("Uploaded segment %s", s.segment.Id))
				nzbFile.Segments[s.segment.Number-1].Id = msgId

				return nil
			})
		}

	}

	if err := p.Wait(); err != nil {
		slog.With("err", err).ErrorContext(ctx, "failed to upload segments")

		return err
	}

	// Splice the repaired files back into the NZB only once every segment has
	// been posted, so a partial upload never rewrites the manifest.
	for nzbFile, bs := range brokenSegments {
		slog.InfoContext(ctx, fmt.Sprintf("Uploaded %d segments for file %s", len(bs), nzbFile.Filename))

		for i, f := range nzb.Files {
			if f.Filename == nzbFile.Filename {
				nzb.Files[i] = *nzbFile
				break
			}
		}
	}

	return nil
}

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
	"sync/atomic"
	"time"

	"github.com/Tensai75/nzbparser"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/kipsilabs/nzb-repair/internal/config"
	"github.com/k0kubun/go-ansi"
	"github.com/mnightingale/rapidyenc"
	"github.com/schollz/progressbar/v3"
	"github.com/sourcegraph/conc/pool"
)

// NNTPPool is the interface for NNTP operations used by the repair process.
// *nntppool.Client satisfies this interface.
type NNTPPool interface {
	BodyStream(ctx context.Context, messageID string, w io.Writer, onMeta ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error)
	PostYenc(ctx context.Context, headers nntppool.PostHeaders, body io.Reader, meta rapidyenc.Meta) (*nntppool.PostResult, error)
	Close() error
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

// countMissingParSegments checks par2 segments without writing to disk.
// Returns (missing, total, error).
func countMissingParSegments(
	ctx context.Context,
	cfg config.Config,
	downloadPool NNTPPool,
	parFiles []nzbparser.NzbFile,
) (missing, total int64, err error) {
	for _, f := range parFiles {
		for _, s := range f.Segments {
			total++
			if ctx.Err() != nil {
				return missing, total, ctx.Err()
			}
			segErr := retryDownload(ctx, cfg, s.Id, func() error {
				_, e := downloadPool.BodyStream(ctx, s.Id, io.Discard)
				return e
			})
			if segErr != nil {
				if errors.Is(segErr, nntppool.ErrArticleNotFound) {
					missing++
				} else if !errors.Is(segErr, context.Canceled) {
					return missing, total, fmt.Errorf("error checking par2 segment %s: %w", s.Id, segErr)
				}
			}
		}
	}
	return missing, total, nil
}

// uploadPar2Files uploads generated par2 files and returns new NzbFile entries.
func uploadPar2Files(
	ctx context.Context,
	par2FilePaths []string,
	cfg config.Config,
	uploadPool NNTPPool,
	nzb *nzbparser.Nzb,
) ([]nzbparser.NzbFile, error) {
	var newFiles []nzbparser.NzbFile

	groups := []string{}
	if len(nzb.Files) > 0 {
		groups = nzb.Files[0].Groups
	}

	for _, path := range par2FilePaths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read par2 file %s: %w", path, err)
		}

		filename := filepath.Base(path)
		fileSize := int64(len(data))
		segSize := defaultSegmentSize
		totalSegments := (len(data) + segSize - 1) / segSize

		nzbFile := nzbparser.NzbFile{
			Filename:      filename,
			Basefilename:  filename,
			Poster:        "nzb-repair",
			Date:          int(time.Now().Unix()),
			TotalSegments: totalSegments,
			Bytes:         fileSize,
			Groups:        groups,
		}

		p := pool.New().WithContext(ctx).
			WithMaxGoroutines(cfg.UploadWorkers).
			WithCancelOnError()

		segments := make([]nzbparser.NzbSegment, totalSegments)
		for i := range totalSegments {
			segNum := i + 1
			start := i * segSize
			end := start + segSize
			if end > len(data) {
				end = len(data)
			}
			chunk := make([]byte, end-start)
			copy(chunk, data[start:end])

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
					PartSize:   int64(len(chunk)),
					PartNumber: int64(segNum),
					Offset:     int64(start),
					TotalParts: int64(totalSegments),
				}
				if _, err := uploadPool.PostYenc(ctx, headers, bytes.NewReader(chunk), meta); err != nil {
					return fmt.Errorf("failed to upload par2 segment: %w", err)
				}
				segments[i] = nzbparser.NzbSegment{
					Bytes:  len(chunk),
					Number: segNum,
					Id:     msgId,
				}
				return nil
			})
		}

		if err := p.Wait(); err != nil {
			return nil, err
		}

		nzbFile.Segments = segments
		newFiles = append(newFiles, nzbFile)
		slog.InfoContext(ctx, "Uploaded par2 file", "filename", filename, "segments", totalSegments)
	}

	return newFiles, nil
}

func RepairNzb(
	ctx context.Context,
	cfg config.Config,
	downloadPool NNTPPool,
	uploadPool NNTPPool,
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

	// Download files
	startTime := time.Now()
	for _, f := range restFiles {
		if ctx.Err() != nil {
			slog.With("err", err).ErrorContext(ctx, "repair canceled")

			return nil
		}

		err := downloadWorker(ctx, cfg, downloadPool, f, brokenSegmentCh, tmpDir)
		if err != nil {
			slog.With("err", err).ErrorContext(ctx, "failed to download file")
		}

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
		for _, f := range parFiles {
			if ctx.Err() != nil {
				return nil
			}

			if err := downloadWorker(ctx, cfg, downloadPool, f, nil, tmpDir); err != nil {
				slog.With("err", err).InfoContext(ctx, "failed to download par2 file, cancelling repair")
			}
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
	uploadPool NNTPPool,
	nzb *nzbparser.Nzb,
) error {
	for nzbFile, bs := range brokenSegments {
		if ctx.Err() != nil {
			slog.ErrorContext(ctx, "repair canceled")

			return nil
		}

		tmpFile, err := os.Open(filepath.Join(tmpFolder, nzbFile.Filename))
		if err != nil {
			slog.With("err", err).ErrorContext(ctx, "failed to open file")

			return err
		}

		fs, err := tmpFile.Stat()
		if err != nil {
			slog.With("err", err).ErrorContext(ctx, "failed to get file info")
			_ = tmpFile.Close()

			return err
		}

		fileSize := fs.Size()
		totalSegments := int64(nzbFile.TotalSegments)
		// s.segment.Bytes is the yEnc-encoded article size (~10% larger than decoded binary).
		// The repaired file contains decoded binary data, so compute offsets from actual file size.
		decodedSegSize := (fileSize + totalSegments - 1) / totalSegments

		p := pool.New().WithContext(ctx).
			WithMaxGoroutines(cfg.UploadWorkers).
			WithCancelOnError()

		for _, s := range bs {
			p.Go(func(ctx context.Context) error {
				if ctx.Err() != nil {
					slog.With("err", err).ErrorContext(ctx, "repair canceled")

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

		if err := p.Wait(); err != nil {
			slog.With("err", err).ErrorContext(ctx, "failed to upload segments")
			_ = tmpFile.Close()

			return err
		}

		_ = tmpFile.Close()
		slog.InfoContext(ctx, fmt.Sprintf("Uploaded %d segments for file %s", len(bs), nzbFile.Filename))

		// Replace the original broken file in the nzb with the repaired version
		for i, f := range nzb.Files {
			if f.Filename == nzbFile.Filename {
				nzb.Files[i] = *nzbFile
				break
			}
		}
	}

	return nil
}

func downloadWorker(
	ctx context.Context,
	config config.Config,
	downloadPool NNTPPool,
	file nzbparser.NzbFile,
	brokenSegmentCh chan<- brokenSegment,
	tmpFolder string,
) error {
	brokenSegmentCounter := atomic.Int64{}

	p := pool.New().WithContext(ctx).
		WithMaxGoroutines(config.DownloadWorkers).
		WithCancelOnError()

	slog.InfoContext(ctx, fmt.Sprintf("Starting downloading file %s", file.Filename))

	filePath := filepath.Join(tmpFolder, file.Filename)

	// Check if file exists
	if _, err := os.Stat(filePath); err == nil {
		slog.InfoContext(ctx, fmt.Sprintf("File %s already exists, skipping download", file.Filename))
		return nil
	}

	fileWriter, err := os.Create(filePath)
	if err != nil {
		slog.With("err", err).ErrorContext(ctx, "failed to create file: %v")

		return fmt.Errorf("failed to create file: %w", err)
	}

	defer func() {
		_ = fileWriter.Close()
	}()

	bar := progressbar.NewOptions(int(file.Bytes),
		progressbar.OptionSetWriter(ansi.NewAnsiStdout()),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(15),
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowTotalBytes(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	c, cancel := context.WithCancel(context.Background())
	defer cancel()

	once := sync.Once{}

	for _, s := range file.Segments {
		select {
		case <-c.Done():
			return nil
		case <-ctx.Done():
			return nil
		default:
			p.Go(func(c context.Context) error {
				buff := bytes.NewBuffer(make([]byte, 0))
				err := retryDownload(c, config, s.Id, func() error {
					buff.Reset() // discard partial writes from a failed attempt
					_, e := downloadPool.BodyStream(c, s.Id, buff)
					return e
				})
				if err != nil {
					if errors.Is(err, nntppool.ErrArticleNotFound) {
						if brokenSegmentCh != nil {
							slog.DebugContext(ctx, fmt.Sprintf("segment %s not found, sending for repair: %v", s.Id, err))

							brokenSegmentCh <- brokenSegment{
								segment: &s,
								file:    &file,
							}
							brokenSegmentCounter.Add(1)

							// Recalculate segment size for wrong segment sizes
							once.Do(func() {
								for _, s := range file.Segments {
									s.Bytes = buff.Len()
								}
							})
						} else if !errors.Is(err, context.Canceled) {
							return fmt.Errorf("segment %v not found", s.Id)
						}

						return nil
					}

					if errors.Is(err, context.Canceled) {
						return nil
					}

					slog.ErrorContext(ctx, fmt.Sprintf("failed to download segment %s canceling the repair: %v", s.Id, err))
					cancel()

					return err
				}

				start := (s.Number - 1) * buff.Len()

				_, err = fileWriter.WriteAt(buff.Bytes(), int64(start))
				if err != nil {
					slog.With("err", err).ErrorContext(ctx, "failed to write segment")

					return err
				}

				_ = bar.Add(s.Bytes)

				return nil
			})
		}
	}

	if err := p.Wait(); err != nil {
		return err
	}

	return nil
}

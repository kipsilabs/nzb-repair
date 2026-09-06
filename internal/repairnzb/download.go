package repairnzb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/Tensai75/nzbparser"
	nntppool "github.com/javi11/nntppool/v4"
	"github.com/k0kubun/go-ansi"
	"github.com/kipsilabs/nzb-repair/internal/config"
	"github.com/schollz/progressbar/v3"
	"github.com/sourcegraph/conc/pool"
)

// segmentWriter places a segment's decoded bytes at an absolute offset in the
// destination file.
//
// It is positioned by SetBase from the article's own yEnc "=ypart begin="
// header, which the pool reports through its onMeta callback before the body
// streams. Writing at absolute offsets makes a retry idempotent: re-streaming a
// segment overwrites exactly the region the failed attempt touched.
type segmentWriter struct {
	dst     io.WriterAt
	base    int64
	off     int64
	written int64
	err     error
}

func newSegmentWriter(dst io.WriterAt, base int64) *segmentWriter {
	return &segmentWriter{dst: dst, base: base, off: base}
}

// SetBase repositions the segment. The header callback that drives it fires
// before the first body byte; a call arriving after bytes have already landed
// cannot move them, so it is recorded as an error rather than silently ignored.
func (w *segmentWriter) SetBase(off int64) {
	if w.written > 0 {
		if w.base != off {
			w.err = fmt.Errorf("segment offset %d reported after %d bytes written at %d",
				off, w.written, w.base)
		}

		return
	}

	w.base = off
	w.off = off
}

func (w *segmentWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	n, err := w.dst.WriteAt(p, w.off)
	w.off += int64(n)
	w.written += int64(n)

	if err != nil {
		return n, fmt.Errorf("write segment at offset %d: %w", w.base, err)
	}

	return n, nil
}

// Reset returns the writer to a fresh state for a retry of the same segment.
func (w *segmentWriter) Reset(base int64) {
	w.base = base
	w.off = base
	w.written = 0
	w.err = nil
}

// Err reports a repositioning failure detected during streaming.
func (w *segmentWriter) Err() error { return w.err }

// Written reports how many bytes reached the destination.
func (w *segmentWriter) Written() int64 { return w.written }

// workerCount clamps a configured worker budget to what conc accepts; a pool
// must have at least one goroutine.
func workerCount(n int) int {
	if n < 1 {
		return 1
	}

	return n
}

// segmentJob is one article to fetch, paired with the file it belongs to.
type segmentJob struct {
	file    *nzbparser.NzbFile
	segment nzbparser.NzbSegment
	dst     *os.File
}

// downloadAll fetches every segment of every file through a single worker pool.
//
// Scheduling across files rather than one file at a time keeps the connection
// pool saturated: a per-file pool drains to zero at each file boundary and pays
// the ramp-up again for the next one.
//
// Segments that the providers report as missing are sent to brokenSegmentCh for
// par2 repair; a nil channel makes a missing segment a hard error instead.
func downloadAll(
	ctx context.Context,
	cfg config.Config,
	downloadPool Downloader,
	files []nzbparser.NzbFile,
	brokenSegmentCh chan<- brokenSegment,
	tmpFolder string,
) error {
	jobs, handles, totalBytes, err := planDownload(files, tmpFolder)
	if err != nil {
		return err
	}

	defer func() {
		for _, f := range handles {
			_ = f.Close()
		}
	}()

	if len(jobs) == 0 {
		return nil
	}

	bar := newDownloadBar(totalBytes)

	p := pool.New().WithContext(ctx).
		WithMaxGoroutines(workerCount(cfg.DownloadWorkers)).
		WithCancelOnError()

	for _, job := range jobs {
		p.Go(func(ctx context.Context) error {
			return downloadSegment(ctx, cfg, downloadPool, job, brokenSegmentCh, bar)
		})
	}

	if err := p.Wait(); err != nil {
		return err
	}

	_ = bar.Finish()

	return nil
}

// planDownload flattens the files into a segment work list, creating one
// destination handle per file. Files already present on disk are skipped.
func planDownload(
	files []nzbparser.NzbFile,
	tmpFolder string,
) (jobs []segmentJob, handles []*os.File, totalBytes int64, err error) {
	for i := range files {
		file := &files[i]
		filePath := filepath.Join(tmpFolder, file.Filename)

		if _, statErr := os.Stat(filePath); statErr == nil {
			slog.Info(fmt.Sprintf("File %s already exists, skipping download", file.Filename))
			continue
		}

		fh, createErr := os.Create(filePath)
		if createErr != nil {
			for _, open := range handles {
				_ = open.Close()
			}

			return nil, nil, 0, fmt.Errorf("failed to create file %s: %w", filePath, createErr)
		}

		handles = append(handles, fh)
		totalBytes += file.Bytes

		for _, s := range file.Segments {
			jobs = append(jobs, segmentJob{file: file, segment: s, dst: fh})
		}
	}

	return jobs, handles, totalBytes, nil
}

func downloadSegment(
	ctx context.Context,
	cfg config.Config,
	downloadPool Downloader,
	job segmentJob,
	brokenSegmentCh chan<- brokenSegment,
	bar *progressbar.ProgressBar,
) error {
	if ctx.Err() != nil {
		return nil
	}

	// A single-part article carries no "=ypart" header and always starts the
	// file, so offset 0 is correct for it even if no header arrives.
	multipart := job.file.TotalSegments > 1
	w := newSegmentWriter(job.dst, 0)

	err := retryDownload(ctx, cfg, job.segment.Id, func() error {
		w.Reset(0)

		positioned := false
		onMeta := func(m nntppool.YEncMeta) {
			positioned = true

			w.SetBase(m.PartBegin)
		}

		if _, e := downloadPool.BodyStream(ctx, job.segment.Id, w, onMeta); e != nil {
			return e
		}

		if err := w.Err(); err != nil {
			return err
		}

		// Guessing a position from the segment number would silently corrupt
		// the file whenever the parts are not uniformly sized.
		if multipart && !positioned {
			return fmt.Errorf("segment %s: article carried no yEnc part header, "+
				"cannot place it within a %d-part file", job.segment.Id, job.file.TotalSegments)
		}

		return nil
	})

	if err != nil {
		return handleSegmentError(ctx, err, job, brokenSegmentCh)
	}

	_ = bar.Add64(w.Written())

	return nil
}

// handleSegmentError classifies a failed segment: a genuine miss is routed for
// par2 repair, cancellation is silent, anything else aborts the run.
func handleSegmentError(
	ctx context.Context,
	err error,
	job segmentJob,
	brokenSegmentCh chan<- brokenSegment,
) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}

	if errors.Is(err, nntppool.ErrArticleNotFound) {
		if brokenSegmentCh == nil {
			return fmt.Errorf("segment %v not found", job.segment.Id)
		}

		slog.DebugContext(ctx, fmt.Sprintf("segment %s not found, sending for repair", job.segment.Id))

		seg := job.segment
		select {
		case brokenSegmentCh <- brokenSegment{segment: &seg, file: job.file}:
		case <-ctx.Done():
		}

		return nil
	}

	slog.ErrorContext(ctx, fmt.Sprintf("failed to download segment %s, canceling the repair: %v", job.segment.Id, err))

	return err
}

func newDownloadBar(total int64) *progressbar.ProgressBar {
	return progressbar.NewOptions64(total,
		progressbar.OptionSetDescription("Downloading"),
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
}

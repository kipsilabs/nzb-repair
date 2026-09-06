package scanner

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/kipsilabs/nzb-repair/internal/queue"
	"github.com/opencontainers/selinux/pkg/pwalkdir"
)

// Scanner periodically scans directories for .nzb files.
type Scanner struct {
	dir          string
	queue        queue.Queuer
	log          *slog.Logger
	scanInterval time.Duration
	isScanning   bool
}

// NewScanner creates a new Scanner instance.
func New(dir string, q queue.Queuer, logger *slog.Logger, scanInterval time.Duration) *Scanner {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		logger.Warn("Failed to get absolute path for scan directory, relative paths might be inconsistent.", "directory", dir, "error", err)
		absDir = dir
	}

	return &Scanner{
		dir:          absDir,
		queue:        q,
		log:          logger.With("component", "scanner", "directory", absDir),
		scanInterval: scanInterval,
	}
}

// Run starts the periodic scanning process.
// It blocks until the context is canceled.
func (s *Scanner) Run(ctx context.Context) error {
	s.log.InfoContext(ctx, "Starting scanner", "interval", s.scanInterval)

	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()

	// Perform initial scan
	if err := s.scanDirectory(ctx, s.dir); err != nil {
		s.log.ErrorContext(ctx, "Initial scan failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			s.log.InfoContext(ctx, "Stopping scanner due to context cancellation")
			return ctx.Err()
		case <-ticker.C:
			if s.isScanning {
				s.log.DebugContext(ctx, "Skipping scan as previous scan is still in progress")
				continue
			}

			if err := s.scanDirectory(ctx, s.dir); err != nil {
				s.log.ErrorContext(ctx, "Scan failed", "error", err)
			}
		}
	}
}

// scanDirectory recursively scans a directory for .nzb files and adds them to the queue.
func (s *Scanner) scanDirectory(ctx context.Context, dirPath string) error {
	s.isScanning = true
	defer func() { s.isScanning = false }()

	s.log.InfoContext(ctx, "Starting directory scan", "directory", dirPath)
	startTime := time.Now()

	err := pwalkdir.Walk(dirPath, func(path string, info fs.DirEntry, walkErr error) error {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Handle errors during walking
		if walkErr != nil {
			s.log.WarnContext(ctx, "Error accessing path during scan", "path", path, "error", walkErr)
			if info != nil && info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Process NZB files
		if !info.IsDir() && strings.ToLower(filepath.Ext(info.Name())) == ".nzb" {
			s.log.DebugContext(ctx, "Found NZB file during scan", "path", path)
			s.addFileToQueue(ctx, path)
		}

		return nil
	})

	duration := time.Since(startTime)
	if err != nil && !errors.Is(err, context.Canceled) {
		s.log.ErrorContext(ctx, "Error during directory scan", "directory", dirPath, "duration", duration, "error", err)
		return fmt.Errorf("scan failed: %w", err)
	}

	s.log.InfoContext(ctx, "Finished scanning directory", "directory", dirPath, "duration", duration)
	return nil
}

// addFileToQueue handles the logic of validating and adding a file path to the queue.
func (s *Scanner) addFileToQueue(ctx context.Context, filePath string) {
	s.log.InfoContext(ctx, "Adding detected NZB file to queue", "path", filePath)

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		s.log.WarnContext(ctx, "Failed to get absolute path, using original", "path", filePath, "error", err)
		absPath = filePath
	}

	relPath, err := filepath.Rel(s.dir, absPath)
	if err != nil {
		s.log.ErrorContext(ctx, "Failed to calculate relative path, using base filename as fallback", "base_dir", s.dir, "file_path", absPath, "error", err)
		relPath = filepath.Base(absPath)
	}

	err = s.queue.AddJob(absPath, relPath)
	if err != nil {
		s.log.ErrorContext(ctx, "Failed to add job to queue", "path", absPath, "relative_path", relPath, "error", err)
	} else {
		s.log.InfoContext(ctx, "Successfully added job to queue", "path", absPath, "relative_path", relPath)
	}
}

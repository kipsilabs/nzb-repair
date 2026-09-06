package repairnzb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// fakeWriterAt records WriteAt calls against a growable in-memory buffer.
type fakeWriterAt struct {
	data []byte
}

func (f *fakeWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if end := int(off) + len(p); end > len(f.data) {
		grown := make([]byte, end)
		copy(grown, f.data)
		f.data = grown
	}

	copy(f.data[off:], p)

	return len(p), nil
}

func TestSegmentWriter(t *testing.T) {
	tests := []struct {
		name     string
		fallback int64
		setBase  *int64
		writes   []string
		want     string
	}{
		{
			name:     "writes at the fallback offset when no header arrives",
			fallback: 4,
			writes:   []string{"abc", "de"},
			want:     "\x00\x00\x00\x00abcde",
		},
		{
			name:     "header offset overrides the fallback",
			fallback: 100,
			setBase:  ptr(int64(2)),
			writes:   []string{"xy", "z"},
			want:     "\x00\x00xyz",
		},
		{
			name:     "offset zero writes at the start of the file",
			fallback: 0,
			setBase:  ptr(int64(0)),
			writes:   []string{"hello"},
			want:     "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := &fakeWriterAt{}
			w := newSegmentWriter(dst, tt.fallback)

			if tt.setBase != nil {
				w.SetBase(*tt.setBase)
			}

			for _, chunk := range tt.writes {
				if _, err := w.Write([]byte(chunk)); err != nil {
					t.Fatalf("Write(%q): %v", chunk, err)
				}
			}

			if err := w.Err(); err != nil {
				t.Fatalf("Err: %v", err)
			}

			if got := string(dst.data); got != tt.want {
				t.Errorf("got %q; want %q", got, tt.want)
			}
		})
	}
}

// TestSegmentWriterRepositionAfterWriteIsAnError guards against silently
// misplacing bytes if the pool ever reports the header late.
func TestSegmentWriterRepositionAfterWriteIsAnError(t *testing.T) {
	w := newSegmentWriter(&fakeWriterAt{}, 0)

	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	w.SetBase(500)

	if w.Err() == nil {
		t.Error("expected an error when the offset moves after bytes were written")
	}
}

// TestSegmentWriterResetRewritesSameRegion covers a retried download: the second
// attempt must land on the same region as the first, not after it.
func TestSegmentWriterReset(t *testing.T) {
	dst := &fakeWriterAt{}
	w := newSegmentWriter(dst, 3)

	if _, err := w.Write([]byte("XX")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	w.Reset(3)

	if _, err := w.Write([]byte("ab")); err != nil {
		t.Fatalf("Write after reset: %v", err)
	}

	if got, want := string(dst.data), "\x00\x00\x00ab"; got != want {
		t.Errorf("got %q; want %q", got, want)
	}
	if got, want := w.Written(), int64(2); got != want {
		t.Errorf("Written() = %d; want %d", got, want)
	}
}

// TestSegmentWriterRealFile exercises the *os.File path used in production.
func TestSegmentWriterRealFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.bin")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	defer func() { _ = f.Close() }()

	w := newSegmentWriter(f, 6)
	if _, err := w.Write([]byte("tail")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if want := []byte("\x00\x00\x00\x00\x00\x00tail"); !bytes.Equal(got, want) {
		t.Errorf("got %q; want %q", got, want)
	}
}

func ptr[T any](v T) *T { return &v }

func TestWorkerCount(t *testing.T) {
	tests := []struct {
		in, want int
	}{
		{-5, 1},
		{0, 1},
		{1, 1},
		{64, 64},
	}

	for _, tt := range tests {
		if got := workerCount(tt.in); got != tt.want {
			t.Errorf("workerCount(%d) = %d; want %d", tt.in, got, tt.want)
		}
	}
}

// TestDownloadAllZeroWorkersDoesNotPanic guards the conc contract: a pool must
// be created with at least one goroutine.
func TestDownloadAllZeroWorkersDoesNotPanic(t *testing.T) {
	cfg := downloadTestConfig(0)

	if _, err := os.Stat(t.TempDir()); err != nil {
		t.Fatalf("temp dir: %v", err)
	}

	// No files means no work, but the pool is still constructed.
	if err := downloadAll(context.Background(), cfg, nil, nil, nil, t.TempDir()); err != nil {
		t.Errorf("downloadAll with no files: %v", err)
	}
}

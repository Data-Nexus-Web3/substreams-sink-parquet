package sinker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type RotatingParquetWriter struct {
	store  Store
	opts   SinkerFactoryOptions
	schema *arrow.Schema
	part   *Partitioner
	subdir string

	activeStart uint64
	activeEnd   uint64
	activeName  string
	tmpPath     string

	f  *os.File
	pw *pqarrow.FileWriter

	mu sync.Mutex

	// stats
	rowsWritten   int64
	filesUploaded int64
	bytesUploaded int64
	lastFileName  string

	// async upload
	upCh chan uploadReq
	upWg sync.WaitGroup

	// upload telemetry
	inflightUploads int64
	uploadTotalDur  time.Duration
	lastUploadDur   time.Duration
	lastUploadBytes int64

	// guards and counters
	completedRanges map[string]struct{}
	currentFileRows int64
}

func NewRotatingParquetWriter(store Store, opts SinkerFactoryOptions, schema *arrow.Schema, part *Partitioner, subdir string) *RotatingParquetWriter {
	w := &RotatingParquetWriter{store: store, opts: opts, schema: schema, part: part, subdir: subdir, completedRanges: make(map[string]struct{})}
	if opts.UploadWorkers > 0 {
		w.upCh = make(chan uploadReq, opts.UploadWorkers*2)
		for i := 0; i < opts.UploadWorkers; i++ {
			w.upWg.Add(1)
			go w.uploadWorker()
		}
	}
	return w
}

func (w *RotatingParquetWriter) openNewFileUnlocked(rs, re uint64) error {
	name := w.part.FileName(rs, re)
	// Use distinct temp subdir for each table to avoid collisions
	tmpBase := tmpDirOrDefault(w.opts.TmpDir)
	if w.subdir != "" {
		tmpBase = filepath.Join(tmpBase, w.subdir)
		if err := os.MkdirAll(tmpBase, 0o755); err != nil {
			return fmt.Errorf("create temp subdir %s: %w", tmpBase, err)
		}
	}
	tmpPath := filepath.Join(tmpBase, name+".partial")
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp parquet %s: %w", tmpPath, err)
	}
	w.f = file
	w.tmpPath = tmpPath
	w.activeStart = rs
	w.activeEnd = re
	w.activeName = name
	w.currentFileRows = 0
	// Create parquet writer with proper properties
	if w.schema != nil {
		codec := compressionCodec(w.opts.Compression)
		props := []parquet.WriterProperty{parquet.WithCompression(codec)}
		if w.opts.CompressionLevel != 0 {
			props = append(props, parquet.WithCompressionLevel(w.opts.CompressionLevel))
		}
		// Apply row-group, dict, page, and stats options
		if w.opts.RowGroupRows > 0 {
			props = append(props, parquet.WithMaxRowGroupLength(int64(w.opts.RowGroupRows)))
		}
		props = append(props, parquet.WithDictionaryDefault(w.opts.DictEncoding))
		if w.opts.PageSize > 0 {
			props = append(props, parquet.WithDataPageSize(int64(w.opts.PageSize)))
		}
		props = append(props, parquet.WithStats(w.opts.ParquetStats))
		writerProps := parquet.NewWriterProperties(props...)
		arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
		pw, err := pqarrow.NewFileWriter(w.schema, w.f, writerProps, arrowProps)
		if err != nil {
			w.f.Close()
			w.f = nil
			os.Remove(tmpPath)
			return fmt.Errorf("init parquet writer: %w", err)
		}
		w.pw = pw
	}
	return nil
}

func (w *RotatingParquetWriter) AppendRecord(ctx context.Context, rec arrow.Record, block uint64) (rotated bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if we need to rotate
	rs, re := w.part.RangeFor(block)
	// If we haven't opened any file yet, emit empty files for all prior ranges
	// from configured start up to the current range start.
	if w.f == nil {
		w.backfillEmptyUpTo(ctx, rs)
	}
	if w.f == nil || rs != w.activeStart || re != w.activeEnd {
		// Need to rotate - finalize old file and open new one
		if w.f != nil {
			if err := w.finalizeUnlocked(ctx); err != nil {
				return false, err
			}
		}
		// Open new file
		if err := w.openNewFileUnlocked(rs, re); err != nil {
			return false, err
		}
	}

	// Write the record
	if w.pw != nil && rec != nil && rec.NumRows() > 0 {
		if err := w.pw.Write(rec); err != nil {
			return false, fmt.Errorf("parquet write: %w", err)
		}
		n := int64(rec.NumRows())
		w.rowsWritten += n
		w.currentFileRows += n
	}
	return false, nil
}

func (w *RotatingParquetWriter) finalizeUnlocked(ctx context.Context) error {
	// Early exit if nothing to finalize
	if w.f == nil && w.pw == nil && w.tmpPath == "" {
		return nil
	}

	// CRITICAL: pqarrow.FileWriter.Close() closes the underlying os.File
	// So we must NOT call w.f.Sync() or w.f.Close() after closing the parquet writer
	if w.pw != nil {
		if err := w.pw.Close(); err != nil {
			return fmt.Errorf("close parquet writer: %w", err)
		}
		w.pw = nil
		// The file is now closed by the parquet writer
		w.f = nil
	}

	// If we have a temp file to upload, do it now
	if w.tmpPath != "" {
		name := w.activeName
		if w.subdir != "" {
			name = w.subdir + "/" + name
		}
		tmp := w.tmpPath

		// Clear state BEFORE upload to prevent double-finalize
		w.tmpPath = ""
		w.activeStart = 0
		w.activeEnd = 0
		w.activeName = ""

		// stat size before upload for metrics
		var sz int64
		if st, err := os.Stat(tmp); err == nil {
			sz = st.Size()
		}
		if w.upCh != nil {
			// async upload
			w.upCh <- uploadReq{name: name, path: tmp, size: sz}
		} else {
			if err := uploadFile(ctx, w.store, name, tmp); err != nil {
				return fmt.Errorf("upload %s: %w", name, err)
			}
			if err := os.Remove(tmp); err != nil {
				fmt.Fprintf(os.Stderr, "warning: failed to remove temp file %s: %v\n", tmp, err)
			}
			w.filesUploaded++
			w.bytesUploaded += sz
			w.lastFileName = name
		}
		// mark this range as completed to avoid accidental re-opens
		w.completedRanges[name] = struct{}{}
	}

	return nil
}

// ensureEmptyPrevRangeIfNeeded emits a single empty Parquet file for the
// immediately previous range if no file was written for it yet.
func (w *RotatingParquetWriter) ensureEmptyPrevRangeIfNeeded(ctx context.Context, nextRangeStart uint64) {
	// Only applies after we've advanced beyond the writer's configured start
	if nextRangeStart <= w.part.start {
		return
	}
	prevStart := nextRangeStart - w.part.size
	prevEnd := nextRangeStart
	if prevStart < w.part.start {
		return
	}
	name := w.part.FileName(prevStart, prevEnd)
	if w.subdir != "" {
		name = w.subdir + "/" + name
	}
	if _, ok := w.completedRanges[name]; ok {
		return
	}
	// Create and immediately finalize an empty file
	if err := w.openNewFileUnlocked(prevStart, prevEnd); err == nil {
		_ = w.finalizeUnlocked(ctx)
	}
}

// backfillEmptyUpTo emits empty Parquet files for all partition ranges between
// the configured start and the provided nextRangeStart, if they have not been
// produced yet.
func (w *RotatingParquetWriter) backfillEmptyUpTo(ctx context.Context, nextRangeStart uint64) {
	if nextRangeStart <= w.part.start {
		return
	}
	// Iterate from p.start in partition steps up to nextRangeStart
	for rs := w.part.start; rs < nextRangeStart; rs += w.part.size {
		re := rs + w.part.size
		if w.part.end > 0 && re > w.part.end {
			re = w.part.end
		}
		name := w.part.FileName(rs, re)
		if w.subdir != "" {
			name = w.subdir + "/" + name
		}
		if _, ok := w.completedRanges[name]; ok {
			continue
		}
		if err := w.openNewFileUnlocked(rs, re); err == nil {
			_ = w.finalizeUnlocked(ctx)
		}
	}
}

func (w *RotatingParquetWriter) finalize(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.finalizeUnlocked(ctx)
}

func (w *RotatingParquetWriter) Close() error {
	// finalize current file
	_ = w.finalize(context.Background())
	// drain async uploads
	if w.upCh != nil {
		close(w.upCh)
		w.upWg.Wait()
	}
	return nil
}

type WriterStats struct {
	Files        int64
	Bytes        int64
	Rows         int64
	ActiveName   string
	ActiveStart  uint64
	ActiveEnd    uint64
	Inflight     int64
	LastUploadMs int64
	LastUploadB  int64
}

func (w *RotatingParquetWriter) Stats() WriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return WriterStats{
		Files:        w.filesUploaded,
		Bytes:        w.bytesUploaded,
		Rows:         w.rowsWritten,
		ActiveName:   w.activeName,
		ActiveStart:  w.activeStart,
		ActiveEnd:    w.activeEnd,
		Inflight:     w.inflightUploads,
		LastUploadMs: w.lastUploadDur.Milliseconds(),
		LastUploadB:  w.lastUploadBytes,
	}
}

// ---- helpers / interfaces ----

type Store interface {
	WriteObject(ctx context.Context, object string, content io.Reader) error
}

// dstore implements WriteObject(ctx, object, []byte); we'll accept that interface.

type BuilderLike interface {
	Schema() *arrow.Schema
}

func tmpDirOrDefault(v string) string {
	if v != "" {
		return v
	}
	return os.TempDir()
}

func uploadFile(ctx context.Context, store Store, name, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return store.WriteObject(ctx, name, f)
}

type uploadReq struct {
	name string
	path string
	size int64
}

func (w *RotatingParquetWriter) uploadWorker() {
	defer w.upWg.Done()
	for req := range w.upCh {
		start := time.Now()
		w.mu.Lock()
		w.inflightUploads++
		w.mu.Unlock()
		// best-effort context
		_ = uploadFile(context.Background(), w.store, req.name, req.path)
		_ = os.Remove(req.path)
		dur := time.Since(start)
		w.mu.Lock()
		w.inflightUploads--
		w.filesUploaded++
		w.bytesUploaded += req.size
		w.lastFileName = req.name
		w.lastUploadDur = dur
		w.lastUploadBytes = req.size
		w.uploadTotalDur += dur
		w.mu.Unlock()
	}
}

func compressionCodec(s string) compress.Compression {
	switch strings.ToLower(s) {
	case "snappy":
		return compress.Codecs.Snappy
	case "gzip":
		return compress.Codecs.Gzip
	case "lz4":
		return compress.Codecs.Lz4
	case "uncompressed", "none", "":
		return compress.Codecs.Uncompressed
	default:
		return compress.Codecs.Zstd
	}
}

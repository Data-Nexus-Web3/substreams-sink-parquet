package sinker

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	dstorepkg "github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type SinkerFactoryOptions struct {
	// Storage
	StoreURL     string
	OutputPrefix string
	TmpDir       string
	PadWidth     int

	// Partitioning
	StartBlock    uint64
	EndBlock      uint64
	PartitionSize uint64

	// Parquet tuning
	Compression      string
	CompressionLevel int
	RowGroupRows     int
	PageSize         int
	DictEncoding     bool
	TargetFileBytes  int64
	ParquetStats     bool

	// Processing buffer (post-undo)
	ProcessingBufferSize     int
	ProcessingBufferBytes    int64
	ProcessingBufferMaxItems int

	// Ops
	UndoBufferSize int
	CursorFile     string
	DebugMode      bool

	// Explosion
	Explode bool

	// Buffering / concurrency
	FlushInterval        time.Duration
	ExplodedWriteWorkers int
	UploadWorkers        int
}

type ParquetSinker struct {
	*shutter.Shutter
	*sink.Sinker

	logger *zap.Logger
	tracer logging.Tracer

	store Store
	conv  Converter

	// Exploded tables: per root repeated field
	exploded map[string]*ExplodedTable

	// Partitioning & writer
	partitioner *Partitioner
	writer      *RotatingParquetWriter

	// Cursor & buffering
	cursorFile string

	processingQueue       chan bufferedProcessItem
	processingBufferSize  int
	processingBufferBytes int64
	processingMaxItems    int

	// Undo buffer
	undoBufferSize int
	undoBuffer     *UndoBuffer

	// Buffering / partition tracking
	mainBufferedRows int
	mainLastFlush    time.Time
	mainRangeStart   uint64

	// Async write pool for exploded tables
	writeQueue chan writeTask

	// Writer stats previous snapshots for per-second rates
	prevStats map[string]prevWriterStat
}

type writeTask struct {
	w     *RotatingParquetWriter
	rec   arrow.Record
	block uint64
}

type prevWriterStat struct {
	files int64
	bytes int64
	rows  int64
	t     time.Time
}
type ExplodedTable struct {
	fieldName string
	conv      *FieldConverter
	writer    *RotatingParquetWriter
}

type bufferedProcessItem struct {
	data   *pbsubstreamsrpc.BlockScopedData
	isLive *bool
	cursor *sink.Cursor
}

func SinkerFactory(base *sink.Sinker, opts SinkerFactoryOptions) func(ctx context.Context, logger *zap.Logger, tracer logging.Tracer) (*ParquetSinker, error) {
	return func(ctx context.Context, logger *zap.Logger, tracer logging.Tracer) (*ParquetSinker, error) {
		// Normalize local file URLs to absolute paths
		storeURL := opts.StoreURL
		if u, err := url.Parse(storeURL); err == nil {
			if u.Scheme == "file" || u.Scheme == "" {
				p := u.Path
				if p == "" && u.Opaque != "" {
					p = u.Opaque
				}
				if p == "" {
					p = "."
				}
				if !filepath.IsAbs(p) {
					abs, _ := filepath.Abs(p)
					p = abs
				}
				u.Scheme = "file"
				u.Host = ""
				u.Path = p
				storeURL = u.String()
			}
		}

		store, err := dstorepkg.NewStore(storeURL, opts.OutputPrefix, "", false)
		if err != nil {
			return nil, fmt.Errorf("create store: %w", err)
		}

		// Build descriptor-driven Arrow schema converter
		conv, err := NewProtoConverter(base.Package().ProtoFiles, protoreflect.FullName(base.OutputModuleTypeUnprefixed()))
		if err != nil {
			return nil, fmt.Errorf("init proto converter: %w", err)
		}

		p := &ParquetSinker{
			Shutter:               shutter.New(),
			Sinker:                base,
			logger:                logger,
			tracer:                tracer,
			store:                 NewDStoreAdapter(store),
			conv:                  conv,
			cursorFile:            opts.CursorFile,
			processingBufferSize:  opts.ProcessingBufferSize,
			processingBufferBytes: opts.ProcessingBufferBytes,
			processingMaxItems:    opts.ProcessingBufferMaxItems,
			undoBufferSize:        opts.UndoBufferSize,
		}

		p.partitioner = NewPartitioner(opts.StartBlock, opts.EndBlock, opts.PartitionSize, opts.PadWidth)
		p.writer = NewRotatingParquetWriter(p.store, opts, p.conv.Schema(), p.partitioner, "")

		// Initialize exploded tables for root-level repeated fields
		if opts.Explode {
			if pc, ok := p.conv.(*ProtoConverter); ok {
				p.exploded = make(map[string]*ExplodedTable)
				md := pc.MessageDescriptor()
				for i := 0; i < md.Fields().Len(); i++ {
					fd := md.Fields().Get(i)
					if fd.IsList() && !fd.IsMap() {
						fc := NewFieldConverter(fd)
						w := NewRotatingParquetWriter(p.store, opts, fc.Schema(), p.partitioner, string(fd.Name()))
						p.exploded[string(fd.Name())] = &ExplodedTable{fieldName: string(fd.Name()), conv: fc, writer: w}
					}
				}
			}
		}

		// Start worker pool if requested
		if opts.ExplodedWriteWorkers > 0 {
			p.writeQueue = make(chan writeTask, opts.ExplodedWriteWorkers*2)
			for i := 0; i < opts.ExplodedWriteWorkers; i++ {
				go func() {
					for t := range p.writeQueue {
						_, _ = t.w.AppendRecord(context.Background(), t.rec, t.block)
						t.rec.Release()
					}
				}()
			}
		}

		p.prevStats = make(map[string]prevWriterStat)

		if p.processingBufferSize > 0 || p.processingBufferBytes > 0 {
			capItems := p.processingBufferSize
			if capItems <= 0 {
				capItems = 1024
			}
			p.processingQueue = make(chan bufferedProcessItem, capItems)
			go p.consumeProcessingQueue(ctx)
		}

		if opts.UndoBufferSize > 0 {
			p.undoBuffer = NewUndoBuffer(opts.UndoBufferSize, p, logger)
			logger.Info("undo buffer enabled", zap.Int("size", opts.UndoBufferSize))
		} else {
			logger.Info("undo buffer disabled")
		}

		logger.Info("Parquet sinker created",
			zap.String("store_url", opts.StoreURL),
			zap.Uint64("partition_size", opts.PartitionSize),
			zap.Int("row_group_rows", opts.RowGroupRows),
		)
		_ = time.Second // keep import
		return p, nil
	}
}

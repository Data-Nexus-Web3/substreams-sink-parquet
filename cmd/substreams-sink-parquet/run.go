package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	parquetSinker "github.com/streamingfast/substreams-sink-parquet/parquet/sinker"
	"github.com/streamingfast/substreams/manifest"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

var sinkRunCmd = Command(sinkRunE,
	"run <store_url> <manifest>",
	"Runs Parquet sink process",
	ExactArgs(2),
	Flags(func(flags *pflag.FlagSet) {
		sink.AddFlagsToSet(flags)
		addParquetFlags(flags)
	}),
	Example("substreams-sink-parquet run file:///tmp/out uniswap-v3@v0.2.10 --output-module=map_pools"),
	OnCommandErrorLogAndExit(zlog),
)

func addParquetFlags(flags *pflag.FlagSet) {
	// Storage & naming
	flags.String("output-prefix", "", "Object key prefix inside store URL")
	flags.String("tmp-dir", "", "Local temp directory for staging parquet files (default: system temp)")
	flags.Int("pad-width", 10, "Zero pad width for file ranges")
	flags.String("cursor-file", "cursor.txt", "File to store cursor for checkpoint recovery")

	// Output module selection (explicit, per Kafka sink pattern)
	flags.String("output-module", "", "Name of the substreams module to consume (if not specified, will try to infer from manifest)")

	// Partitioning
	flags.Uint64("partition-size", 5000, "Blocks per parquet file")

	// Parquet tuning
	flags.String("compression", "zstd", "Parquet compression codec: zstd|snappy|gzip|lz4|uncompressed")
	flags.Int("compression-level", 0, "Compression level where applicable (e.g., zstd)")
	flags.Int("row-group-rows", 20000, "Target rows per row group")
	flags.Int("page-size", 0, "Parquet page size in bytes (0=default)")
	flags.Bool("dict-encoding", true, "Enable dictionary encoding")
	flags.Int64("target-file-bytes", 256<<20, "Soft target size per file in bytes (rotate)")
	flags.Bool("parquet-stats", true, "Enable Parquet column statistics (min/max/null-count)")
	flags.Duration("flush-interval", time.Second, "Flush buffered rows at this interval (0=disable time-based flush)")
	flags.Int("exploded-write-workers", 0, "Concurrent workers for exploded table writes (0=sync)")
	flags.Int("upload-workers", 2, "Concurrent upload workers per writer (0=sync uploads)")
	flags.Int("explode-field-workers", 0, "Concurrency for per-field explode build (0=auto)")

	// Explosion control
	flags.Bool("explode", false, "Explode root-level repeated fields into separate tables (one level only)")

	// Processing buffer (post-undo)
	flags.Int("processing-buffer-size", 0, "Number of blocks to buffer after undo (0=disabled)")
	flags.Int64("processing-buffer-bytes", 0, "Total byte budget for processing buffer (0=disabled)")
	flags.Int("processing-buffer-max-items", 4096, "Max items allowed in processing buffer")

	// Debug
	flags.Bool("debug", false, "Enable debug logging")
}

func sinkRunE(cmd *cobra.Command, args []string) error {
	app := NewApplication(cmd.Context())

	storeURL := args[0]
	manifestPath := args[1]

	endpoint := sflags.MustGetString(cmd, "endpoint")
	if endpoint == "" {
		network := sflags.MustGetString(cmd, "network")
		if network == "" {
			reader, err := manifest.NewReader(manifestPath)
			if err != nil {
				return fmt.Errorf("setup manifest reader: %w", err)
			}
			pkgBundle, err := reader.Read()
			if err != nil {
				return fmt.Errorf("read manifest: %w", err)
			}
			network = pkgBundle.Package.Network
		}
		var err error
		endpoint, err = manifest.ExtractNetworkEndpoint(network, sflags.MustGetString(cmd, "endpoint"), zlog)
		if err != nil {
			return err
		}
	}

	supportedOutputTypes := sink.IgnoreOutputModuleType
	outputModuleName := sflags.MustGetString(cmd, "output-module")
	if outputModuleName == "" {
		return fmt.Errorf("--output-module is required")
	}

	baseSinker, err := sink.NewFromViper(
		cmd,
		supportedOutputTypes,
		manifestPath,
		outputModuleName,
		"substreams-sink-parquet",
		zlog,
		tracer,
	)
	if err != nil {
		return fmt.Errorf("new base sinker: %w", err)
	}

	// Gather options
    options := parquetSinker.SinkerFactoryOptions{
		StoreURL:     storeURL,
		OutputPrefix: sflags.MustGetString(cmd, "output-prefix"),
		TmpDir:       sflags.MustGetString(cmd, "tmp-dir"),
		PadWidth:     sflags.MustGetInt(cmd, "pad-width"),

        // Anchor partitions to CLI-provided bounds
        StartBlock:    sflags.MustGetUint64(cmd, "start-block"),
        EndBlock:      sflags.MustGetUint64(cmd, "stop-block"),
		PartitionSize: sflags.MustGetUint64(cmd, "partition-size"),

		Compression:      sflags.MustGetString(cmd, "compression"),
		CompressionLevel: sflags.MustGetInt(cmd, "compression-level"),
		RowGroupRows:     sflags.MustGetInt(cmd, "row-group-rows"),
		PageSize:         sflags.MustGetInt(cmd, "page-size"),
		DictEncoding:     sflags.MustGetBool(cmd, "dict-encoding"),
		TargetFileBytes:  sflags.MustGetInt64(cmd, "target-file-bytes"),
		ParquetStats:     sflags.MustGetBool(cmd, "parquet-stats"),

		Explode: sflags.MustGetBool(cmd, "explode"),

		ProcessingBufferSize:     sflags.MustGetInt(cmd, "processing-buffer-size"),
		ProcessingBufferBytes:    sflags.MustGetInt64(cmd, "processing-buffer-bytes"),
		ProcessingBufferMaxItems: sflags.MustGetInt(cmd, "processing-buffer-max-items"),

		DebugMode:      sflags.MustGetBool(cmd, "debug"),
		UndoBufferSize: sflags.MustGetInt(cmd, "undo-buffer-size"),
		CursorFile:     sflags.MustGetString(cmd, "cursor-file"),

		FlushInterval:        sflags.MustGetDuration(cmd, "flush-interval"),
		ExplodedWriteWorkers: sflags.MustGetInt(cmd, "exploded-write-workers"),
		UploadWorkers:        sflags.MustGetInt(cmd, "upload-workers"),
		ExplodeFieldWorkers:  sflags.MustGetInt(cmd, "explode-field-workers"),
	}

	zlog.Info("starting Parquet sink",
		zap.String("store_url", storeURL),
		zap.String("manifest", manifestPath),
		zap.String("endpoint", endpoint),
		zap.Uint64("partition_size", options.PartitionSize),
		zap.String("compression", options.Compression),
		zap.Int("row_group_rows", options.RowGroupRows),
	)

	factory := parquetSinker.SinkerFactory(baseSinker, options)
	pqSink, err := factory(app.Context(), zlog, tracer)
	if err != nil {
		return fmt.Errorf("unable to setup Parquet sinker: %w", err)
	}

	app.SuperviseAndStart(pqSink)
	return app.WaitForTermination(zlog, 0*time.Second, 30*time.Second)
}

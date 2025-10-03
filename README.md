## substreams-sink-parquet

High-throughput Substreams sink that writes Arrow/Parquet to local or blob storage (S3, GCS, Azure) with partitioning and optional one-level "explode" of root repeated fields.

### Features
- Arrow-Go v18 Parquet writer
- One-time Arrow schema generation via Protobuf descriptors
- Partitioning by block ranges with zero‑padded filenames
- Optional explode of root-level repeated fields into separate tables/subdirectories
- Local and blob storage through `dstore` (file://, s3://, gs://, az://)
- Tunable Parquet properties (codec, level, row group size, page size, dict, stats)
- Streaming-friendly buffering and undo buffer integration
- Graceful file rotation/finalization; async uploads to blob storage
- Periodic internal writer stats logging (rows/bytes/rates/uploads)

### Install
```bash
go build -o bin/substreams-sink-parquet ./cmd/substreams-sink-parquet
```

### Quick start (local)
```bash
./bin/substreams-sink-parquet run \
  ./tmp/parquet-out \
  /path/to/your/substreams.spkg \
  --endpoint=mainnet.eth.streamingfast.io:443 \
  --output-module=substreams_module \
  --start-block=19000000 \
  --stop-block=+15000 \
  --partition-size=5000 \
  --explode=true \
  --compression=snappy \
  --row-group-rows=100000 \
  --dict-encoding=true \
  --page-size=0 \
  --parquet-stats=false \
  --flush-interval=1s \
  --exploded-write-workers=4 \
  --target-file-bytes=536870912 \
  --tmp-dir=./tmp/parquet-staging \
  --upload-workers=4
```

### Write to S3
```bash
./bin/substreams-sink-parquet run \
  "s3://your-bucket/prefix?region=us-east-2" \
  /path/to/your/substreams.spkg \
  --endpoint=mainnet.eth.streamingfast.io:443 \
  --output-module=substreams_module \
  --start-block=19000000 \
  --stop-block=+15000 \
  --partition-size=5000 \
  --explode=true \
  --compression=snappy \
  --row-group-rows=100000 \
  --parquet-stats=false \
  --flush-interval=1s \
  --exploded-write-workers=4 \
  --upload-workers=4 \
  --tmp-dir=/mnt/nvme/tmp
```

Notes:
- For local paths, `file://./tmp/parquet-out` and `./tmp/parquet-out` are normalized to an absolute `file://...` URL.
- `--tmp-dir` should point to fast local storage (NVMe) to maximize throughput during staging before upload.

### Explode semantics
- `--explode=true` creates a subdirectory per root-level repeated field (no recursion) and writes one row per element into that table.
- `--explode=false` writes one root row per block; list fields remain Arrow lists (strings or structs) within the main table.

### Partitioning
- Files rotate on block ranges of `--partition-size` blocks.
- Filenames are zero‑padded: `0019000000-0019005000.parquet` for start=19,000,000 and size=5,000.

### Flags
Core sink flags (from base Substreams sink):
- `--endpoint`, `--network`, `--start-block`, `--stop-block`, plus standard retry/liveness flags.
- `--output-module` (required): name of the module producing the output type.

Parquet sink flags:
- Storage & naming
  - `--output-prefix` string: key prefix inside the store URL
  - `--tmp-dir` string: local temp directory for staging Parquet files
  - `--pad-width` int: zero pad width for file ranges (default 10)
  - `--cursor-file` string: cursor persistence file (default `cursor.txt`)
- Partitioning
  - `--partition-size` uint64: blocks per Parquet file (default 5000)
- Parquet tuning
  - `--compression` string: zstd|snappy|lz4|gzip|uncompressed (default zstd)
  - `--compression-level` int: level for applicable codecs (e.g., zstd)
  - `--row-group-rows` int: target rows per row group (default 20000)
  - `--page-size` int: Parquet page size in bytes (0 = default)
  - `--dict-encoding` bool: enable dictionary encoding (default true)
  - `--target-file-bytes` int64: soft target file size to inform rotation
  - `--parquet-stats` bool: enable Parquet column stats (default true)
- Output shaping
  - `--explode` bool: explode root-level repeated fields into separate tables (default false)
- Buffering / concurrency
  - `--flush-interval` duration: time-based flush for buffered rows (default 1s)
  - `--exploded-write-workers` int: concurrent workers for exploded table writes (0 = sync)
  - `--upload-workers` int: concurrent upload workers per writer (0 = sync uploads; default 2)
- Processing buffer (post-undo)
  - `--processing-buffer-size` int
  - `--processing-buffer-bytes` int64
  - `--processing-buffer-max-items` int
- Ops / misc
  - `--undo-buffer-size` int
  - `--debug` bool
  - `--metrics-listen-addr` addr (global)
  - `--pprof-listen-addr` addr (global)

### Observability
- Parquet writer stats log every 30s:
  - files, bytes, rows, rows_per_sec, bytes_per_sec, active partition range
  - for `--explode=true`, one line per exploded table; otherwise a single line for the main writer
- Enable pprof/metrics:
```bash
--pprof-listen-addr=0.0.0.0:6060 --metrics-listen-addr=0.0.0.0:9102
```

### Performance tips
- Use NVMe for `--tmp-dir`
- Increase `--row-group-rows` (e.g., 100k–200k)
- For speed: `--compression=snappy` or `--compression=zstd --compression-level=1`
- Overlap writes and uploads with `--exploded-write-workers` and `--upload-workers`

### Storage URLs
- `file://` (or local path): normalized to absolute file URL
- `s3://bucket/prefix?region=...`
- `gs://bucket/prefix`
- `az://container/prefix`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Apache License 2.0

## Support

- [StreamingFast Documentation](https://substreams.streamingfast.io/)
- [StreamingFast Discord](https://discord.gg/streamingfast)


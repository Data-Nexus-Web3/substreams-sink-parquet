package sinker

import (
	"context"
	"fmt"
	"time"

	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

func (p *ParquetSinker) Run(ctx context.Context) error {
	cursor, err := p.loadCursor()
	if err != nil {
		cursor = sink.NewBlankCursor()
	}
	// periodic parquet writer stats
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			if len(p.exploded) == 0 && p.writer != nil {
				// only main writer stats when not exploding
				s := p.writer.Stats()
				key := "__main__"
				now := time.Now()
				prev := p.prevStats[key]
				dt := now.Sub(prev.t).Seconds()
				var rowsPs, bytesPs float64
				if prev.t.IsZero() || dt <= 0 {
					rowsPs = 0
					bytesPs = 0
				} else {
					rowsPs = float64(s.Rows-prev.rows) / dt
					bytesPs = float64(s.Bytes-prev.bytes) / dt
				}
				p.prevStats[key] = prevWriterStat{files: s.Files, bytes: s.Bytes, rows: s.Rows, t: now}
                p.logger.Info("parquet writer stats",
					zap.Int64("files", s.Files),
					zap.Int64("bytes", s.Bytes),
					zap.Int64("rows", s.Rows),
					zap.Int64("inflight_uploads", s.Inflight),
					zap.Int64("last_upload_ms", s.LastUploadMs),
					zap.Int64("last_upload_bytes", s.LastUploadB),
                    zap.Int64("last_upload_rows", s.LastUploadR),
					zap.Float64("rows_per_sec", rowsPs),
					zap.Float64("bytes_per_sec", bytesPs),
					zap.String("active_name", s.ActiveName),
					zap.Uint64("active_start", s.ActiveStart),
					zap.Uint64("active_end", s.ActiveEnd),
				)
			}
			// exploded table writers (only when exploding)
			if len(p.exploded) > 0 {
				for name, table := range p.exploded {
					if table != nil && table.writer != nil {
						s := table.writer.Stats()
						now := time.Now()
						prev := p.prevStats[name]
						dt := now.Sub(prev.t).Seconds()
						var rowsPs, bytesPs float64
						if prev.t.IsZero() || dt <= 0 {
							rowsPs = 0
							bytesPs = 0
						} else {
							rowsPs = float64(s.Rows-prev.rows) / dt
							bytesPs = float64(s.Bytes-prev.bytes) / dt
						}
						p.prevStats[name] = prevWriterStat{files: s.Files, bytes: s.Bytes, rows: s.Rows, t: now}
                        p.logger.Info("parquet writer stats",
							zap.String("table", name),
							zap.Int64("files", s.Files),
							zap.Int64("bytes", s.Bytes),
							zap.Int64("rows", s.Rows),
							zap.Int64("inflight_uploads", s.Inflight),
							zap.Int64("last_upload_ms", s.LastUploadMs),
							zap.Int64("last_upload_bytes", s.LastUploadB),
                            zap.Int64("last_upload_rows", s.LastUploadR),
							zap.Float64("rows_per_sec", rowsPs),
							zap.Float64("bytes_per_sec", bytesPs),
							zap.String("active_name", s.ActiveName),
							zap.Uint64("active_start", s.ActiveStart),
							zap.Uint64("active_end", s.ActiveEnd),
						)
					}
				}
			}
		}
	}()
	p.Sinker.Run(ctx, cursor, p)
	// Ensure all writers are finalized and uploaded on stream end
	_ = p.Close()
	return p.Sinker.Err()
}

func (p *ParquetSinker) Close() error {
	if p.IsTerminated() {
		return nil
	}
	if p.processingQueue != nil {
		close(p.processingQueue)
		// wait for processing workers to finish
		p.procWg.Wait()
	}
	if p.writeQueue != nil {
		close(p.writeQueue)
		// wait for write workers to drain
		p.writeWg.Wait()
	}
	// Close main writer
	if p.writer != nil {
		if err := p.writer.Close(); err != nil {
			return fmt.Errorf("close main writer: %w", err)
		}
	}
	// Close all exploded table writers
	for name, table := range p.exploded {
		if table.writer != nil {
			if err := table.writer.Close(); err != nil {
				return fmt.Errorf("close exploded writer %s: %w", name, err)
			}
		}
	}
	p.Shutdown(nil)
	return nil
}

func (p *ParquetSinker) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	if p.undoBufferSize > 0 && p.undoBuffer != nil {
		return p.undoBuffer.AddBlock(data, isLive, cursor)
	}
	if p.processingQueue != nil {
		p.processingQueue <- bufferedProcessItem{data: data, isLive: isLive, cursor: cursor}
		return nil
	}
	return p.processBlock(ctx, data, cursor)
}

func (p *ParquetSinker) HandleBlockUndoSignal(ctx context.Context, undo *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	if p.undoBuffer != nil {
		p.undoBuffer.HandleUndo(undo)
		return p.saveCursor(cursor)
	}
	return fmt.Errorf("undo not supported without buffer; enable --undo-buffer-size")
}

func (p *ParquetSinker) consumeProcessingQueue(ctx context.Context) {
	// Single-threaded fallback; retained for safety
	for item := range p.processingQueue {
		_ = p.processBlock(ctx, item.data, item.cursor)
	}
}

func (p *ParquetSinker) processBlock(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, cursor *sink.Cursor) error {
	if data.Output == nil || data.Output.MapOutput == nil || data.Output.MapOutput.Value == nil {
		return nil
	}
	// Write root-level row if not exploding, with buffering by rows/time
	if p.exploded == nil {
		if err := p.conv.Append(data.Clock.Number, data.Clock.Id, data.Output.MapOutput.Value); err != nil {
			return err
		}
		p.mainBufferedRows++
		shouldFlush := p.mainBufferedRows >= p.writer.opts.RowGroupRows
		if !shouldFlush && p.writer.opts.FlushInterval > 0 {
			if p.mainLastFlush.IsZero() {
				p.mainLastFlush = time.Now()
			}
			if time.Since(p.mainLastFlush) >= p.writer.opts.FlushInterval {
				shouldFlush = true
			}
		}
		if shouldFlush {
			rec, err := p.conv.MakeRecord()
			if err != nil {
				return err
			}
			if rec != nil && rec.NumRows() > 0 {
				defer rec.Release()
				if _, err := p.writer.AppendRecord(ctx, rec, data.Clock.Number); err != nil {
					return err
				}
				p.conv.Reset()
			}
			p.mainBufferedRows = 0
			p.mainLastFlush = time.Now()
		}
	}

	// Explode root-level repeated fields into dedicated tables (no recursion)
	if len(p.exploded) > 0 {
		m := dynamicUnmarshal(p.conv, data.Output.MapOutput.Value)
		if m != nil {
			md := m.Descriptor()
			for i := 0; i < md.Fields().Len(); i++ {
				fd := md.Fields().Get(i)
				t, ok := p.exploded[string(fd.Name())]
				if !ok || !fd.IsList() || fd.IsMap() {
					continue
				}
				// Build using a worker-local converter to avoid concurrent builder access
				localConv := NewFieldConverter(fd)
				list := m.Get(fd).List()
				for j := 0; j < list.Len(); j++ {
					localConv.AppendElement(list.Get(j), fd)
				}
				r, _ := localConv.MakeRecord()
				if r != nil && r.NumRows() > 0 {
					if p.writeQueue != nil {
						r.Retain()
						p.writeQueue <- writeTask{w: t.writer, rec: r, block: data.Clock.Number}
					} else {
						defer r.Release()
						if _, err := t.writer.AppendRecord(ctx, r, data.Clock.Number); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return p.saveCursor(cursor)
}

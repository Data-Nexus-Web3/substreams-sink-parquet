package sinker

import (
	"context"

	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
)

type UndoBuffer struct {
	size int
	p    *ParquetSinker
}

func NewUndoBuffer(size int, p *ParquetSinker, _ interface{}) *UndoBuffer {
	return &UndoBuffer{size: size, p: p}
}

func (u *UndoBuffer) AddBlock(data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	// Minimal passthrough for scaffold. Later: ring buffer to protect against reorgs.
	return u.p.processBlock(context.Background(), data, cursor)
}

func (u *UndoBuffer) HandleUndo(_ *pbsubstreamsrpc.BlockUndoSignal) {}

func (u *UndoBuffer) GetCurrentBufferSize() int { return 0 }

func (u *UndoBuffer) Close() error { return nil }

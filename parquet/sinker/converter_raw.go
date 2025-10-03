package sinker

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RawConverter implements Converter using a simple schema:
// block_number: uint64, block_id: utf8, payload: binary
type RawConverter struct {
	alloc  memory.Allocator
	schema *arrow.Schema

	blockNumB *array.Uint64Builder
	blockIDB  *array.StringBuilder
	payloadB  *array.BinaryBuilder
}

func NewRawConverter() *RawConverter {
	alloc := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "block_number", Type: arrow.PrimitiveTypes.Uint64, Nullable: false},
		{Name: "block_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: false},
	}, nil)

	return &RawConverter{
		alloc:     alloc,
		schema:    schema,
		blockNumB: array.NewUint64Builder(alloc),
		blockIDB:  array.NewStringBuilder(alloc),
		payloadB:  array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary),
	}
}

func (c *RawConverter) Append(blockNumber uint64, blockID string, rawPayload []byte) error {
	c.blockNumB.Append(blockNumber)
	c.blockIDB.Append(blockID)
	c.payloadB.Append(rawPayload)
	return nil
}

func (c *RawConverter) MakeRecord() (arrow.Record, error) {
	// Build arrays
	bn := c.blockNumB.NewUint64Array()
	bid := c.blockIDB.NewStringArray()
	pl := c.payloadB.NewBinaryArray()

	rec := array.NewRecord(c.schema, []arrow.Array{bn, bid, pl}, int64(bn.Len()))

	// Release arrays (record holds references)
	bn.Release()
	bid.Release()
	pl.Release()

	// Reset builders for next batch
	c.blockNumB.Release()
	c.blockIDB.Release()
	c.payloadB.Release()

	c.blockNumB = array.NewUint64Builder(c.alloc)
	c.blockIDB = array.NewStringBuilder(c.alloc)
	c.payloadB = array.NewBinaryBuilder(c.alloc, arrow.BinaryTypes.Binary)

	return rec, nil
}

func (c *RawConverter) Schema() *arrow.Schema { return c.schema }

func (c *RawConverter) Reset() {
	// Clear current builders
	c.blockNumB.Release()
	c.blockIDB.Release()
	c.payloadB.Release()

	c.blockNumB = array.NewUint64Builder(c.alloc)
	c.blockIDB = array.NewStringBuilder(c.alloc)
	c.payloadB = array.NewBinaryBuilder(c.alloc, arrow.BinaryTypes.Binary)
}

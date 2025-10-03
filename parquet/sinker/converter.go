package sinker

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// Converter converts protobuf-encoded bytes into Arrow records
type Converter interface {
	// Append appends one row to the internal builders
    Append(blockNumber uint64, blockID string, rawPayload []byte) error
	// MakeRecord produces an Arrow record for the currently accumulated rows and resets the internal builders
	MakeRecord() (arrow.Record, error)
	// Schema returns the Arrow schema for produced records
	Schema() *arrow.Schema
	// Reset clears any accumulated rows/builders
	Reset()
}

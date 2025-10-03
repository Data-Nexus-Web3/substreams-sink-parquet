package sinker

import (
	"context"
	"io"

	dstorepkg "github.com/streamingfast/dstore"
)

// DStoreAdapter adapts streamingfast dstore to the local Store interface
type DStoreAdapter struct{ inner dstorepkg.Store }

func NewDStoreAdapter(s dstorepkg.Store) DStoreAdapter { return DStoreAdapter{inner: s} }

func (a DStoreAdapter) WriteObject(ctx context.Context, object string, content io.Reader) error {
	return a.inner.WriteObject(ctx, object, content)
}

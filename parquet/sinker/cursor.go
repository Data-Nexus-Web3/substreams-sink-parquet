package sinker

import (
	"fmt"
	"os"

	sink "github.com/streamingfast/substreams/sink"
)

func (p *ParquetSinker) loadCursor() (*sink.Cursor, error) {
	if p.cursorFile == "" {
		return sink.NewBlankCursor(), nil
	}
	if _, err := os.Stat(p.cursorFile); os.IsNotExist(err) {
		return sink.NewBlankCursor(), nil
	}
	data, err := os.ReadFile(p.cursorFile)
	if err != nil {
		return nil, fmt.Errorf("read cursor: %w", err)
	}
	if len(data) == 0 {
		return sink.NewBlankCursor(), nil
	}
	return sink.NewCursor(string(data))
}

func (p *ParquetSinker) saveCursor(cursor *sink.Cursor) error {
	if p.cursorFile == "" {
		return nil
	}
	return os.WriteFile(p.cursorFile, []byte(cursor.String()), 0644)
}

package sinker

import (
	"fmt"
)

type Partitioner struct {
	start uint64
	end   uint64 // 0 = unbounded
	size  uint64
	pad   int

	currentStart uint64
}

func NewPartitioner(start, end, size uint64, pad int) *Partitioner {
	return &Partitioner{start: start, end: end, size: size, pad: pad, currentStart: start}
}

func (p *Partitioner) RangeFor(block uint64) (rangeStart, rangeEnd uint64) {
	b := block
	if b < p.start {
		b = p.start
	}
	idx := (b - p.start) / p.size
	rangeStart = p.start + idx*p.size
	rangeEnd = rangeStart + p.size
	if p.end > 0 && rangeEnd > p.end {
		rangeEnd = p.end
	}
	return
}

func (p *Partitioner) FileName(rangeStart, rangeEnd uint64) string {
	return fmt.Sprintf("%0*d-%0*d.parquet", p.pad, rangeStart, p.pad, rangeEnd)
}

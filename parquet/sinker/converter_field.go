package sinker

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// FieldConverter builds records for one root-level repeated field
type FieldConverter struct {
	alloc  memory.Allocator
	schema *arrow.Schema
	rb     *array.RecordBuilder

	// if scalar list, use value-only column
	isScalar bool
}

func NewFieldConverter(fd protoreflect.FieldDescriptor) *FieldConverter {
	alloc := memory.DefaultAllocator
	var schema *arrow.Schema
	isScalar := fd.Kind() != protoreflect.MessageKind
	if isScalar {
		// table has a single column holding scalar values
		schema = arrow.NewSchema([]arrow.Field{{Name: string(fd.Name()), Type: scalarArrowType(fd.Kind()), Nullable: true}}, nil)
	} else {
		// message element -> flatten nested fields as top-level columns (no wrapper struct)
		nested := fd.Message()
		fields := make([]arrow.Field, 0, nested.Fields().Len())
		for i := 0; i < nested.Fields().Len(); i++ {
			fields = append(fields, buildArrowField(nested.Fields().Get(i)))
		}
		schema = arrow.NewSchema(fields, nil)
	}
	rb := array.NewRecordBuilder(alloc, schema)
	return &FieldConverter{alloc: alloc, schema: schema, rb: rb, isScalar: isScalar}
}

func (c *FieldConverter) Schema() *arrow.Schema { return c.schema }

func (c *FieldConverter) Reset() { c.rb.Release(); c.rb = array.NewRecordBuilder(c.alloc, c.schema) }

func (c *FieldConverter) AppendElement(val protoreflect.Value, fd protoreflect.FieldDescriptor) {
	if c.isScalar {
		appendValueToBuilder(val, fd, c.rb.Field(0))
		return
	}
	// message element: append one row across all top-level columns
	nested := val.Message()
	md := nested.Descriptor()
	for i := 0; i < md.Fields().Len(); i++ {
		nfd := md.Fields().Get(i)
		fb := c.rb.Field(i)
		if !nested.Has(nfd) {
			appendNull(fb, nfd)
			continue
		}
		v := nested.Get(nfd)
		appendValueToBuilder(v, nfd, fb)
	}
}

func (c *FieldConverter) MakeRecord() (arrow.Record, error) { return c.rb.NewRecord(), nil }

func scalarArrowType(k protoreflect.Kind) arrow.DataType {
	switch k {
	case protoreflect.BoolKind:
		return arrow.FixedWidthTypes.Boolean
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return arrow.PrimitiveTypes.Int32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return arrow.PrimitiveTypes.Int64
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return arrow.PrimitiveTypes.Uint32
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return arrow.PrimitiveTypes.Uint64
	case protoreflect.FloatKind:
		return arrow.PrimitiveTypes.Float32
	case protoreflect.DoubleKind:
		return arrow.PrimitiveTypes.Float64
	case protoreflect.StringKind:
		return arrow.BinaryTypes.String
	case protoreflect.BytesKind:
		return arrow.BinaryTypes.Binary
	case protoreflect.EnumKind:
		return arrow.PrimitiveTypes.Int32
	default:
		return arrow.BinaryTypes.Binary
	}
}

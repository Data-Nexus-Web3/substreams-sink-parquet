package sinker

import (
    "fmt"

    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/reflect/protodesc"
    "google.golang.org/protobuf/reflect/protoreflect"
    "google.golang.org/protobuf/reflect/protoregistry"
    "google.golang.org/protobuf/types/descriptorpb"
    "google.golang.org/protobuf/types/dynamicpb"
)

type ProtoConverter struct {
    alloc   memory.Allocator
    schema  *arrow.Schema
    rb      *array.RecordBuilder
    msgDesc protoreflect.MessageDescriptor
}

func NewProtoConverter(protoFiles []*descriptorpb.FileDescriptorProto, fullName protoreflect.FullName) (*ProtoConverter, error) {
    files, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: protoFiles})
    if err != nil { return nil, fmt.Errorf("protodesc: %w", err) }
    d, err := files.FindDescriptorByName(fullName)
    if err != nil {
        if err == protoregistry.NotFound { return nil, fmt.Errorf("type %q not found", fullName) }
        return nil, fmt.Errorf("find descriptor: %w", err)
    }
    msgDesc, ok := d.(protoreflect.MessageDescriptor)
    if !ok { return nil, fmt.Errorf("descriptor %q is not a message", fullName) }

    schema := buildArrowSchemaFromMessage(msgDesc)
    alloc := memory.DefaultAllocator
    rb := array.NewRecordBuilder(alloc, schema)
    return &ProtoConverter{alloc: alloc, schema: schema, rb: rb, msgDesc: msgDesc}, nil
}

func (c *ProtoConverter) Schema() *arrow.Schema { return c.schema }

func (c *ProtoConverter) Reset() { c.rb.Release(); c.rb = array.NewRecordBuilder(c.alloc, c.schema) }

func (c *ProtoConverter) MessageDescriptor() protoreflect.MessageDescriptor { return c.msgDesc }

func (c *ProtoConverter) Append(_ uint64, _ string, rawPayload []byte) error {
    msg := dynamicpb.NewMessage(c.msgDesc)
    // Unmarshal payload into dynamic message
    if err := proto.Unmarshal(rawPayload, msg); err != nil {
        return fmt.Errorf("unmarshal: %w", err)
    }
    appendMessageToBuilders(msg, c.rb, 0)
    return nil
}

func (c *ProtoConverter) MakeRecord() (arrow.Record, error) {
    rec := c.rb.NewRecord()
    return rec, nil
}

// Schema construction
func buildArrowSchemaFromMessage(md protoreflect.MessageDescriptor) *arrow.Schema {
    fields := make([]arrow.Field, 0, md.Fields().Len())
    for i := 0; i < md.Fields().Len(); i++ {
        fd := md.Fields().Get(i)
        fields = append(fields, buildArrowField(fd))
    }
    return arrow.NewSchema(fields, nil)
}

func buildArrowField(fd protoreflect.FieldDescriptor) arrow.Field {
    name := string(fd.Name())
    nullable := true
    // Map fields: represent as list<struct<key,value>>
    if fd.IsMap() {
        keyField := fd.Message().Fields().ByName("key")
        valField := fd.Message().Fields().ByName("value")
        kvStruct := arrow.StructOf(buildArrowField(keyField), buildArrowField(valField))
        return arrow.Field{Name: name, Type: arrow.ListOf(kvStruct), Nullable: nullable}
    }

    var dt arrow.DataType
    switch fd.Kind() {
    case protoreflect.BoolKind:
        dt = arrow.FixedWidthTypes.Boolean
    case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
        dt = arrow.PrimitiveTypes.Int32
    case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
        dt = arrow.PrimitiveTypes.Int64
    case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
        dt = arrow.PrimitiveTypes.Uint32
    case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
        dt = arrow.PrimitiveTypes.Uint64
    case protoreflect.FloatKind:
        dt = arrow.PrimitiveTypes.Float32
    case protoreflect.DoubleKind:
        dt = arrow.PrimitiveTypes.Float64
    case protoreflect.StringKind:
        dt = arrow.BinaryTypes.String
    case protoreflect.BytesKind:
        dt = arrow.BinaryTypes.Binary
    case protoreflect.EnumKind:
        dt = arrow.PrimitiveTypes.Int32
    case protoreflect.MessageKind:
        // Struct of nested fields
        nested := fd.Message()
        nestedFields := make([]arrow.Field, 0, nested.Fields().Len())
        for i := 0; i < nested.Fields().Len(); i++ {
            nestedFields = append(nestedFields, buildArrowField(nested.Fields().Get(i)))
        }
        dt = arrow.StructOf(nestedFields...)
    default:
        dt = arrow.BinaryTypes.Binary
    }

    if fd.IsList() && !fd.IsMap() {
        // List element type is dt; record builder expects a ListBuilder with element builder matching dt
        dt = arrow.ListOf(dt)
    }
    return arrow.Field{Name: name, Type: dt, Nullable: nullable}
}

// Appending values
func appendMessageToBuilders(msg protoreflect.Message, rb *array.RecordBuilder, depth int) {
    md := msg.Descriptor()
    for i := 0; i < md.Fields().Len(); i++ {
        fd := md.Fields().Get(i)
        fb := rb.Field(i)
        if !msg.Has(fd) {
            // append null
            appendNull(fb, fd)
            continue
        }
        val := msg.Get(fd)
        appendValueToBuilder(val, fd, fb)
    }
}

func appendNull(b array.Builder, fd protoreflect.FieldDescriptor) {
    switch vb := b.(type) {
    case *array.BooleanBuilder:
        vb.AppendNull()
    case *array.Int32Builder:
        vb.AppendNull()
    case *array.Int64Builder:
        vb.AppendNull()
    case *array.Uint32Builder:
        vb.AppendNull()
    case *array.Uint64Builder:
        vb.AppendNull()
    case *array.Float32Builder:
        vb.AppendNull()
    case *array.Float64Builder:
        vb.AppendNull()
    case *array.StringBuilder:
        vb.AppendNull()
    case *array.BinaryBuilder:
        vb.AppendNull()
    case *array.ListBuilder:
        vb.AppendNull()
    case *array.StructBuilder:
        vb.AppendNull()
    default:
        b.AppendNull()
    }
}

func appendValueToBuilder(val protoreflect.Value, fd protoreflect.FieldDescriptor, b array.Builder) {
    if fd.IsMap() {
        // list<struct<key,value>>
        lb, ok := b.(*array.ListBuilder)
        if !ok { b.AppendNull(); return }
        lb.Append(true)
        sb, ok := lb.ValueBuilder().(*array.StructBuilder)
        if !ok { b.AppendNull(); return }
        keyField := fd.Message().Fields().ByName("key")
        valField := fd.Message().Fields().ByName("value")
        m := val.Map()
        m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
            appendValueToBuilder(protoreflect.ValueOf(k.Interface()), keyField, sb.FieldBuilder(0))
            appendValueToBuilder(v, valField, sb.FieldBuilder(1))
            sb.Append(true)
            return true
        })
        return
    }

    if fd.IsList() {
        lb, ok := b.(*array.ListBuilder)
        if !ok { b.AppendNull(); return }
        lb.Append(true)
        vb := lb.ValueBuilder()
        list := val.List()
        for i := 0; i < list.Len(); i++ {
            elem := list.Get(i)
            if fd.Kind() == protoreflect.MessageKind {
                // element is a message -> value builder must be StructBuilder
                sb, ok := vb.(*array.StructBuilder)
                if !ok { b.AppendNull(); continue }
                sb.Append(true)
                nested := elem.Message()
                md := nested.Descriptor()
                for fi := 0; fi < md.Fields().Len(); fi++ {
                    nfd := md.Fields().Get(fi)
                    fb := sb.FieldBuilder(fi)
                    if !nested.Has(nfd) {
                        appendNull(fb, nfd)
                        continue
                    }
                    v := nested.Get(nfd)
                    appendValueToBuilder(v, nfd, fb)
                }
            } else {
                // scalar element -> append directly based on kind
                appendScalarElement(elem, fd.Kind(), vb)
            }
        }
        return
    }

    switch fd.Kind() {
    case protoreflect.BoolKind:
        b.(*array.BooleanBuilder).Append(val.Bool())
    case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
        b.(*array.Int32Builder).Append(int32(val.Int()))
    case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
        b.(*array.Int64Builder).Append(val.Int())
    case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
        b.(*array.Uint32Builder).Append(uint32(val.Uint()))
    case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
        b.(*array.Uint64Builder).Append(val.Uint())
    case protoreflect.FloatKind:
        b.(*array.Float32Builder).Append(float32(val.Float()))
    case protoreflect.DoubleKind:
        b.(*array.Float64Builder).Append(val.Float())
    case protoreflect.StringKind:
        b.(*array.StringBuilder).Append(val.String())
    case protoreflect.BytesKind:
        b.(*array.BinaryBuilder).Append(val.Bytes())
    case protoreflect.EnumKind:
        b.(*array.Int32Builder).Append(int32(val.Enum()))
    case protoreflect.MessageKind:
        sb, ok := b.(*array.StructBuilder)
        if !ok { b.AppendNull(); return }
        sb.Append(true)
        nested := val.Message()
        md := nested.Descriptor()
        for i := 0; i < md.Fields().Len(); i++ {
            nfd := md.Fields().Get(i)
            fb := sb.FieldBuilder(i)
            if !nested.Has(nfd) {
                appendNull(fb, nfd)
                continue
            }
            v := nested.Get(nfd)
            appendValueToBuilder(v, nfd, fb)
        }
    default:
        b.AppendNull()
    }
}

// appendScalarElement appends a single non-list, non-map element value of the provided kind to builder b
func appendScalarElement(val protoreflect.Value, kind protoreflect.Kind, b array.Builder) {
    switch kind {
    case protoreflect.BoolKind:
        b.(*array.BooleanBuilder).Append(val.Bool())
    case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
        b.(*array.Int32Builder).Append(int32(val.Int()))
    case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
        b.(*array.Int64Builder).Append(val.Int())
    case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
        b.(*array.Uint32Builder).Append(uint32(val.Uint()))
    case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
        b.(*array.Uint64Builder).Append(val.Uint())
    case protoreflect.FloatKind:
        b.(*array.Float32Builder).Append(float32(val.Float()))
    case protoreflect.DoubleKind:
        b.(*array.Float64Builder).Append(val.Float())
    case protoreflect.StringKind:
        b.(*array.StringBuilder).Append(val.String())
    case protoreflect.BytesKind:
        b.(*array.BinaryBuilder).Append(val.Bytes())
    case protoreflect.EnumKind:
        b.(*array.Int32Builder).Append(int32(val.Enum()))
    case protoreflect.MessageKind:
        // Should not be used for message kind here; handled by list branch or message branch above
        sb, ok := b.(*array.StructBuilder)
        if !ok { b.AppendNull(); return }
        sb.Append(true)
        nested := val.Message()
        md := nested.Descriptor()
        for i := 0; i < md.Fields().Len(); i++ {
            nfd := md.Fields().Get(i)
            fb := sb.FieldBuilder(i)
            if !nested.Has(nfd) { appendNull(fb, nfd); continue }
            v := nested.Get(nfd)
            appendValueToBuilder(v, nfd, fb)
        }
    default:
        b.AppendNull()
    }
}




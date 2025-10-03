package sinker

import (
    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/reflect/protoreflect"
    "google.golang.org/protobuf/types/dynamicpb"
)

// dynamicUnmarshal decodes bytes to a dynamicpb.Message using the converter's descriptor if available
func dynamicUnmarshal(conv Converter, raw []byte) protoreflect.Message {
    pc, ok := conv.(*ProtoConverter)
    if !ok { return nil }
    msg := dynamicpb.NewMessage(pc.MessageDescriptor())
    if err := proto.Unmarshal(raw, msg); err != nil { return nil }
    return msg.ProtoReflect()
}



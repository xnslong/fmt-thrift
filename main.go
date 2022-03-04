package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/apache/thrift/lib/go/thrift"
)

func main() {
	in := bufio.NewReader(os.Stdin)
	err := discardPossibleFrameSize(in)
	if err != nil {
		log.Fatal("read data error", err)
	}

	transport := thrift.NewStreamTransportR(in)
	defer transport.Close()

	proto := thrift.NewTBinaryProtocol(transport, false, false)

	m := make(map[string]interface{})
	ctx := context.Background()

	err = readMsg(ctx, m, proto)
	if err != nil {
		log.Fatal("read message error", err)
	}

	bytes, err := json.Marshal(m)
	if err != nil {
		log.Fatal("json marshal error", err)
	}

	fmt.Printf("%s", bytes)
}

func discardPossibleFrameSize(in *bufio.Reader) error {
	peek, err := in.Peek(4)
	if err != nil {
		return err
	}
	v := binary.BigEndian.Uint32(peek)
	if int(v)&thrift.VERSION_MASK != thrift.VERSION_1 {
		_, _ = in.Discard(4)
	}
	return nil
}

func MessageType(mt thrift.TMessageType) string {
	switch mt {
	case thrift.CALL:
		return "CALL"
	case thrift.REPLY:
		return "REPLY"
	case thrift.EXCEPTION:
		return "EXCEPTION"
	case thrift.ONEWAY:
		return "ONEWAY"
	default:
		return "INVALID"
	}
}

func readMsg(ctx context.Context, m map[string]interface{}, proto *thrift.TBinaryProtocol) error {
	name, typeId, seqId, err := proto.ReadMessageBegin()
	if err != nil {
		return err
	}

	m["1 NAME"] = name
	m["2 SEQ_ID"] = seqId
	m["3 TYPE"] = MessageType(typeId)

	structure, err := readStruct(ctx, proto)
	if err != nil {
		return err
	}
	if structure != nil {
		m["4 PAYLOAD"] = structure
	}

	err = proto.ReadMessageEnd()
	if err != nil {
		return err
	}

	return nil
}

func readValue(ctx context.Context, proto *thrift.TBinaryProtocol, id thrift.TType) (interface{}, error) {
	switch id {
	case thrift.BOOL:
		return proto.ReadBool()
	case thrift.BYTE:
		return proto.ReadByte()
	case thrift.DOUBLE:
		return proto.ReadDouble()
	case thrift.I16:
		return proto.ReadI16()
	case thrift.I32:
		return proto.ReadI32()
	case thrift.I64:
		return proto.ReadI64()
	case thrift.STRING, thrift.UTF8, thrift.UTF16:
		return proto.ReadString()
	case thrift.STRUCT:
		return readStruct(ctx, proto)
	case thrift.MAP:
		return readMap(ctx, proto)
	case thrift.SET:
		return readSet(ctx, proto)
	case thrift.LIST:
		return readList(ctx, proto)
	default:
		return nil, fmt.Errorf("unsupported type: %s", id)
	}
}

func readList(ctx context.Context, proto *thrift.TBinaryProtocol) (map[string]interface{}, error) {
	elemType, size, err := proto.ReadListBegin()
	if err != nil {
		return nil, err
	}

	m := make(map[string]interface{}, size+1)
	m["_elem_type"] = elemType.String()

	for i := 0; i < size; i++ {
		value, err := readValue(ctx, proto, elemType)
		if err != nil {
			return nil, err
		}

		m[fmt.Sprintf("%d", i)] = value
	}

	err = proto.ReadListEnd()
	if err != nil {
		return nil, err
	}

	return m, nil
}

func readSet(ctx context.Context, proto *thrift.TBinaryProtocol) (map[string]interface{}, error) {
	elemType, size, err := proto.ReadSetBegin()
	if err != nil {
		return nil, fmt.Errorf("invalid set begin: %w", err)
	}

	m := make(map[string]interface{}, size+1)
	m["_elem_type"] = elemType.String()

	for i := 0; i < size; i++ {
		value, err := readValue(ctx, proto, elemType)
		if err != nil {
			return nil, fmt.Errorf("read elem error: %w", err)
		}

		m[fmt.Sprintf("%d", i)] = value
	}

	err = proto.ReadSetEnd()
	if err != nil {
		return nil, fmt.Errorf("invalid set end: %w", err)
	}

	return m, nil
}

func readMap(ctx context.Context, proto *thrift.TBinaryProtocol) (map[string]interface{}, error) {
	keyType, elemType, size, err := proto.ReadMapBegin()
	if err != nil {
		return nil, fmt.Errorf("invalid map begin: %w", err)
	}

	m := make(map[string]interface{}, 3)
	m["1 key_type"] = keyType.String()
	m["2 elem_type"] = elemType.String()

	entries := make(map[string]interface{}, size)
	m["3 entries"] = entries

	for i := 0; i < size; i++ {
		key, err := readValue(ctx, proto, keyType)
		if err != nil {
			return nil, fmt.Errorf("read key error: %w", err)
		}

		value, err := readValue(ctx, proto, elemType)
		if err != nil {
			return nil, fmt.Errorf("read value error: %w", err)
		}

		entries[fmt.Sprintf("%v", key)] = value
	}

	err = proto.ReadSetEnd()
	if err != nil {
		return nil, fmt.Errorf("invalid map end: %w", err)
	}

	return m, nil
}

func readStruct(ctx context.Context, proto *thrift.TBinaryProtocol) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	_, err := proto.ReadStructBegin()
	if err != nil {
		return nil, err
	}

	for {
		_, id, seqId, err := proto.ReadFieldBegin()
		if err != nil {
			return nil, err
		}

		if id == thrift.STOP {
			break
		}

		val, err := readValue(ctx, proto, id)
		if err != nil {
			return nil, err
		}

		m[fmt.Sprintf("%d %s", seqId, id.String())] = val
	}

	err = proto.ReadStructEnd()
	if err != nil {
		return nil, err
	}

	return m, nil
}

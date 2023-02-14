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

func readValue(ctx context.Context, proto *thrift.TBinaryProtocol, id thrift.TType) (interface{}, string, error) {
	switch id {
	case thrift.BOOL:
		v, err := proto.ReadBool()
		return v, id.String(), err
	case thrift.BYTE:
		v, err := proto.ReadByte()
		return v, id.String(), err
	case thrift.DOUBLE:
		v, err := proto.ReadDouble()
		return v, id.String(), err
	case thrift.I16:
		v, err := proto.ReadI16()
		return v, id.String(), err
	case thrift.I32:
		v, err := proto.ReadI32()
		return v, id.String(), err
	case thrift.I64:
		v, err := proto.ReadI64()
		return v, id.String(), err
	case thrift.STRING, thrift.UTF8, thrift.UTF16:
		v, err := proto.ReadString()
		return v, id.String(), err
	case thrift.STRUCT:
		v, err := readStruct(ctx, proto)
		return v, id.String(), err
	case thrift.MAP:
		return readMap(ctx, proto)
	case thrift.SET:
		return readSet(ctx, proto)
	case thrift.LIST:
		return readList(ctx, proto)
	default:
		return nil, id.String(), fmt.Errorf("unsupported type: %s", id)
	}
}

func readList(ctx context.Context, proto *thrift.TBinaryProtocol) (map[string]interface{}, string, error) {
	elemType, size, err := proto.ReadListBegin()
	if err != nil {
		return nil, "", err
	}

	m := make(map[string]interface{}, size+1)

	for i := 0; i < size; i++ {
		value, anno, err := readValue(ctx, proto, elemType)
		if err != nil {
			return nil, "", err
		}

		m[fmt.Sprintf("%d %s", i, anno)] = value
	}

	err = proto.ReadListEnd()
	if err != nil {
		return nil, "", err
	}

	return m, "list<" + elemType.String() + ">", nil
}

func readSet(ctx context.Context, proto *thrift.TBinaryProtocol) (map[string]interface{}, string, error) {
	elemType, size, err := proto.ReadSetBegin()
	if err != nil {
		return nil, "", err
	}

	m := make(map[string]interface{}, size+1)

	for i := 0; i < size; i++ {
		value, anno, err := readValue(ctx, proto, elemType)
		if err != nil {
			return nil, "", err
		}

		m[fmt.Sprintf("%d %s", i, anno)] = value
	}

	err = proto.ReadSetEnd()
	if err != nil {
		return nil, "", err
	}

	return m, "set<" + elemType.String() + ">", nil
}

func readMap(ctx context.Context, proto *thrift.TBinaryProtocol) (map[interface{}]interface{}, string, error) {
	keyType, elemType, size, err := proto.ReadMapBegin()
	if err != nil {
		return nil, "", err
	}

	m := make(map[interface{}]interface{}, size+2)

	for i := 0; i < size; i++ {
		key, _, err := readValue(ctx, proto, keyType)
		if err != nil {
			return nil, "", err
		}

		value, valAnno, err := readValue(ctx, proto, elemType)
		if err != nil {
			return nil, "", err
		}

		m[fmt.Sprintf("%s %s", key, valAnno)] = value
	}

	err = proto.ReadSetEnd()
	if err != nil {
		return nil, "", err
	}

	return m, "map<" + keyType.String() + "," + elemType.String() + ">", nil
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

		val, anno, err := readValue(ctx, proto, id)
		if err != nil {
			return nil, err
		}

		m[fmt.Sprintf("%d %s", seqId, anno)] = val
	}

	err = proto.ReadStructEnd()
	if err != nil {
		return nil, err
	}

	return m, nil
}

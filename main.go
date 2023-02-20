package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/apache/thrift/lib/go/thrift"
)

func main() {
	flag.BoolVar(&withType, "t", true, "print field type")
	flag.Parse()

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
	var realValAnno string = elemType.String()

	for i := 0; i < size; i++ {
		value, anno, err := readValue(ctx, proto, elemType)
		if err != nil {
			return nil, "", err
		}

		if realValAnno == elemType.String() && realValAnno != anno {
			realValAnno = anno
		}
		m[keyOf(i, anno)] = value
	}

	err = proto.ReadListEnd()
	if err != nil {
		return nil, "", err
	}

	return m, "list<" + realValAnno + ">", nil
}

func readSet(ctx context.Context, proto *thrift.TBinaryProtocol) (map[string]interface{}, string, error) {
	elemType, size, err := proto.ReadSetBegin()
	if err != nil {
		return nil, "", err
	}

	m := make(map[string]interface{}, size+1)
	var realValAnno string = elemType.String()

	for i := 0; i < size; i++ {
		value, anno, err := readValue(ctx, proto, elemType)
		if err != nil {
			return nil, "", err
		}

		if realValAnno == elemType.String() && realValAnno != anno {
			realValAnno = anno
		}
		m[keyOf(i, anno)] = value
	}

	err = proto.ReadSetEnd()
	if err != nil {
		return nil, "", err
	}

	return m, "set<" + realValAnno + ">", nil
}

func readMap(ctx context.Context, proto *thrift.TBinaryProtocol) (map[string]interface{}, string, error) {
	keyType, elemType, size, err := proto.ReadMapBegin()
	if err != nil {
		return nil, "", err
	}

	m := make(map[string]interface{}, size+2)

	var realKeyAnno string = keyType.String()
	var realValAnno string = elemType.String()

	for i := 0; i < size; i++ {
		key, keyAnno, err := readValue(ctx, proto, keyType)
		if err != nil {
			return nil, "", err
		}
		if realKeyAnno == keyType.String() && realKeyAnno != keyAnno {
			realKeyAnno = keyAnno
		}

		value, valAnno, err := readValue(ctx, proto, elemType)
		if err != nil {
			return nil, "", err
		}
		if realValAnno == elemType.String() && realValAnno != valAnno {
			realValAnno = valAnno
		}

		m[fmt.Sprint(key)] = value
	}

	err = proto.ReadSetEnd()
	if err != nil {
		return nil, "", err
	}

	return m, "map<" + realKeyAnno + "," + realValAnno + ">", nil
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

		m[keyOf(seqId, anno)] = val
	}

	err = proto.ReadStructEnd()
	if err != nil {
		return nil, err
	}

	return m, nil
}

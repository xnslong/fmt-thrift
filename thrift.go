package main

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
)

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

var withType bool

func keyOf(id interface{}, anno string) string {
	if withType {
		return fmt.Sprintf("%v %v", id, anno)
	} else {
		return fmt.Sprint(id)
	}
}

func readMsg(ctx context.Context, m map[string]interface{}, proto *thrift.TBinaryProtocol) error {
	name, typeId, seqId, err := proto.ReadMessageBegin()
	if err != nil {
		return err
	}

	m[keyOf(1, "NAME")] = name
	m[keyOf(2, "SEQ_ID")] = seqId
	m[keyOf(3, "TYPE")] = MessageType(typeId)

	payload, err := readStruct(ctx, proto)
	if err != nil {
		return err
	}
	if payload != nil {
		m[keyOf(4, "PAYLOAD")] = payload
	}

	err = proto.ReadMessageEnd()
	if err != nil {
		return err
	}

	return nil
}

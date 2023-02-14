package main

import (
	"context"

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

func readMsg(ctx context.Context, m map[string]interface{}, proto *thrift.TBinaryProtocol) error {
	name, typeId, seqId, err := proto.ReadMessageBegin()
	if err != nil {
		return err
	}

	m["1 NAME"] = name
	m["2 SEQ_ID"] = seqId
	m["3 TYPE"] = MessageType(typeId)

	payload, err := readStruct(ctx, proto)
	if err != nil {
		return err
	}
	if payload != nil {
		m["4 PAYLOAD"] = payload
	}

	err = proto.ReadMessageEnd()
	if err != nil {
		return err
	}

	return nil
}

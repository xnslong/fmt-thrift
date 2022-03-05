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


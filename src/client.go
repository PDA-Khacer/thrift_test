package main

import (
	"bigset/thrift/gen-go/openstars/core/bigset/generic"
	"context"
	"fmt"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
)

var defaultCtx = context.Background()

func handleClient(client *generic.TStringBigSetKVServiceClient) (err error) {
	// fmt.Println(client.CreateStringBigSet(defaultCtx, "test_thrift"))
	// fmt.Println(client.GetBigSetInfoByName(defaultCtx, "test_thrift"))
	// fmt.Println(client.AssignBigSetName(defaultCtx, "test_thrift", 0))
	// fmt.Println(client.RemoveAll(defaultCtx, "test_thrift"))
	it := generic.NewTItem()
	it.Value = []byte("hello")
	it.Key = []byte("01")
	fmt.Println(client.BsPutItem(defaultCtx, "test_thrift", it))
	fmt.Println(client.BsExisted(defaultCtx, "test_thrift", []byte("01")))
	fmt.Println(client.GetTotalCount(defaultCtx, "test_thrift"))
	fmt.Println(client.BsGetItem(defaultCtx, "test_thrift", []byte("01")))
	fmt.Println([]byte("hello"))
	return err
}

func main() {
	var transport thrift.TTransport
	var err error
	transport, err = thrift.NewTSocket("127.0.0.1:18990")
	if err != nil {
		log.Fatal("Error opening socket:", err)
	}
	transportBuff := thrift.NewTBufferedTransportFactory(8192)
	transportFactory1 := thrift.NewTFramedTransportFactory(transportBuff)
	transport, err = transportFactory1.GetTransport(transport)
	if err != nil {
		log.Fatal(err)
	}
	protocolFactory := thrift.NewTBinaryProtocolFactory(true, true)
	if err != nil {
		log.Fatal(err)
	}
	defer transport.Close()
	if err := transport.Open(); err != nil {
		log.Fatal(err)
	}
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	handleClient(generic.NewTStringBigSetKVServiceClient(thrift.NewTStandardClient(iprot, oprot)))
}

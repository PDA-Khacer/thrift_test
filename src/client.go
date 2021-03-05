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
	it1 := generic.NewTItem()
	it1.Value = []byte("alo")
	it1.Key = []byte("07")
	it2 := generic.NewTItem()
	it2.Value = []byte("kk")
	it2.Key = []byte("08")
	itemSet := generic.TItemSet{Items: []*generic.TItem{it1, it2, it}}

	fmt.Println(client.CreateStringBigSet(defaultCtx, "test_thrift"))
	fmt.Println(client.BsPutItem(defaultCtx, "test_thrift", it))
	fmt.Println(client.BsPutItem(defaultCtx, "test_thrift", &generic.TItem{Key: []byte("02"), Value: []byte("oke")}))
	fmt.Println(client.BsPutItem(defaultCtx, "test_thrift", &generic.TItem{Key: []byte("03"), Value: []byte("yes")}))
	fmt.Println(client.BsPutItem(defaultCtx, "test_thrift", &generic.TItem{Key: []byte("04"), Value: []byte("sir")}))
	fmt.Println(client.BsPutItem(defaultCtx, "test_thrift", &generic.TItem{Key: []byte("00"), Value: []byte("ii")}))

	fmt.Println(client.BsMultiPut(defaultCtx, "test_thrift", &itemSet, true, true))

	fmt.Println(client.BsExisted(defaultCtx, "test_thrift", []byte("01")))
	fmt.Println(client.AssignBigSetName(defaultCtx, "test_thrift", 0))
	fmt.Println(client.GetTotalCount(defaultCtx, "test_thrift"))
	fmt.Println(client.BsGetItem(defaultCtx, "test_thrift", []byte("01")))
	fmt.Println(client.GetBigSetInfoByName(defaultCtx, "test_thrift"))

	fmt.Println(client.BsGetSlice(defaultCtx, "test_thrift", 0, 2))
	fmt.Println(client.BsGetSliceFromItem(defaultCtx, "test_thrift", []byte("01"), 2))
	// duoc xap xep
	fmt.Println(client.BsGetSliceR(defaultCtx, "test_thrift", 0, 2))
	// do duoc xap xep nen ta lay tu phan tu co khoa cao nhat
	fmt.Println(client.BsGetSliceFromItemR(defaultCtx, "test_thrift", []byte("04"), 2))

	fmt.Println(client.BsRangeQuery(defaultCtx, "test_thrift", []byte("01"), []byte("04")))
	// do 01 truoc 04 nen se khong co du lieu
	fmt.Println(client.BsRangeQuery(defaultCtx, "test_thrift", []byte("04"), []byte("01")))

	// fmt.Println(client.BsBulkLoad(defaultCtx, "test_thrift", &itemSet))

	fmt.Println(client.BsRemoveItem(defaultCtx, "test_thrift", []byte("01")))
	fmt.Println(client.RemoveAll(defaultCtx, "test_thrift"))

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

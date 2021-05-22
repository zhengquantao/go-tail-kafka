package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"sync"
	"time"
)

func main()  {

	go kafka_producer_test()
	go kafka_consumer_test()
	tail_test()
}

func tail_test(){
	fileName := "./my.log"
	config := tail.Config{
		ReOpen: true,
		Follow: true,
		Location: &tail.SeekInfo{Offset:0, Whence:2},
		MustExist: false,
		Poll: true,
	}
	tails, err:= tail.TailFile(fileName, config)
	if err != nil{
		fmt.Println("err", err)
		return
	}
	var(
		line *tail.Line
		ok bool

	)
	for{
		line, ok = <-tails.Lines
		if !ok{
			fmt.Println("tail file close reopen, filename:%s\n", tails.Filename)
			time.Sleep(time.Second)
		}
		fmt.Println("line: ", line.Text)
	}
}


func kafka_producer_test(){
	config := sarama.NewConfig()
	// 发送完数据需要leader和follow都确认
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 选出一个partition
	config.Producer.Partitioner = sarama.NewCustomPartitioner()
	// 成功交付的消息将在success channel 返回
	config.Producer.Return.Successes = true
	// 构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = "web_log"
	msg.Value = sarama.StringEncoder("this is a test log")
	//连接kafka
	client, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	if err !=nil{
		fmt.Println("producer closed, err:", err)
		return
	}
	defer client.Close()
	// 发送消息
	pid, offset, err := client.SendMessage(msg)
	if err != nil{
		fmt.Println("send msg failed, err:", err)
		return
	}
	fmt.Printf("pid:%v offset:%v\n", pid, offset)


}


func kafka_consumer_test(){
	var wg sync.WaitGroup
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)
	if err != nil{
		fmt.Println("Failed to start consumer: %s", err)
		return
	}
	partitionList, err := consumer.Partitions("web_log")  //获得该topic所有的分区
	if err != nil{
		fmt.Println("Failed to get the list of partition:, ",err)
		return
	}
	fmt.Println(partitionList)

	for partition := range partitionList{
		pc, err := consumer.ConsumePartition("web_log", int32(partition), sarama.OffsetNewest)
		if err != nil{
			fmt.Println("Failed to start consumer for partition %d: %s\n", partition, err)
			return
		}
		wg.Add(1)
		go func(sarama.PartitionConsumer) { //为每个分区开一个go协程去取值
			for msg := range pc.Messages(){  //阻塞直到有值发送过来，然后再继续等待
				fmt.Printf("Partition:%d, Offset:%d, key:%s, value:%s\n", msg.Partition, msg.Offset, string(msg.Key),string(msg.Value))
			}
			defer pc.AsyncClose()
			wg.Done()
		}(pc)
	}
	wg.Wait()
	consumer.Close()
}
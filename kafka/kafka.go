package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"sync"
	"time"
)

type logData struct {
	topic string
	data  string
}

var (
	producer    sarama.SyncProducer
	consumer    sarama.Consumer
	logDataChan chan *logData
)

func Init(addr []string, maxSize int) (err error) {
	config := sarama.NewConfig()
	// 发送完数据需要leader和follow都确认
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 选出一个partition
	config.Producer.Partitioner = sarama.NewCustomPartitioner()
	// 成功交付的消息将在success channel 返回
	config.Producer.Return.Successes = true
	//连接kafka
	producer, err = sarama.NewSyncProducer(addr, config)

	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	// 初始化lodDataChan
	logDataChan = make(chan *logData, maxSize)
	// 后台启动
	go sendToKafka()
	return
}

func SendToChan(topic, data string) {
	msg := &logData{
		topic: topic,
		data:  data,
	}
	logDataChan <- msg
}

func sendToKafka() {
	for {
		select {
		case logData := <-logDataChan:
			// 构造一个消息
			msg := &sarama.ProducerMessage{}
			msg.Topic = logData.topic
			msg.Value = sarama.StringEncoder(logData.data)
			// 发送消息
			pid, offset, err := producer.SendMessage(msg)
			if err != nil {
				fmt.Println("send msg failed, err:", err)
				return
			}
			fmt.Printf("pid:%v offset:%v\n", pid, offset)
			fmt.Println("发送成功~")
		default:
			time.Sleep(time.Millisecond * 50)
		}
	}

}

func ConsumeFromKafka(topic string, addr []string) {
	var wg sync.WaitGroup
	consumer, err := sarama.NewConsumer(addr, nil)
	if err != nil {
		fmt.Println("Failed to start consumer: %s", err)
		return
	}
	partitionList, err := consumer.Partitions(topic) //获得该topic所有的分区
	if err != nil {
		fmt.Println("Failed to get the list of partition:, ", err)
		return
	}
	fmt.Println(partitionList)

	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Println("Failed to start consumer for partition %d: %s\n", partition, err)
			return
		}
		wg.Add(1)
		go func(sarama.PartitionConsumer) { //为每个分区开一个go协程去取值
			for msg := range pc.Messages() { //阻塞直到有值发送过来，然后再继续等待
				fmt.Printf("Partition:%d, Offset:%d, key:%s, value:%s\n", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			}
			defer pc.AsyncClose()
			wg.Done()
		}(pc)
	}
	wg.Wait()
	consumer.Close()
}

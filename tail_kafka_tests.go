package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"go.etcd.io/etcd/clientv3"
	"net"
	"strings"
	"sync"
	"time"
)

func main() {
	go kafkaProducerTest()
	go kafkaConsumerTest()
	go etcdPush()
	go etcdGet()
	tailTest()
}

// 获取IP
func GetOutboundIP() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip = strings.Split(localAddr.IP.String(), ":")[0]
	fmt.Println("IP:", ip)
	return
}

//kafka 生产者
func kafkaProducerTest() {
	config := sarama.NewConfig()
	// 发送完数据需要leader和follow都确认
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 选出一个partition
	config.Producer.Partitioner = sarama.NewCustomPartitioner()
	// 成功交付的消息将在success channel 返回
	config.Producer.Return.Successes = true
	// 构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = "redis_log"
	msg.Value = sarama.StringEncoder("this is a test log")
	//连接kafka
	client, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("producer closed, err:", err)
		return
	}
	defer client.Close()
	// 发送消息
	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Println("send msg failed, err:", err)
		return
	}
	fmt.Printf("pid:%v offset:%v\n", pid, offset)
}

// kafka消费者
func kafkaConsumerTest() {
	var wg sync.WaitGroup
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)
	if err != nil {
		fmt.Println("Failed to start consumer: %s", err)
		return
	}
	partitionList, err := consumer.Partitions("redis_log") //获得该topic所有的分区
	if err != nil {
		fmt.Println("Failed to get the list of partition:, ", err)
		return
	}
	fmt.Println(partitionList)

	for partition := range partitionList {
		pc, err := consumer.ConsumePartition("redis_log", int32(partition), sarama.OffsetNewest)
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

// tail读取实时文件
func tailTest() {
	fileName := "./redis.log"
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	tails, err := tail.TailFile(fileName, config)
	if err != nil {
		fmt.Println("err", err)
		return
	}
	var (
		line *tail.Line
		ok   bool
	)
	for {
		line, ok = <-tails.Lines
		if !ok {
			fmt.Println("tail file close reopen, filename:%s\n", tails.Filename)
			time.Sleep(time.Second)
		}
		fmt.Println("line: ", line.Text)
	}
}

// etcd增加
func etcdPush() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	// watch 操作
	// watch 用来过去未来的更改操作
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd faild, %v\n", err)
		return
	}
	fmt.Println("connect to etcd success")
	// 延迟关闭
	defer cli.Close()
	// watch
	// 派一个哨兵 一直监视着 某个key的变化（新增、修改、删除）
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//value := `[{"path": "./nginx.log", "topic": "web_log"},{"path": "./redis.log", "topic": "redis_log"},{"path": "./mysql.log", "topic": "mysql_log"}]`
	value := `[{"path": "./nginx.log", "topic": "web_log"},{"path": "./redis.log", "topic": "redis_log"}]`
	key := "/collect/%s/collect_config"
	//先拉取IP
	ipStr, err := GetOutboundIP()
	if err != nil {
		panic(err)
	}
	etcdConfKey := fmt.Sprint(key, ipStr)
	cli.Put(ctx, etcdConfKey, value)
	cancel()
	if err != nil {
		fmt.Printf("put to etcd failed, err:%v", err)
		return
	}
}

func etcdGet() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	// watch 操作
	// watch 用来过去未来的更改操作
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd faild, %v\n", err)
		return
	}
	fmt.Println("connect to etcd success")
	// 延迟关闭
	defer cli.Close()
	// watch
	// 派一个哨兵 一直监视着 某个key的变化（新增、修改、删除）
	key := "/collect/%s/collect_config"
	//先拉取IP
	ipStr, err := GetOutboundIP()
	etcdConfKey := fmt.Sprint(key, ipStr)
	if err != nil {
		panic(err)
	}
	ch := cli.Watch(context.Background(), etcdConfKey)
	for msg := range ch {
		for _, evt := range msg.Events {
			fmt.Printf("Type:%v Key:%v value:%v\n", evt.Type, string(evt.Kv.Key), string(evt.Kv.Value))
		}
	}
}

package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"kafka_test/conf"
	"kafka_test/etcd"
	"kafka_test/kafka"
	"kafka_test/taillog"
	"kafka_test/util"
	"sync"
	"time"
)

var (
	cfg = new(conf.AppConf)
)

func run() {
	// 生产者
	//go func() {
	//	// 1.读取日志 发送到kafka
	//	for {
	//		select {
	//		//得到消息再往kafka里面推送
	//		case line := <-taillog.ReadChan():
	//			// 2. 发送到kafka
	//			kafka.SendToKafka(cfg.Topic, line.Text)
	//		default:
	//			// 取不到数据 使cpu停一秒
	//			time.Sleep(time.Second)
	//		}
	//	}
	//}()
	// 消费者
	kafka.ConsumeFromKafka(cfg.Topic, []string{cfg.KafkaConf.Address})
}

func main() {
	// 加载配置文件
	errINI := ini.MapTo(cfg, "./conf/config.ini")
	if errINI != nil {
		fmt.Printf("load ini failed, err:%v \n", errINI)
		return
	}
	// 初始化kafka
	err := kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.ChanMaxSize)
	if err != nil {
		fmt.Printf("kafka init error:%v", err)
		return
	}
	fmt.Println("kafka init success")

	// 初始化 ETCD
	err = etcd.Init(cfg.EtcdConf.Address, time.Duration(cfg.EtcdConf.Timeout)*time.Second)
	if err != nil {
		fmt.Printf("etcd init error:%v", err)
		return
	}

	//先拉取IP
	ipStr, err := util.GetOutboundIP()
	if err != nil {
		panic(err)
	}
	etcdConfKey := fmt.Sprint(cfg.EtcdConf.Key, ipStr)
	logEntryConf, err := etcd.GetEtcdConf(etcdConfKey)
	for k, v := range logEntryConf {
		fmt.Println(k, v)
	}
	// 3.收集日志送至kafka
	// 3.1 循环每一个日志收集项，创建TailObj

	taillog.Init(logEntryConf)
	newChan := taillog.NewConfChan() // 获取新通道
	var wg sync.WaitGroup
	wg.Add(1)
	go etcd.WaterEtcdConf(cfg.EtcdConf.Key, newChan)
	wg.Wait()
}

package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"kafka_test/conf"
	"kafka_test/kafka"
	"kafka_test/taillog"
	"time"
)

var (
	cfg = new(conf.AppConf)
)

func run() {
	// 生产者
	go func() {
		// 1.读取日志 发送到kafka
		for {
			select {
			//得到消息再往kafka里面推送
			case line := <-taillog.ReadChan():
				// 2. 发送到kafka
				kafka.SendToKafka(cfg.Topic, line.Text)
			default:
				// 取不到数据 使cpu停一秒
				time.Sleep(time.Second)
			}
		}
	}()
	// 消费者
	kafka.ConsumeFromKafka(cfg.Topic, []string{cfg.Address})
}

func main() {
	// 加载配置文件
	errINI := ini.MapTo(cfg, "./conf/config.ini")
	if errINI != nil {
		fmt.Printf("load ini failed, err:%v \n", errINI)
		return
	}
	// 初始化kafka
	err := kafka.Init([]string{cfg.Address})
	if err != nil {
		fmt.Printf("kafka init error:%v", err)
		return
	}
	fmt.Println("kafka init success")
	// 打开日志文件收集日志
	errTail := taillog.Init(cfg.Path)
	if errTail != nil {
		fmt.Printf("tail init error:%v", errTail)
		return
	}
	fmt.Println("tail init success")
	// 业务逻辑
	run()
}

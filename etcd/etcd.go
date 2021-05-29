package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

var (
	cli *clientv3.Client
)

type LogEntry struct {
	Path  string `json:"path"`
	Topic string `json:"topic"`
}

func Init(addr string, timeout time.Duration) (err error) {
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: timeout * time.Second,
	})
	// watch 操作
	if err != nil {
		// handle error!
		fmt.Printf("connect to etcd faild, %v\n", err)
		return
	}
	return
}

func GetEtcdConf(key string) (logEntryConf []*LogEntry, err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	resp, err := cli.Get(ctx, key)
	if err != nil {
		fmt.Printf("get from etcd failed")
		return
	}
	for _, ev := range resp.Kvs {
		err = json.Unmarshal(ev.Value, &logEntryConf)
		if err != nil {
			fmt.Printf("unmarshal etcd value failed, err: %v\n", err)
		}
	}
	return

}

func WaterEtcdConf(key string, newConfCh chan<- []*LogEntry) {
	ch := cli.Watch(context.Background(), key)
	for msg := range ch {
		for _, evt := range msg.Events {
			fmt.Printf("Type:%v Key:%v value:%v\n", evt.Type, string(evt.Kv.Key), string(evt.Kv.Value))
			//通知taillog.tskMgr
			//1.先判断操作的类型
			var newConf []*LogEntry
			if evt.Type != clientv3.EventTypeDelete {
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					fmt.Printf("unmarshal failed, err:%v\n", err)
					continue
				}
			}

			fmt.Println("new conf", newConf)
			newConfCh <- newConf
		}
	}
}

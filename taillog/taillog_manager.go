package taillog

import (
	"fmt"
	"kafka_test/etcd"
	"time"
)

var tskMgr *tailLogMgr

// tailTask 管理者
type tailLogMgr struct {
	logEntry    []*etcd.LogEntry
	tskMap      map[string]*TailTask
	newConfChan chan []*etcd.LogEntry
}

func Init(logEntryConf []*etcd.LogEntry) {
	tskMgr = &tailLogMgr{
		logEntry:    logEntryConf,
		tskMap:      make(map[string]*TailTask, 16),
		newConfChan: make(chan []*etcd.LogEntry), //无缓冲区的通道
	}
	for _, logEntry := range logEntryConf {
		// conf: *etcd.logEntry
		// logEntry.Path: 要收集的日志文件的路径
		tailObj := NewTailTask(logEntry.Path, logEntry.Topic)
		pt := fmt.Sprint("%s_%s", logEntry.Path, logEntry.Topic)
		tskMgr.tskMap[pt] = tailObj
	}
}

//监听newConfChan
func (t *tailLogMgr) run() {
	for {
		select {
		case newConf := <-t.newConfChan:
			fmt.Println(newConf)
			for _, conf := range newConf {
				pt := fmt.Sprint("%s_%s", conf.Path, conf.Topic)
				_, ok := t.tskMap[pt]
				if ok {
					continue
				} else {
					tailObj := NewTailTask(conf.Path, conf.Topic)
					t.tskMap[pt] = tailObj
				}
			}
			//　找出原来t.logEntry 有 但是newConf已经删除 要delete
			for _, c1 := range newConf {
				isDelete := true
				for _, c2 := range newConf {
					if c2.Path == c1.Path && c2.Topic == c1.Topic {
						isDelete = false
						continue
					}
				}
				if isDelete {
					// c1对应的这个tailObj停掉
					pt := fmt.Sprintf("%s_%s", c1.Path, c1.Topic)
					t.tskMap[pt].cancelFunc()
				}
			}
		default:
			time.Sleep(time.Second)
		}
	}
}

func NewConfChan() chan<- []*etcd.LogEntry {
	return tskMgr.newConfChan
}

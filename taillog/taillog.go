package taillog

import (
	"context"
	"fmt"
	"github.com/hpcloud/tail"
	"kafka_test/kafka"
)

var (
	tailObj *tail.Tail
	logChan chan string
)

type TailTask struct {
	path       string
	topic      string
	instance   *tail.Tail
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewTailTask(path, topic string) (tailObj *TailTask) {
	ctx, cancel := context.WithCancel(context.Background())
	tailObj = &TailTask{
		path:       path,
		topic:      topic,
		ctx:        ctx,
		cancelFunc: cancel,
	}
	tailObj.Init() // 根据路径去打开对应的日志
	return
}

func (t *TailTask) Init() {
	config := tail.Config{
		ReOpen:    true,
		Follow:    true,
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2},
		MustExist: false,
		Poll:      true,
	}
	var err error
	t.instance, err = tail.TailFile(t.path, config)
	if err != nil {
		fmt.Println("err", err)
		return
	}
	go t.Run() //后台收集日志发至kafka
}

func (t *TailTask) Run() {
	for {
		select {
		case <-t.ctx.Done():
			fmt.Printf("tail task:%s_%s 结束了", t.path, t.topic)
			return
		case line := <-t.instance.Lines: //从tailObj的通道中一行一行的读取日志数据
			kafka.SendToChan(t.topic, line.Text) //函数调函数
		}
	}
}

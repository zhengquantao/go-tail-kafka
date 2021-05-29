package conf

type AppConf struct {
	KafkaConf `ini:"kafka"`
	TailConf  `ini:"tail"`
	EtcdConf  `ini:"etcd"`
}

type KafkaConf struct {
	Address     string `ini:"address"`
	Topic       string `ini:"topic"`
	ChanMaxSize int    `ini:"chan_max_size"`
}

type TailConf struct {
	Path string `ini:"path"`
}

type EtcdConf struct {
	Address string `ini:"address"`
	Key     string `ini:"collect_key"`
	Timeout int    `ini:"timeout"`
}

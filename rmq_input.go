/*
* heka input plugin for rmq
* create by： dotwoo@gmail.com
 */
package heka2rmq

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/adjust/rmq"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/pborman/uuid"
	"gopkg.in/redis.v3"
)

const (
	unackedLimit = 1000
)

// rmq  consumer config
type RmqInputConfig struct {
	Name     string `toml:"name"`
	RmqAddr  string `toml:"rmq_address"` // redis address
	Queue    string `toml:"queue"`       // rmq queue name
	Password string `toml:"password"`    // redis password
	DBid     int    `toml:"dbid"`        // redis db id

	// Deadlines for network reads and writes
	ConnectTimeout time.Duration `toml:"connect_timeout"`
	ReadTimeout    time.Duration `toml:"read_timeout"`
	WriteTimeout   time.Duration `toml:"write_timeout"`
}

// heka input plugin struct
type RmqInput struct {
	Rconfig                *RmqInputConfig
	RmqQueue               rmq.Queue
	Queue                  string
	name                   string
	processMessageCount    int64
	processMessageFailures int64

	Stopchan   chan bool
	ir         pipeline.InputRunner
	pConfig    *pipeline.PipelineConfig
	packSupply chan *pipeline.PipelinePack
}

func (r *RmqInput) ConfigStruct() interface{} {
	hn := r.pConfig.Hostname()
	return &RmqInputConfig{
		Name:           hn,
		ConnectTimeout: 30 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	}
}

func (r *RmqInput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	r.pConfig = pConfig
}

func (r *RmqInput) SetName(name string) {
	r.name = name
}

func (r *RmqInput) Init(config interface{}) (err error) {
	conf := config.(*RmqInputConfig)
	r.Rconfig = conf

	redisop := &redis.Options{
		Network: "tcp",
		Addr:    conf.RmqAddr,
		DB:      int64(conf.DBid),
	}
	if conf.Password != "" {
		redisop.Password = conf.Password
	}
	if conf.ConnectTimeout != 0 {
		redisop.DialTimeout = conf.ConnectTimeout
	}
	if conf.ReadTimeout != 0 {
		redisop.ReadTimeout = conf.ReadTimeout
	}
	if conf.WriteTimeout != 0 {
		redisop.WriteTimeout = conf.WriteTimeout
	}
	redisClient := redis.NewClient(redisop)
	redisconnect := rmq.OpenConnectionWithRedisClient("consumer", redisClient)
	r.RmqQueue = redisconnect.OpenQueue(conf.Queue)

	return nil
}

func (r *RmqInput) Run(ir pipeline.InputRunner,
	helper pipeline.PluginHelper) (err error) {

	dRunner := ir.NewSplitterRunner("")
	defer func() {
		dRunner.Done()
	}()
	r.ir = ir
	r.Stopchan = make(chan bool)
	r.packSupply = ir.InChan()
	r.RmqQueue.StartConsuming(unackedLimit, 500*time.Millisecond)
	r.RmqQueue.AddConsumer(r.name, r)
	<-r.Stopchan

	return nil
}

func (r *RmqInput) Stop() {
	r.RmqQueue.StopConsuming()
	r.Stopchan <- true
}

func (r *RmqInput) addField(pack *pipeline.PipelinePack, name string,
	value interface{}, representation string) {

	if field, err := message.NewField(name, value, representation); err == nil {
		pack.Message.AddField(field)
	} else {
		r.ir.LogError(fmt.Errorf("can't add '%s' field: %s", name, err.Error()))
	}
}

func (r *RmqInput) Consume(delivery rmq.Delivery) {
	pack := <-r.packSupply

	pack.Message.SetUuid(uuid.NewRandom())
	pack.Message.SetTimestamp(time.Now().UnixNano())
	pack.Message.SetType("rmq.input")
	pack.Message.SetHostname(r.Queue)
	pack.Message.SetPayload(string(delivery.Payload()))
	r.addField(pack, "Queue", r.Queue, "")
	r.addField(pack, "BDID", r.Rconfig.DBid, "")
	r.processMessageCount++

	message.NewStringField(pack.Message, "queue", r.Queue)
	//message.NewStringField(pack.Message, "channel", input.Channel)
	r.ir.Deliver(pack)

	delivery.Ack()

	return
}

func (r *RmqInput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&r.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures",
		atomic.LoadInt64(&r.processMessageFailures), "count")
	return nil
}

func init() {
	pipeline.RegisterPlugin("RmqInput", func() interface{} {
		return new(RmqInput)
	})
}

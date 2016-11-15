/*
* heka input plugin for rmq
* create by： dotwoo@gmail.com
 */
package heka2rmq

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"gopkg.in/redis.v3"

	"github.com/adjust/rmq"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

const (
	ModeRoundRobin = iota
	ModeHostPool
)

type RmqOutputConfig struct {
	Name     string `toml:"name"`
	RmqAddr  string `toml:"rmq_address"`
	Queue    string `toml:"queue"`
	Password string `toml:"password"`
	DBid     int    `toml:"dbid"`

	// Deadlines for network reads and writes
	ConnectTimeout time.Duration `toml:"connect_timeout"`
	ReadTimeout    time.Duration `toml:"read_timeout"`
	WriteTimeout   time.Duration `toml:"write_timeout"`

	RetryQueueSize uint64 `toml:"retry_queue_size"`
	MaxMsgRetries  uint64 `toml:"max_msg_retries"`
	RetryOptions   *pipeline.RetryOptions
}

type RmqOutput struct {
	Rconfig                *RmqOutputConfig
	RmqQueue               rmq.Queue
	Queue                  string
	name                   string
	processMessageCount    int64
	processMessageFailures int64

	ir          pipeline.OutputRunner
	retryChan   chan RetryMsg
	retryHelper *pipeline.RetryHelper
	pConfig     *pipeline.PipelineConfig
}

type RetryMsg struct {
	count     uint64
	maxCount  uint64
	Body      []byte
	retryChan chan RetryMsg
}

func (m RetryMsg) Retry() error {
	if m.count > m.maxCount {
		return errors.New("Exceeded max retry attempts sending message. No longer requeuing message.")
	}
	m.retryChan <- m
	m.count++
	return nil
}
func (r *RmqOutput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	r.pConfig = pConfig
}

func (r *RmqOutput) ConfigStruct() interface{} {
	hn := r.pConfig.Hostname()
	return &RmqOutputConfig{
		Name:           hn,
		ConnectTimeout: 30 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,

		RetryQueueSize: 50,
		MaxMsgRetries:  3,
		RetryOptions: &pipeline.RetryOptions{
			MaxRetries: 3,
			Delay:      "1s",
		},
	}
}

func (r *RmqOutput) Init(config interface{}) (err error) {
	conf := config.(*RmqOutputConfig)
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

	r.retryHelper, err = pipeline.NewRetryHelper(*conf.RetryOptions)
	if err != nil {
		fmt.Println("NewRetryHelper,error:", err)
		return
	}

	r.retryChan = make(chan RetryMsg, r.Rconfig.RetryQueueSize)
	return nil
}

func (r *RmqOutput) Run(ir pipeline.OutputRunner,
	helper pipeline.PluginHelper) (err error) {
	if ir.Encoder() == nil {
		return errors.New("Encoder required.")
	}

	var (
		pack     *pipeline.PipelinePack
		outgoing []byte
		msg      RetryMsg
	)

	r.ir = ir
	inChan := ir.InChan()
	ok := true

	for ok {
		select {
		case pack, ok = <-inChan:
			if !ok {
				fmt.Println("inchan read err! return")
				return nil
			}
			r.processMessageCount++
			outgoing, err = r.ir.Encode(pack)
			if err != nil {
				r.ir.LogError(err)
			} else {
				sok := r.RmqQueue.PublishBytes(outgoing)
				if sok == false {
					r.processMessageFailures++
					r.ir.LogError(err)
					err = r.retryHelper.Wait()
					if err != nil {
						fmt.Println("this.retryHelper.Wait! return")
						return
					}
					// Create a retry msg, and requeue it
					msg := RetryMsg{Body: outgoing, retryChan: r.retryChan, maxCount: r.Rconfig.MaxMsgRetries}
					err = msg.Retry()
					if err != nil {
						r.ir.LogError(err)
					}
				} else {
					r.retryHelper.Reset()
				}
			}
			pack.Recycle(nil)
		case msg, ok = <-r.retryChan:
			if !ok {
				fmt.Println("retry chan read err! return")
				return nil
			}
			sok := r.RmqQueue.PublishBytes(msg.Body)
			if sok == false {
				r.ir.LogError(err)
				err = r.retryHelper.Wait()
				if err != nil {
					fmt.Println("retrychan wait! return")
					return
				}
				// requeue the message
				err = msg.Retry()
				if err != nil {
					r.ir.LogError(err)
				}
			} else {
				r.retryHelper.Reset()
			}
		}
	}

	return nil
}

func (r *RmqOutput) cleanup() {
	close(r.retryChan)
	r.RmqQueue.Close()
	r.RmqQueue = nil
}

func (r *RmqOutput) CleanupForRestart() {
	r.cleanup()
	return
}

func (r *RmqOutput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&r.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures",
		atomic.LoadInt64(&r.processMessageFailures), "count")
	return nil
}

func init() {
	pipeline.RegisterPlugin("RmqOutput", func() interface{} {
		return new(RmqOutput)
	})
}

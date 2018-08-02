package sqsx

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"sync/atomic"
	"time"
)

type ConsumerConfig struct {
	MaxWorkers int

	PollTimeout time.Duration
}

type Consumer interface {
	Start(handler interface{}) error
	Stop()
}

type consumer struct {
	queueName, queueURL string
	config              *ConsumerConfig
	svc                 Service
	stop                chan chan bool

	consumeFn     func(m *sqs.Message, handler interface{}) error
	deleteMessage func(m *sqs.Message)
}

func (c consumer) Start(handler interface{}) error {
	var stopped chan bool
	free := int32(c.config.MaxWorkers)
	for {
		select {
		case stopped = <-c.stop:
			for {
				if atomic.LoadInt32(&free) == int32(c.config.MaxWorkers) {
					stopped <- true
					return nil
				}
			}
		default:
			maxMessages := atomic.LoadInt32(&free)
			if maxMessages == 0 {
				continue
			}
			if SQSMaxBatchSize < maxMessages {
				maxMessages = SQSMaxBatchSize
			}

			result, err := c.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(c.queueURL),
				MaxNumberOfMessages: aws.Int64(int64(maxMessages)),
				WaitTimeSeconds:     aws.Int64(int64(c.config.PollTimeout.Seconds())),
			})
			if err != nil {
				return errorf(err, "unable to receive message(s) from queue %q", c.queueName)
			}

			if len(result.Messages) <= 0 {
				continue
			}
			for _, m := range result.Messages {
				atomic.AddInt32(&free, -1)
				go func(m *sqs.Message) {
					// TODO: consume
					c.consumeFn(m, handler)
					atomic.AddInt32(&free, 1)
				}(m)
			}
		}
	}
}

func (c consumer) consume(m *sqs.Message, handler interface{}) error {
	return nil
}

func (c consumer) Stop() {
	done := make(chan bool)
	c.stop <- done
	<-done
}

func NewConsumer(queueName string, svc Service, config ...*ConsumerConfig) (Consumer, error) {
	if queueName == "" {
		return nil, ErrQueueDoesNotExist
	}
	resp, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			return nil, ErrQueueDoesNotExist
		}
		return nil, errorf(err, "could not get outbox URL")
	}

	c := &consumer{
		queueName: queueName,
		queueURL:  aws.StringValue(resp.QueueUrl),
		svc:       svc,
		stop:      make(chan chan bool),
		config:    &ConsumerConfig{PollTimeout: SQSMaxPollTimeout, MaxWorkers: 1},
	}
	for _, cfg := range config {
		if cfg == nil {
			continue
		}
		if cfg.PollTimeout < SQSMaxPollTimeout {
			c.config.PollTimeout = cfg.PollTimeout
		}
		if cfg.MaxWorkers > 0 {
			c.config.MaxWorkers = cfg.MaxWorkers
		}
	}
	c.consumeFn = c.consume
	return c, nil
}

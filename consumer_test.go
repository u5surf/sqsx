package sqsx

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewConsumer(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		svc := &mockService{}
		p, err := NewConsumer("QUEUE_NAME", svc)
		if assert.NoError(t, err) {
			impl := p.(*consumer)
			assert.Equal(t, "QUEUE_NAME", impl.queueName)
			assert.Equal(t, "QUEUE_URL", impl.queueURL)
			assert.NotNil(t, impl.stop)
			assert.NotNil(t, impl.svc)
			assert.NotNil(t, impl.svc)
			if assert.NotNil(t, impl.config) {
				assert.Equal(t, SQSMaxPollTimeout, impl.config.PollTimeout)
			}
		}
	})

	t.Run("QueueDoesNotExist", func(t *testing.T) {
		svc := &mockService{}
		svc.getQueueUrl = func(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
			return nil, awserr.New(sqs.ErrCodeQueueDoesNotExist, "", nil)
		}
		_, err := NewConsumer("QUEUE_NAME", svc)
		assert.Equal(t, ErrQueueDoesNotExist, err)

		_, err = NewConsumer("", svc)
		assert.Equal(t, ErrQueueDoesNotExist, err)
	})

	t.Run("UnknownErrorGetQueueURL", func(t *testing.T) {
		svc := &mockService{}
		svc.getQueueUrl = func(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
			return nil, errors.New("unknown")
		}
		_, err := NewConsumer("QUEUE_NAME", svc)
		assert.Error(t, err)
	})

	t.Run("Config", func(t *testing.T) {
		svc := &mockService{}
		c := ConsumerConfig{PollTimeout: time.Second}
		con, err := NewConsumer("QUEUE_NAME", svc, &c)
		impl := con.(*consumer)
		if assert.NoError(t, err) {
			if assert.NotNil(t, impl.config) {
				assert.Equal(t, time.Second, impl.config.PollTimeout)
			}
		}
	})

	t.Run("MultipleConfig", func(t *testing.T) {
		svc := &mockService{}
		c := ConsumerConfig{PollTimeout: time.Second * 5}
		c2 := ConsumerConfig{PollTimeout: time.Second * 10}
		con, err := NewConsumer("QUEUE_NAME", svc, &c, nil, &c2)
		impl := con.(*consumer)
		if assert.NoError(t, err) {
			if assert.NotNil(t, impl.config) {
				assert.Equal(t, time.Second*10, impl.config.PollTimeout)
			}
		}
	})
}

func TestConsumer_Start(t *testing.T) {
	var pollTimeout = time.Second * 5
	t.Run("OK_OneMessage", func(t *testing.T) {
		recvCount := 0
		svc := &mockService{}
		svc.receiveMessage = func(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
			if recvCount == 0 {
				recvCount++
				return &sqs.ReceiveMessageOutput{
					Messages: []*sqs.Message{
						{
							MessageId: aws.String("msg_0"),
						},
					},
				}, nil
			}
			<-time.NewTimer(pollTimeout).C
			return &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{}}, nil
		}
		consumeCount := 0
		con, _ := NewConsumer("QUEUE_NAME", svc, &ConsumerConfig{PollTimeout: pollTimeout})
		con.(*consumer).consumeFn = func(m *sqs.Message, handler interface{}) error {
			consumeCount++
			return nil
		}
		go con.Start(nil)
		<-time.NewTimer(time.Second).C
		assert.Equal(t, 1, consumeCount)
	})

	t.Run("OK_ThreeMessages", func(t *testing.T) {
		recvCount := 0
		svc := &mockService{}
		svc.receiveMessage = func(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
			if recvCount < 3 {
				recvCount++
				return &sqs.ReceiveMessageOutput{
					Messages: []*sqs.Message{
						{
							MessageId: aws.String("msg_0"),
						},
					},
				}, nil
			}
			<-time.NewTimer(pollTimeout).C
			return &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{}}, nil
		}
		consumeCount := 0
		con, _ := NewConsumer("QUEUE_NAME", svc, &ConsumerConfig{PollTimeout: pollTimeout})
		con.(*consumer).consumeFn = func(m *sqs.Message, handler interface{}) error {
			consumeCount++
			return nil
		}
		go con.Start(nil)
		<-time.NewTimer(time.Second).C
		assert.Equal(t, 3, consumeCount)
	})

	t.Run("OK_SimulateProcessing", func(t *testing.T) {
		recvCount := 0
		svc := &mockService{}
		svc.receiveMessage = func(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
			switch recvCount {
			case 0:
				recvCount++
				assert.Equal(t, int64(2), aws.Int64Value(input.MaxNumberOfMessages))
				return &sqs.ReceiveMessageOutput{
					Messages: []*sqs.Message{
						{
							MessageId: aws.String("msg_0"),
						},
						{
							MessageId: aws.String("msg_1"),
						},
					},
				}, nil
			case 1:
				// Simulate polling, wait 1.5s before sending the next messages.
				// Since msg_1 is returned before msg_0, receive message gets called
				// before msg_0's worker is free. Therefore we expect the max number
				// of messages requested to be 1, instead of 2.
				recvCount++
				assert.Equal(t, int64(1), aws.Int64Value(input.MaxNumberOfMessages))
				<-time.NewTimer(time.Millisecond * 1500).C
				return &sqs.ReceiveMessageOutput{
					Messages: []*sqs.Message{
						{
							MessageId: aws.String("msg_3"),
						},
					},
				}, nil
			default:
				<-time.NewTimer(pollTimeout).C
				return &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{}}, nil
			}
		}
		consumeCount := int32(0)
		con, _ := NewConsumer("QUEUE_NAME", svc, &ConsumerConfig{MaxWorkers: 2, PollTimeout: pollTimeout})
		con.(*consumer).consumeFn = func(m *sqs.Message, handler interface{}) error {
			switch aws.StringValue(m.MessageId) {
			case "msg_0":
				<-time.NewTimer(time.Millisecond * 1000).C
			case "msg_1":
				<-time.NewTimer(time.Millisecond * 800).C
			case "msg_2":
				<-time.NewTimer(time.Second).C
			}
			atomic.AddInt32(&consumeCount, 1)
			return nil
		}
		go con.Start(nil)
		<-time.NewTimer(time.Second * 4).C
		assert.Equal(t, int32(3), consumeCount)
	})

	t.Run("ErrorReceiveMessage", func(t *testing.T) {
		svc := &mockService{}
		svc.receiveMessage = func(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
			return nil, errors.New("error")
		}
		con, _ := NewConsumer("QUEUE_NAME", svc, &ConsumerConfig{PollTimeout: pollTimeout})
		err := con.Start(nil)
		assert.Error(t, err)
	})

	t.Run("NoMessages", func(t *testing.T) {
		svc := &mockService{}
		svc.receiveMessage = func(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
			return &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{}}, nil
		}
		con, _ := NewConsumer("QUEUE_NAME", svc, &ConsumerConfig{PollTimeout: pollTimeout})
		go con.Start(nil)
		<-time.NewTimer(time.Second).C
	})

	t.Run("15_MaxWorker", func(t *testing.T) {
		svc := &mockService{}
		svc.receiveMessage = func(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
			return &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{}}, nil
		}
		con, _ := NewConsumer("QUEUE_NAME", svc, &ConsumerConfig{MaxWorkers: 15, PollTimeout: pollTimeout})
		go con.Start(nil)
		<-time.NewTimer(time.Second).C
	})
}
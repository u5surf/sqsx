package sqsx

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var _ = (Service)(&mockService{})

type mockService struct {
	getQueueUrl             func(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error)
	sendMessageBatch        func(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
	receiveMessage          func(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	deleteMessage           func(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
	changeMessageVisibility func(input *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error)
}

func (s *mockService) GetQueueUrl(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	if s.getQueueUrl == nil {
		return &sqs.GetQueueUrlOutput{QueueUrl: aws.String("QUEUE_URL")}, nil
	}
	return s.getQueueUrl(input)
}

func (s *mockService) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	return s.sendMessageBatch(input)
}

func (s *mockService) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	return s.receiveMessage(input)
}

func (s *mockService) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return s.deleteMessage(input)
}

func (s *mockService) ChangeMessageVisibility(input *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error) {
	return s.changeMessageVisibility(input)
}

type mockPubImpl struct {
	Publisher
	stopFn         func()
	startFn        func()
	publishFn      func(msg interface{}, config ...*MessageConfig) error
	publishBatchFn func(input []*envelope)
}

func (m mockPubImpl) Stop() {
	if m.stopFn != nil {
		m.stopFn()
		return
	}
	m.Publisher.Stop()
}

func (m mockPubImpl) Start() {
	if m.startFn != nil {
		m.startFn()
		return
	}
	m.Publisher.Start()
}

func (m mockPubImpl) Publish(msg interface{}, config ...*MessageConfig) error {
	if m.publishFn != nil {
		return m.publishFn(msg, config...)
	}
	return m.Publisher.Publish(msg, config...)
}

func (m mockPubImpl) publishBatch(input []*envelope) {
	if m.publishBatchFn != nil {
		m.publishBatchFn(input)
	}
	m.Publisher.(*publisher).publishBatch(input)
}

func TestNewPublisher(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		svc := &mockService{}
		p, err := NewPublisher("QUEUE_NAME", svc)
		if assert.NoError(t, err) {
			impl := p.(*publisher)
			assert.Equal(t, "QUEUE_NAME", impl.queueName)
			assert.Equal(t, "QUEUE_URL", impl.queueURL)
			assert.NotNil(t, impl.stop)
			assert.NotNil(t, impl.svc)
			assert.NotNil(t, impl.jsonMarshalFn)
			assert.NotNil(t, impl.publishBatchFn)
			if assert.NotNil(t, impl.config) {
				assert.Equal(t, time.Millisecond*7, impl.config.BatchWindow)
			}
		}
	})

	t.Run("QueueDoesNotExist", func(t *testing.T) {
		svc := &mockService{}
		svc.getQueueUrl = func(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
			return nil, awserr.New(sqs.ErrCodeQueueDoesNotExist, "", nil)
		}
		_, err := NewPublisher("QUEUE_NAME", svc)
		assert.Equal(t, ErrQueueDoesNotExist, err)

		_, err = NewPublisher("", svc)
		assert.Equal(t, ErrQueueDoesNotExist, err)
	})

	t.Run("UnknownErrorGetQueueURL", func(t *testing.T) {
		svc := &mockService{}
		svc.getQueueUrl = func(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
			return nil, errors.New("unknown")
		}
		_, err := NewPublisher("QUEUE_NAME", svc)
		assert.Error(t, err)
	})

	t.Run("Config", func(t *testing.T) {
		svc := &mockService{}
		c := PublisherConfig{BatchWindow: time.Second}
		p, err := NewPublisher("QUEUE_NAME", svc, &c)
		impl := p.(*publisher)
		if assert.NoError(t, err) {
			if assert.NotNil(t, impl.config) {
				assert.Equal(t, time.Second, impl.config.BatchWindow)
			}
		}
	})

	t.Run("MultipleConfig", func(t *testing.T) {
		svc := &mockService{}
		c := PublisherConfig{BatchWindow: time.Second}
		c2 := PublisherConfig{BatchWindow: time.Millisecond}
		p, err := NewPublisher("QUEUE_NAME", svc, &c, nil, &c2)
		impl := p.(*publisher)
		if assert.NoError(t, err) {
			if assert.NotNil(t, impl.config) {
				assert.Equal(t, time.Millisecond, impl.config.BatchWindow)
			}
		}
	})
}

func TestPubImpl_Stop(t *testing.T) {
	svc := &mockService{}
	p, _ := NewPublisher("QUEUE_NAME", svc)
	impl := p.(*publisher)
	go func() {
		select {
		case w := <-impl.stop:
			w <- true
		}
	}()
	p.Stop()
}

func TestPubImpl_Start(t *testing.T) {
	t.Run("TriggerMaxSQSBatchSize", func(t *testing.T) {
		svc := &mockService{}
		p, _ := NewPublisher("QUEUE_NAME", svc)

		c := 0
		done := make(chan bool)
		impl := p.(*publisher)
		impl.publishBatchFn = func(input []*envelope) {
			t.Logf("publishBatchFn: %d messages", len(input))
			switch c {
			case 0:
				assert.Len(t, input, 10)
			default:
				assert.Equal(t, 1, c, "publish called more than once: %d", c)
				return
			}
			c++
			close(done)
		}
		go impl.Start()
		for i := 0; i < 10; i++ {
			impl.outbox <- envelope{body: fmt.Sprintf("m_%d", i), status: make(chan error)}
		}
		<-done
	})

	t.Run("TriggerBatchWindow", func(t *testing.T) {
		svc := &mockService{}
		p, _ := NewPublisher("QUEUE_NAME", svc)

		c := 0
		done := make(chan bool)
		impl := p.(*publisher)
		impl.publishBatchFn = func(input []*envelope) {
			t.Logf("publishBatchFn: %d messages", len(input))
			switch c {
			case 0:
				assert.Len(t, input, 3)
			default:
				assert.Equal(t, 1, c, "publish called more than once: %d", c)
				return
			}
			c++
			close(done)
		}
		go impl.Start()
		for i := 0; i < 3; i++ {
			impl.outbox <- envelope{body: fmt.Sprintf("m_%d", i), status: make(chan error)}
		}
		<-done
	})

	t.Run("TriggerStop", func(t *testing.T) {
		svc := &mockService{}
		p, _ := NewPublisher("QUEUE_NAME", svc, &PublisherConfig{BatchWindow: time.Second * 100})

		c := 0
		impl := p.(*publisher)
		impl.publishBatchFn = func(input []*envelope) {
			t.Logf("publishBatchFn: %d messages", len(input))
			switch c {
			case 0:
				assert.Len(t, input, 5)
			default:
				assert.Equal(t, 1, c, "publish called more than once: %d", c)
				return
			}
			c++
		}
		go impl.Start()
		for i := 0; i < 5; i++ {
			impl.outbox <- envelope{body: fmt.Sprintf("m_%d", i), status: make(chan error)}
		}
		done := make(chan bool)
		impl.stop <- done
		<-done
	})

	t.Run("TriggerAll", func(t *testing.T) {
		svc := &mockService{}
		p, _ := NewPublisher("QUEUE_NAME", svc, &PublisherConfig{BatchWindow: time.Second})

		c := 0
		impl := p.(*publisher)
		done := make(chan bool)
		impl.publishBatchFn = func(input []*envelope) {
			switch c {
			case 0:
				t.Logf("SQSBatchSize: %d", len(input))
				assert.Len(t, input, 10)
			case 1:
				t.Logf("Timeout: %d", len(input))
				assert.Len(t, input, 5)
				go func() {
					for i := 0; i < 3; i++ {
						impl.outbox <- envelope{body: fmt.Sprintf("m_%d", i), status: make(chan error)}
					}
					<-time.NewTimer(time.Millisecond * 500).C
					impl.stop <- done
				}()
			case 2:
				t.Logf("Stop: %d", len(input))
				assert.Len(t, input, 3)
			default:
				assert.Equal(t, 1, c, "publish called more than 3 times: %d", c)
				return
			}
			c++
		}
		go impl.Start()
		for i := 0; i < 15; i++ {
			impl.outbox <- envelope{body: fmt.Sprintf("m_%d", i), status: make(chan error)}
		}
		<-done
	})
}

func TestPubImpl_Publish(t *testing.T) {
	t.Run("ErrInvalidMessage", func(t *testing.T) {
		svc := &mockService{}
		p, _ := NewPublisher("QUEUE_NAME", svc, &PublisherConfig{BatchWindow: time.Second * 100})
		impl := p.(*publisher)
		err := impl.Publish(nil)
		if assert.Error(t, err) {
			assert.Equal(t, ErrInvalidMessage, err)
		}
	})

	t.Run("OK", func(t *testing.T) {
		svc := &mockService{}
		p, _ := NewPublisher("QUEUE_NAME", svc, &PublisherConfig{BatchWindow: time.Second * 100})
		impl := p.(*publisher)
		done := make(chan bool)
		go func() {
			err := impl.Publish("test", &MessageConfig{Delay: time.Second})
			assert.NoError(t, err)
			done <- true
		}()
		e := <-impl.outbox
		assert.NotEmpty(t, e.id)
		if assert.NotNil(t, e.body) {
			assert.NotNil(t, e.body)
		}
		assert.NotNil(t, e.status)
		assert.Equal(t, e.delay, time.Second)
		e.status <- nil
		<-done
	})

	t.Run("Error", func(t *testing.T) {
		svc := &mockService{}
		p, _ := NewPublisher("QUEUE_NAME", svc, &PublisherConfig{BatchWindow: time.Second * 100})
		impl := p.(*publisher)
		done := make(chan bool)
		go func() {
			err := impl.Publish("test", &MessageConfig{Delay: time.Second})
			assert.Error(t, err)
			done <- true
		}()
		e := <-impl.outbox
		e.status <- errors.New("could not publish")
		<-done
	})

	t.Run("NilConfig", func(t *testing.T) {
		svc := &mockService{}
		p, _ := NewPublisher("QUEUE_NAME", svc, &PublisherConfig{BatchWindow: time.Second * 100})
		impl := p.(*publisher)
		done := make(chan bool)
		go func() {
			err := impl.Publish("test", nil)
			assert.NoError(t, err)
			done <- true
		}()
		e := <-impl.outbox
		e.status <- nil
		<-done
	})

}

func TestPubImpl_publishBatch(t *testing.T) {
	t.Run("SuccessFailure", func(t *testing.T) {
		svc := &mockService{}
		svc.sendMessageBatch = func(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
			return &sqs.SendMessageBatchOutput{
				Failed: []*sqs.BatchResultErrorEntry{
					{
						Code:    aws.String("CODE"),
						Message: aws.String("ERROR_MESSAGE"),
						Id:      aws.String("cb561661-1848-4512-91e8-68d2d3bee943"),
					},
				},
				Successful: []*sqs.SendMessageBatchResultEntry{
					{
						Id: aws.String("74863f40-a07a-475d-bbdc-9ea4f7404adf"),
					},
					{
						Id: aws.String("51759766-5328-4acf-beac-5da1cebab966"),
					},
				},
			}, nil
		}
		p, _ := NewPublisher("QUEUE_NAME", svc)
		ee := []*envelope{
			{
				id:     "cb561661-1848-4512-91e8-68d2d3bee943",
				status: make(chan error),
				body:   "m1",
			},
			{
				id:     "74863f40-a07a-475d-bbdc-9ea4f7404adf",
				status: make(chan error),
				body:   "m2",
			},
			{
				id:     "51759766-5328-4acf-beac-5da1cebab966",
				status: make(chan error),
				body:   "m3",
			},
		}
		go p.(*publisher).publishBatch(ee)
		c := 0
		deadline := time.NewTicker(time.Millisecond * 100)
		for {
			select {
			case err := <-ee[0].status:
				assert.Error(t, err)
				c++
			case err := <-ee[1].status:
				assert.NoError(t, err)
				c++
			case err := <-ee[2].status:
				assert.NoError(t, err)
				c++
			case <-deadline.C:
				assert.Equal(t, 3, c)
				return
			}
		}
	})
	t.Run("SendMessageBatchError", func(t *testing.T) {
		svc := &mockService{}
		svc.sendMessageBatch = func(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
			return nil, errors.New("error")
		}
		p, _ := NewPublisher("QUEUE_NAME", svc)
		ee := []*envelope{
			{
				id:     "cb561661-1848-4512-91e8-68d2d3bee943",
				status: make(chan error),
				body:   "m1",
			},
			{
				id:     "74863f40-a07a-475d-bbdc-9ea4f7404adf",
				status: make(chan error),
				body:   "m2",
			},
			{
				id:     "51759766-5328-4acf-beac-5da1cebab966",
				status: make(chan error),
				body:   "m3",
			},
		}
		go p.(*publisher).publishBatch(ee)
		c := 0
		deadline := time.NewTicker(time.Millisecond * 100)
		for {
			select {
			case err := <-ee[0].status:
				assert.Error(t, err)
				c++
			case err := <-ee[1].status:
				assert.Error(t, err)
				c++
			case err := <-ee[2].status:
				assert.Error(t, err)
				c++
			case <-deadline.C:
				assert.Equal(t, 3, c)
				return
			}
		}
	})
	t.Run("JsonError", func(t *testing.T) {
		svc := &mockService{}
		svc.sendMessageBatch = func(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
			t.Error("SendMessageBatch called. It should not be called for this test.")
			return nil, nil
		}
		p, _ := NewPublisher("QUEUE_NAME", svc)
		ee := []*envelope{
			{
				id:     "cb561661-1848-4512-91e8-68d2d3bee943",
				status: make(chan error),
				body:   "m1",
			},
			{
				id:     "74863f40-a07a-475d-bbdc-9ea4f7404adf",
				status: make(chan error),
				body:   "m2",
			},
			{
				id:     "51759766-5328-4acf-beac-5da1cebab966",
				status: make(chan error),
				body:   "m3",
			},
		}
		impl := p.(*publisher)
		impl.jsonMarshalFn = func(v interface{}) ([]byte, error) {
			return nil, errors.New("json error")
		}
		go impl.publishBatch(ee)
		c := 0
		deadline := time.NewTicker(time.Millisecond * 100)
		for {
			select {
			case err := <-ee[0].status:
				assert.Error(t, err)
				c++
			case err := <-ee[1].status:
				assert.Error(t, err)
				c++
			case err := <-ee[2].status:
				assert.Error(t, err)
				c++
			case <-deadline.C:
				assert.Equal(t, 3, c)
				return
			}
		}
	})
}

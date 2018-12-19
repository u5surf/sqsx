# sqsx
A library that makes consuming and publishing with SQS easier.

```go
type QueueConfig struct {
	Name            string
	AccountID       string
	Region          string
	AccessKeyID     string
	AccessKeySecret string
	Timeout         time.Duration `default:"3m0s"`
}

func sqsConsumerFromConfig(config QueueConfig) (sqsx.Consumer, error) {
	awsConfig := aws.NewConfig().
		WithRegion(config.Region).
		WithCredentials(credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKeySecret, ""))
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *awsConfig}))
	c, err := sqsx.NewConsumer(config.Name, sqs.New(sess), &sqsx.ConsumerConfig{
		MaxWorkers:  1,
		PollTimeout: time.Second * 20,
		Timeout:     config.Timeout,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create consumer for queue: %v", err)
	}
	return c, nil
}

func sqsProducerFromConfig(config QueueConfig) (sqsx.Publisher, error) {
	awsConfig := aws.NewConfig().
		WithRegion(config.Region).
		WithCredentials(credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKeySecret, ""))
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *awsConfig}))
	p, err := sqsx.NewPublisher(config.Name, sqs.New(sess), &sqsx.PublisherConfig{
		BatchWindow: time.Millisecond * 200,
	})
	if err != nil {
		return nil, fmt.Errorf("could not create publisher for queue: %v", err)
	}
	return p, nil
}
```
## Consume a message from SQS
```go
type Job struct {
	// Define fields
}

func performJob(job *Job) error {
	// Perform job here
	return nil
}

type consumeJobHandler struct {
	DeadlineTimeout time.Duration
}

func (j *consumeJobHandler) Error(message *sqs.Message, ok bool, err error) {
	log.Error().Err(err).Bool("success", ok).
		Str("messageID", aws.StringValue(message.MessageId)).
		Interface("message", message).Msg("Error processing message")
}

func (j *consumeJobHandler) Handle(message *sqs.Message, deadline sqsx.ExtendTimeout) error {
	done := make(chan error)
	go func(resp chan<- error) {
		var job Job
		err := json.Unmarshal([]byte(aws.StringValue(message.Body)), &job)
		if err != nil {
			resp <- fmt.Errorf("could not unmarshal message body:\n%v", err)
		}
		resp <- performJob(&job)
	}(done)

	// Extend sqs timeout for the message.
	t := time.NewTicker(j.DeadlineTimeout - (time.Second * 10))
	for {
		select {
		case <-t.C:
			log.Info().Str("messageID", aws.StringValue(message.MessageId)).Msg("Extending timeout.")
			if err := deadline(message, j.DeadlineTimeout ); err != nil {
				log.Error().Err(err).
					Str("messageID", aws.StringValue(message.MessageId)).
					Msg("could not extend message timeout")
			}
		case err := <-done:
			return err
		}
	}
}
```

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

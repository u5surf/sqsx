package sqsx

import (
	"time"
)

type PublisherConfig struct {
	// Publish compatible configuration fields.
	BatchWindow time.Duration
}

type ConsumerConfig struct {
	MaxWorkers int

	BatchSize int

	PollTimeout time.Duration
}

type MessageConfig struct {
	// Service delay time for message in seconds.
	Delay time.Duration
}

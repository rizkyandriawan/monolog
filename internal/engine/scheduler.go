package engine

import (
	"log"
	"time"

	"github.com/rizkyandriawan/monolog/internal/config"
)

// FetchScheduler processes pending fetch requests on a timer
type FetchScheduler struct {
	engine   *Engine
	ticker   *time.Ticker
	interval time.Duration
	stopChan chan struct{}
}

// NewFetchScheduler creates a new FetchScheduler
func NewFetchScheduler(engine *Engine, interval time.Duration) *FetchScheduler {
	return &FetchScheduler{
		engine:   engine,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

// Start starts the scheduler
func (s *FetchScheduler) Start() {
	s.ticker = time.NewTicker(s.interval)
	go s.loop()
}

// Stop stops the scheduler
func (s *FetchScheduler) Stop() {
	if s.ticker != nil {
		s.ticker.Stop()
	}
	close(s.stopChan)
}

func (s *FetchScheduler) loop() {
	for {
		select {
		case <-s.ticker.C:
			s.process()
		case <-s.stopChan:
			return
		}
	}
}

func (s *FetchScheduler) process() {
	queue := s.engine.GetPendingQueue()
	topicStore := s.engine.GetTopicStore()

	completed := queue.Process(topicStore)
	if len(completed) > 0 && s.engine.config.Logging.Level == "debug" {
		log.Printf("[scheduler] processed %d pending fetch requests", len(completed))
	}
}

// RetentionScheduler cleans up old messages on a timer
type RetentionScheduler struct {
	engine   *Engine
	ticker   *time.Ticker
	config   config.RetentionConfig
	stopChan chan struct{}
}

// NewRetentionScheduler creates a new RetentionScheduler
func NewRetentionScheduler(engine *Engine, cfg config.RetentionConfig) *RetentionScheduler {
	return &RetentionScheduler{
		engine:   engine,
		config:   cfg,
		stopChan: make(chan struct{}),
	}
}

// Start starts the scheduler
func (s *RetentionScheduler) Start() {
	if !s.config.Enabled {
		return
	}
	s.ticker = time.NewTicker(s.config.CheckInterval)
	go s.loop()
}

// Stop stops the scheduler
func (s *RetentionScheduler) Stop() {
	if s.ticker != nil {
		s.ticker.Stop()
	}
	select {
	case <-s.stopChan:
		// already closed
	default:
		close(s.stopChan)
	}
}

func (s *RetentionScheduler) loop() {
	for {
		select {
		case <-s.ticker.C:
			s.cleanup()
		case <-s.stopChan:
			return
		}
	}
}

func (s *RetentionScheduler) cleanup() {
	cutoff := time.Now().Add(-s.config.MaxAge)
	topicStore := s.engine.GetTopicStore()

	topics := s.engine.ListTopics()
	for _, topic := range topics {
		deleted, err := topicStore.DeleteBefore(topic, cutoff)
		if err != nil {
			log.Printf("[retention] cleanup failed for topic %s: %v", topic, err)
			continue
		}
		if deleted > 0 {
			log.Printf("[retention] deleted %d records from topic %s", deleted, topic)
		}
	}
}

// MemberExpirationScheduler cleans up expired consumer group members
type MemberExpirationScheduler struct {
	engine   *Engine
	ticker   *time.Ticker
	timeout  time.Duration
	stopChan chan struct{}
}

// NewMemberExpirationScheduler creates a new MemberExpirationScheduler
func NewMemberExpirationScheduler(engine *Engine, timeout time.Duration) *MemberExpirationScheduler {
	return &MemberExpirationScheduler{
		engine:   engine,
		timeout:  timeout,
		stopChan: make(chan struct{}),
	}
}

// Start starts the scheduler
func (s *MemberExpirationScheduler) Start() {
	// Check every 1/3 of the timeout
	interval := s.timeout / 3
	if interval < time.Second {
		interval = time.Second
	}
	s.ticker = time.NewTicker(interval)
	go s.loop()
}

// Stop stops the scheduler
func (s *MemberExpirationScheduler) Stop() {
	if s.ticker != nil {
		s.ticker.Stop()
	}
	select {
	case <-s.stopChan:
	default:
		close(s.stopChan)
	}
}

func (s *MemberExpirationScheduler) loop() {
	for {
		select {
		case <-s.ticker.C:
			s.expire()
		case <-s.stopChan:
			return
		}
	}
}

func (s *MemberExpirationScheduler) expire() {
	// This would call groupStore.ExpireMembers but we need access to it
	// For now, this is a placeholder - the engine should expose this
}

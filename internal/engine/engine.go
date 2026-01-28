package engine

import (
	"fmt"
	"log"
	"sync"

	"github.com/rizkyandriawan/monolog/internal/config"
	"github.com/rizkyandriawan/monolog/internal/store"
)

// Engine is the core business logic layer
type Engine struct {
	config       *config.Config
	topicStore   store.TopicStoreInterface
	groupStore   store.GroupStoreInterface
	pending      *PendingQueue
	fetchSched   *FetchScheduler
	retentionSched *RetentionScheduler
	stopChan     chan struct{}
	wg           sync.WaitGroup
}

// New creates a new Engine
func New(cfg *config.Config, topicStore store.TopicStoreInterface, groupStore store.GroupStoreInterface) *Engine {
	e := &Engine{
		config:     cfg,
		topicStore: topicStore,
		groupStore: groupStore,
		pending:    NewPendingQueue(),
		stopChan:   make(chan struct{}),
	}
	e.fetchSched = NewFetchScheduler(e, cfg.Scheduler.TickInterval)
	e.retentionSched = NewRetentionScheduler(e, cfg.Retention)
	return e
}

// Start starts the engine's background tasks
func (e *Engine) Start() {
	e.fetchSched.Start()
	if e.config.Retention.Enabled {
		e.retentionSched.Start()
	}
}

// Stop stops the engine
func (e *Engine) Stop() {
	close(e.stopChan)
	e.fetchSched.Stop()
	e.retentionSched.Stop()
	e.wg.Wait()
}

// --- Topic Operations ---

// CreateTopic creates a new topic
func (e *Engine) CreateTopic(name string) error {
	if e.topicStore.TopicExists(name) {
		return fmt.Errorf("topic already exists: %s", name)
	}
	return e.topicStore.CreateTopic(name)
}

// EnsureTopic ensures a topic exists, creating it if auto-create is enabled
func (e *Engine) EnsureTopic(name string) error {
	if e.topicStore.TopicExists(name) {
		return nil
	}
	if !e.config.Topics.AutoCreate {
		return fmt.Errorf("topic not found: %s", name)
	}
	return e.topicStore.CreateTopic(name)
}

// ListTopics returns all topic names
func (e *Engine) ListTopics() []string {
	return e.topicStore.ListTopics()
}

// DeleteTopic deletes a topic
func (e *Engine) DeleteTopic(name string) error {
	return e.topicStore.DeleteTopic(name)
}

// GetTopicMeta returns topic metadata
func (e *Engine) GetTopicMeta(name string) (*store.TopicMeta, error) {
	return e.topicStore.GetMeta(name)
}

// TopicExists checks if a topic exists
func (e *Engine) TopicExists(name string) bool {
	return e.topicStore.TopicExists(name)
}

// --- Message Operations ---

// Produce appends records to a topic
func (e *Engine) Produce(topic string, records []store.Record) (int64, error) {
	// Ensure topic exists
	if err := e.EnsureTopic(topic); err != nil {
		return 0, err
	}
	return e.topicStore.Append(topic, records)
}

// ProduceRaw appends raw record batch data (passthrough for compression)
func (e *Engine) ProduceRaw(topic string, data []byte, codec int8, recordCount int) (int64, error) {
	// Ensure topic exists
	if err := e.EnsureTopic(topic); err != nil {
		return 0, err
	}
	return e.topicStore.AppendRaw(topic, data, codec, recordCount)
}

// Fetch reads records from a topic
func (e *Engine) Fetch(topic string, offset int64, maxRecords int) ([]store.Record, error) {
	if !e.topicStore.TopicExists(topic) {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}
	return e.topicStore.Read(topic, offset, maxRecords)
}

// LatestOffset returns the latest offset for a topic
func (e *Engine) LatestOffset(topic string) (int64, error) {
	return e.topicStore.LatestOffset(topic)
}

// EarliestOffset returns the earliest offset for a topic
func (e *Engine) EarliestOffset(topic string) (int64, error) {
	return e.topicStore.EarliestOffset(topic)
}

// --- Pending Fetch Operations ---

// ParkFetch parks a fetch request for later processing
func (e *Engine) ParkFetch(req *PendingFetch) {
	e.pending.Add(req)
}

// GetPendingQueue returns the pending queue
func (e *Engine) GetPendingQueue() *PendingQueue {
	return e.pending
}

// GetTopicStore returns the topic store
func (e *Engine) GetTopicStore() store.TopicStoreInterface {
	return e.topicStore
}

// --- Consumer Group Operations ---

// GetOrCreateGroup gets or creates a consumer group
func (e *Engine) GetOrCreateGroup(groupID string) (*store.Group, error) {
	return e.groupStore.GetOrCreateGroup(groupID)
}

// GetGroup gets a consumer group
func (e *Engine) GetGroup(groupID string) (*store.Group, bool) {
	return e.groupStore.GetGroup(groupID)
}

// ListGroups returns all group IDs
func (e *Engine) ListGroups() []string {
	return e.groupStore.ListGroups()
}

// JoinGroup handles a consumer joining a group
func (e *Engine) JoinGroup(groupID, memberID, clientID string, metadata []byte) (*store.Group, error) {
	group, err := e.groupStore.GetOrCreateGroup(groupID)
	if err != nil {
		return nil, err
	}

	if err := e.groupStore.AddMember(groupID, memberID, clientID, metadata); err != nil {
		return nil, err
	}

	// Refresh group
	group, _ = e.groupStore.GetGroup(groupID)
	return group, nil
}

// SyncGroup handles group sync
func (e *Engine) SyncGroup(groupID, memberID string, assignment []byte) error {
	return e.groupStore.SetMemberAssignment(groupID, memberID, assignment)
}

// Heartbeat updates member heartbeat
func (e *Engine) Heartbeat(groupID, memberID string) error {
	return e.groupStore.UpdateHeartbeat(groupID, memberID)
}

// LeaveGroup handles a consumer leaving a group
func (e *Engine) LeaveGroup(groupID, memberID string) error {
	return e.groupStore.RemoveMember(groupID, memberID)
}

// CommitOffset commits an offset
func (e *Engine) CommitOffset(groupID, topic string, offset int64) error {
	return e.groupStore.CommitOffset(groupID, topic, offset)
}

// FetchOffset fetches the committed offset
func (e *Engine) FetchOffset(groupID, topic string) (int64, error) {
	return e.groupStore.FetchOffset(groupID, topic)
}

// IncrementGeneration increments group generation
func (e *Engine) IncrementGeneration(groupID string) (int32, error) {
	return e.groupStore.IncrementGeneration(groupID)
}

// DeleteGroup deletes a consumer group
func (e *Engine) DeleteGroup(groupID string) error {
	return e.groupStore.DeleteGroup(groupID)
}

// GetConfig returns the config
func (e *Engine) GetConfig() *config.Config {
	return e.config
}

// Log helper
func (e *Engine) log(format string, args ...interface{}) {
	if e.config.Logging.Level == "debug" {
		log.Printf("[engine] "+format, args...)
	}
}

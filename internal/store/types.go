package store

import "time"

// TopicMeta contains topic metadata
type TopicMeta struct {
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
	LatestOffset int64     `json:"latest_offset"`
}

// Record represents a stored message
type Record struct {
	Offset     int64             `json:"offset"`
	LastOffset int64             `json:"last_offset"` // for batches, the last offset in the batch
	Timestamp  int64             `json:"timestamp"`
	Key        []byte            `json:"key,omitempty"`
	Value      []byte            `json:"value"`
	Headers    map[string][]byte `json:"headers,omitempty"`
	Codec      int8              `json:"codec"` // compression codec (passthrough)
}

// Group represents a consumer group
type Group struct {
	ID          string            `json:"id"`
	State       string            `json:"state"` // empty, forming, stable
	Generation  int32             `json:"generation"`
	LeaderID    string            `json:"leader_id"`
	Protocol    string            `json:"protocol"`
	Members     map[string]Member `json:"members"`
	Offsets     map[string]int64  `json:"offsets"` // topic -> offset
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// Member represents a consumer group member
type Member struct {
	ID            string    `json:"id"`
	ClientID      string    `json:"client_id"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Metadata      []byte    `json:"metadata,omitempty"`
	Assignment    []byte    `json:"assignment,omitempty"`
}

// TopicStoreInterface defines topic store operations
type TopicStoreInterface interface {
	CreateTopic(name string) error
	TopicExists(name string) bool
	ListTopics() []string
	DeleteTopic(name string) error
	Append(topic string, records []Record) (int64, error)
	AppendRaw(topic string, data []byte, codec int8, recordCount int) (int64, error)
	Read(topic string, fromOffset int64, maxRecords int) ([]Record, error)
	LatestOffset(topic string) (int64, error)
	EarliestOffset(topic string) (int64, error)
	DeleteBefore(topic string, cutoff time.Time) (int, error)
	GetMeta(topic string) (*TopicMeta, error)
}

// GroupStoreInterface defines group store operations
type GroupStoreInterface interface {
	GetOrCreateGroup(groupID string) (*Group, error)
	GetGroup(groupID string) (*Group, bool)
	ListGroups() []string
	AddMember(groupID, memberID, clientID string, metadata []byte) error
	RemoveMember(groupID, memberID string) error
	UpdateHeartbeat(groupID, memberID string) error
	SetMemberAssignment(groupID, memberID string, assignment []byte) error
	IncrementGeneration(groupID string) (int32, error)
	CommitOffset(groupID, topic string, offset int64) error
	FetchOffset(groupID, topic string) (int64, error)
	ExpireMembers(timeout time.Duration) ([]string, error)
	DeleteGroup(groupID string) error
}

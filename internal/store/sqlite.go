package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteDB wraps SQLite database
type SQLiteDB struct {
	db *sql.DB
}

// OpenSQLite opens or creates a SQLite database
func OpenSQLite(dataDir string) (*SQLiteDB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	dbPath := filepath.Join(dataDir, "monolog.db")
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}

	// Set connection pool settings
	db.SetMaxOpenConns(1) // SQLite works best with single writer
	db.SetMaxIdleConns(1)

	s := &SQLiteDB{db: db}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, err
	}

	return s, nil
}

func (s *SQLiteDB) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS topics (
		name TEXT PRIMARY KEY,
		created_at INTEGER NOT NULL,
		latest_offset INTEGER NOT NULL DEFAULT -1
	);

	CREATE TABLE IF NOT EXISTS messages (
		topic TEXT NOT NULL,
		offset INTEGER NOT NULL,
		last_offset INTEGER NOT NULL,
		timestamp INTEGER NOT NULL,
		key BLOB,
		value BLOB,
		codec INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (topic, offset)
	);
	CREATE INDEX IF NOT EXISTS idx_messages_topic_ts ON messages(topic, timestamp);

	CREATE TABLE IF NOT EXISTS groups (
		id TEXT PRIMARY KEY,
		state TEXT NOT NULL DEFAULT 'empty',
		generation INTEGER NOT NULL DEFAULT 0,
		leader_id TEXT,
		protocol TEXT,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL
	);

	CREATE TABLE IF NOT EXISTS group_members (
		group_id TEXT NOT NULL,
		member_id TEXT NOT NULL,
		client_id TEXT,
		last_heartbeat INTEGER NOT NULL,
		metadata BLOB,
		assignment BLOB,
		PRIMARY KEY (group_id, member_id),
		FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS group_offsets (
		group_id TEXT NOT NULL,
		topic TEXT NOT NULL,
		committed_offset INTEGER NOT NULL,
		PRIMARY KEY (group_id, topic),
		FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
	);
	`
	_, err := s.db.Exec(schema)
	return err
}

func (s *SQLiteDB) Close() error {
	return s.db.Close()
}

func (s *SQLiteDB) DB() *sql.DB {
	return s.db
}

// ============================================================================
// SQLiteTopicStore
// ============================================================================

type SQLiteTopicStore struct {
	db     *SQLiteDB
	mu     sync.RWMutex
	topics map[string]*TopicMeta // in-memory cache
}

func NewSQLiteTopicStore(db *SQLiteDB) *SQLiteTopicStore {
	ts := &SQLiteTopicStore{
		db:     db,
		topics: make(map[string]*TopicMeta),
	}
	ts.loadTopics()
	return ts
}

func (s *SQLiteTopicStore) loadTopics() {
	rows, err := s.db.DB().Query("SELECT name, created_at, latest_offset FROM topics")
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var createdAtMs, latestOffset int64
		if err := rows.Scan(&name, &createdAtMs, &latestOffset); err != nil {
			continue
		}
		s.topics[name] = &TopicMeta{
			Name:         name,
			CreatedAt:    time.UnixMilli(createdAtMs),
			LatestOffset: latestOffset,
		}
	}
}

func (s *SQLiteTopicStore) CreateTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; exists {
		return fmt.Errorf("topic already exists: %s", name)
	}

	now := time.Now()
	_, err := s.db.DB().Exec(
		"INSERT INTO topics (name, created_at, latest_offset) VALUES (?, ?, ?)",
		name, now.UnixMilli(), -1,
	)
	if err != nil {
		return err
	}

	s.topics[name] = &TopicMeta{
		Name:         name,
		CreatedAt:    now,
		LatestOffset: -1,
	}
	return nil
}

func (s *SQLiteTopicStore) TopicExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.topics[name]
	return exists
}

func (s *SQLiteTopicStore) ListTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.topics))
	for name := range s.topics {
		names = append(names, name)
	}
	return names
}

func (s *SQLiteTopicStore) DeleteTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; !exists {
		return fmt.Errorf("topic not found: %s", name)
	}

	tx, err := s.db.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("DELETE FROM messages WHERE topic = ?", name); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM topics WHERE name = ?", name); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	delete(s.topics, name)
	return nil
}

func (s *SQLiteTopicStore) Append(topic string, records []Record) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, exists := s.topics[topic]
	if !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	baseOffset := meta.LatestOffset + 1

	tx, err := s.db.DB().Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO messages (topic, offset, last_offset, timestamp, key, value, codec) VALUES (?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for i, rec := range records {
		offset := baseOffset + int64(i)
		ts := rec.Timestamp
		if ts == 0 {
			ts = time.Now().UnixMilli()
		}
		lastOffset := offset
		if rec.LastOffset > 0 {
			lastOffset = rec.LastOffset
		}

		_, err := stmt.Exec(topic, offset, lastOffset, ts, rec.Key, rec.Value, rec.Codec)
		if err != nil {
			return 0, err
		}
	}

	newLatest := baseOffset + int64(len(records)) - 1
	_, err = tx.Exec("UPDATE topics SET latest_offset = ? WHERE name = ?", newLatest, topic)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	meta.LatestOffset = newLatest
	return baseOffset, nil
}

func (s *SQLiteTopicStore) AppendRaw(topic string, data []byte, codec int8, recordCount int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, exists := s.topics[topic]
	if !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	baseOffset := meta.LatestOffset + 1
	lastOffset := baseOffset + int64(recordCount) - 1
	ts := time.Now().UnixMilli()

	tx, err := s.db.DB().Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	_, err = tx.Exec(
		"INSERT INTO messages (topic, offset, last_offset, timestamp, key, value, codec) VALUES (?, ?, ?, ?, NULL, ?, ?)",
		topic, baseOffset, lastOffset, ts, data, codec,
	)
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec("UPDATE topics SET latest_offset = ? WHERE name = ?", lastOffset, topic)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	meta.LatestOffset = lastOffset
	return baseOffset, nil
}

func (s *SQLiteTopicStore) Read(topic string, fromOffset int64, maxRecords int) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.topics[topic]; !exists {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	rows, err := s.db.DB().Query(
		`SELECT offset, last_offset, timestamp, key, value, codec
		 FROM messages
		 WHERE topic = ? AND last_offset >= ?
		 ORDER BY offset ASC
		 LIMIT ?`,
		topic, fromOffset, maxRecords,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var rec Record
		var key, value []byte
		if err := rows.Scan(&rec.Offset, &rec.LastOffset, &rec.Timestamp, &key, &value, &rec.Codec); err != nil {
			continue
		}
		rec.Key = key
		rec.Value = value
		records = append(records, rec)
	}

	return records, nil
}

func (s *SQLiteTopicStore) LatestOffset(topic string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, exists := s.topics[topic]
	if !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}
	return meta.LatestOffset, nil
}

func (s *SQLiteTopicStore) EarliestOffset(topic string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.topics[topic]; !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	var earliest sql.NullInt64
	err := s.db.DB().QueryRow(
		"SELECT MIN(offset) FROM messages WHERE topic = ?",
		topic,
	).Scan(&earliest)
	if err != nil || !earliest.Valid {
		return 0, nil
	}
	return earliest.Int64, nil
}

func (s *SQLiteTopicStore) DeleteBefore(topic string, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[topic]; !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	result, err := s.db.DB().Exec(
		"DELETE FROM messages WHERE topic = ? AND timestamp < ?",
		topic, cutoff.UnixMilli(),
	)
	if err != nil {
		return 0, err
	}

	affected, _ := result.RowsAffected()
	return int(affected), nil
}

func (s *SQLiteTopicStore) GetMeta(topic string) (*TopicMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, exists := s.topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}
	return meta, nil
}

// ============================================================================
// SQLiteGroupStore
// ============================================================================

type SQLiteGroupStore struct {
	db     *SQLiteDB
	mu     sync.RWMutex
	groups map[string]*Group // in-memory cache
}

func NewSQLiteGroupStore(db *SQLiteDB) *SQLiteGroupStore {
	gs := &SQLiteGroupStore{
		db:     db,
		groups: make(map[string]*Group),
	}
	gs.loadGroups()
	return gs
}

func (s *SQLiteGroupStore) loadGroups() {
	rows, err := s.db.DB().Query("SELECT id, state, generation, leader_id, protocol, created_at, updated_at FROM groups")
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var g Group
		var leaderID, protocol sql.NullString
		var createdAt, updatedAt int64
		if err := rows.Scan(&g.ID, &g.State, &g.Generation, &leaderID, &protocol, &createdAt, &updatedAt); err != nil {
			continue
		}
		g.LeaderID = leaderID.String
		g.Protocol = protocol.String
		g.CreatedAt = time.UnixMilli(createdAt)
		g.UpdatedAt = time.UnixMilli(updatedAt)
		g.Members = make(map[string]Member)
		g.Offsets = make(map[string]int64)
		s.groups[g.ID] = &g
	}

	// Load members
	for groupID, group := range s.groups {
		s.loadMembers(groupID, group)
		s.loadOffsets(groupID, group)
	}
}

func (s *SQLiteGroupStore) loadMembers(groupID string, group *Group) {
	rows, err := s.db.DB().Query(
		"SELECT member_id, client_id, last_heartbeat, metadata, assignment FROM group_members WHERE group_id = ?",
		groupID,
	)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var m Member
		var clientID sql.NullString
		var lastHB int64
		var metadata, assignment []byte
		if err := rows.Scan(&m.ID, &clientID, &lastHB, &metadata, &assignment); err != nil {
			continue
		}
		m.ClientID = clientID.String
		m.LastHeartbeat = time.UnixMilli(lastHB)
		m.Metadata = metadata
		m.Assignment = assignment
		group.Members[m.ID] = m
	}
}

func (s *SQLiteGroupStore) loadOffsets(groupID string, group *Group) {
	rows, err := s.db.DB().Query(
		"SELECT topic, committed_offset FROM group_offsets WHERE group_id = ?",
		groupID,
	)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var topic string
		var offset int64
		if err := rows.Scan(&topic, &offset); err != nil {
			continue
		}
		group.Offsets[topic] = offset
	}
}

func (s *SQLiteGroupStore) GetOrCreateGroup(groupID string) (*Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if group, exists := s.groups[groupID]; exists {
		return group, nil
	}

	now := time.Now()
	_, err := s.db.DB().Exec(
		"INSERT INTO groups (id, state, generation, created_at, updated_at) VALUES (?, 'empty', 0, ?, ?)",
		groupID, now.UnixMilli(), now.UnixMilli(),
	)
	if err != nil {
		return nil, err
	}

	group := &Group{
		ID:        groupID,
		State:     "empty",
		Members:   make(map[string]Member),
		Offsets:   make(map[string]int64),
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.groups[groupID] = group
	return group, nil
}

func (s *SQLiteGroupStore) GetGroup(groupID string) (*Group, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	group, exists := s.groups[groupID]
	return group, exists
}

func (s *SQLiteGroupStore) ListGroups() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.groups))
	for id := range s.groups {
		ids = append(ids, id)
	}
	return ids
}

func (s *SQLiteGroupStore) AddMember(groupID, memberID, clientID string, metadata []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	now := time.Now()
	_, err := s.db.DB().Exec(
		`INSERT OR REPLACE INTO group_members (group_id, member_id, client_id, last_heartbeat, metadata)
		 VALUES (?, ?, ?, ?, ?)`,
		groupID, memberID, clientID, now.UnixMilli(), metadata,
	)
	if err != nil {
		return err
	}

	group.Members[memberID] = Member{
		ID:            memberID,
		ClientID:      clientID,
		LastHeartbeat: now,
		Metadata:      metadata,
	}

	if len(group.Members) == 1 {
		group.LeaderID = memberID
	}
	if group.State == "empty" {
		group.State = "forming"
	}
	group.UpdatedAt = now

	s.updateGroupMeta(group)
	return nil
}

func (s *SQLiteGroupStore) RemoveMember(groupID, memberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	_, err := s.db.DB().Exec(
		"DELETE FROM group_members WHERE group_id = ? AND member_id = ?",
		groupID, memberID,
	)
	if err != nil {
		return err
	}

	delete(group.Members, memberID)
	group.UpdatedAt = time.Now()

	if group.LeaderID == memberID && len(group.Members) > 0 {
		for id := range group.Members {
			group.LeaderID = id
			break
		}
	}

	if len(group.Members) == 0 {
		group.State = "empty"
		group.LeaderID = ""
	}

	s.updateGroupMeta(group)
	return nil
}

func (s *SQLiteGroupStore) UpdateHeartbeat(groupID, memberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	member, exists := group.Members[memberID]
	if !exists {
		return fmt.Errorf("member not found: %s", memberID)
	}

	now := time.Now()
	_, err := s.db.DB().Exec(
		"UPDATE group_members SET last_heartbeat = ? WHERE group_id = ? AND member_id = ?",
		now.UnixMilli(), groupID, memberID,
	)
	if err != nil {
		return err
	}

	member.LastHeartbeat = now
	group.Members[memberID] = member
	group.UpdatedAt = now
	return nil
}

func (s *SQLiteGroupStore) SetMemberAssignment(groupID, memberID string, assignment []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	member, exists := group.Members[memberID]
	if !exists {
		return fmt.Errorf("member not found: %s", memberID)
	}

	_, err := s.db.DB().Exec(
		"UPDATE group_members SET assignment = ? WHERE group_id = ? AND member_id = ?",
		assignment, groupID, memberID,
	)
	if err != nil {
		return err
	}

	member.Assignment = assignment
	group.Members[memberID] = member
	group.UpdatedAt = time.Now()
	return nil
}

func (s *SQLiteGroupStore) IncrementGeneration(groupID string) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return 0, fmt.Errorf("group not found: %s", groupID)
	}

	group.Generation++
	group.State = "stable"
	group.UpdatedAt = time.Now()

	s.updateGroupMeta(group)
	return group.Generation, nil
}

func (s *SQLiteGroupStore) CommitOffset(groupID, topic string, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	_, err := s.db.DB().Exec(
		`INSERT OR REPLACE INTO group_offsets (group_id, topic, committed_offset) VALUES (?, ?, ?)`,
		groupID, topic, offset,
	)
	if err != nil {
		return err
	}

	group.Offsets[topic] = offset
	group.UpdatedAt = time.Now()
	return nil
}

func (s *SQLiteGroupStore) FetchOffset(groupID, topic string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, exists := s.groups[groupID]
	if !exists {
		return -1, fmt.Errorf("group not found: %s", groupID)
	}

	offset, exists := group.Offsets[topic]
	if !exists {
		return -1, nil
	}
	return offset, nil
}

func (s *SQLiteGroupStore) ExpireMembers(timeout time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-timeout)
	var expired []string

	for _, group := range s.groups {
		var toRemove []string
		for memberID, member := range group.Members {
			if member.LastHeartbeat.Before(cutoff) {
				toRemove = append(toRemove, memberID)
			}
		}

		for _, memberID := range toRemove {
			s.db.DB().Exec(
				"DELETE FROM group_members WHERE group_id = ? AND member_id = ?",
				group.ID, memberID,
			)
			delete(group.Members, memberID)
			expired = append(expired, fmt.Sprintf("%s/%s", group.ID, memberID))
		}

		if len(toRemove) > 0 {
			group.UpdatedAt = time.Now()
			if len(group.Members) == 0 {
				group.State = "empty"
				group.LeaderID = ""
			} else if _, exists := group.Members[group.LeaderID]; !exists {
				for id := range group.Members {
					group.LeaderID = id
					break
				}
			}
			s.updateGroupMeta(group)
		}
	}

	return expired, nil
}

func (s *SQLiteGroupStore) DeleteGroup(groupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[groupID]; !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	tx, err := s.db.DB().Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	tx.Exec("DELETE FROM group_offsets WHERE group_id = ?", groupID)
	tx.Exec("DELETE FROM group_members WHERE group_id = ?", groupID)
	tx.Exec("DELETE FROM groups WHERE id = ?", groupID)

	if err := tx.Commit(); err != nil {
		return err
	}

	delete(s.groups, groupID)
	return nil
}

func (s *SQLiteGroupStore) updateGroupMeta(group *Group) {
	s.db.DB().Exec(
		"UPDATE groups SET state = ?, generation = ?, leader_id = ?, protocol = ?, updated_at = ? WHERE id = ?",
		group.State, group.Generation, group.LeaderID, group.Protocol, group.UpdatedAt.UnixMilli(), group.ID,
	)
}

// ============================================================================
// Interface to match existing stores
// ============================================================================

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

// Ensure implementations satisfy interfaces
var _ TopicStoreInterface = (*TopicStore)(nil)
var _ TopicStoreInterface = (*SQLiteTopicStore)(nil)
var _ GroupStoreInterface = (*GroupStore)(nil)
var _ GroupStoreInterface = (*SQLiteGroupStore)(nil)

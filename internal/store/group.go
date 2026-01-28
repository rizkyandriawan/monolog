package store

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// GroupStore handles consumer group storage
type GroupStore struct {
	db     *DB
	mu     sync.RWMutex
	groups map[string]*Group
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

// NewGroupStore creates a new GroupStore
func NewGroupStore(db *DB) *GroupStore {
	gs := &GroupStore{
		db:     db,
		groups: make(map[string]*Group),
	}
	gs.loadGroups()
	return gs
}

// loadGroups loads existing groups from the database
func (s *GroupStore) loadGroups() {
	s.db.Badger().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("groups:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			// Look for meta keys
			if len(key) > 8 && key[len(key)-5:] == ":meta" {
				groupID := key[7 : len(key)-5]
				item.Value(func(val []byte) error {
					var group Group
					if err := json.Unmarshal(val, &group); err == nil {
						s.groups[groupID] = &group
					}
					return nil
				})
			}
		}
		return nil
	})
}

// GetOrCreateGroup gets or creates a consumer group
func (s *GroupStore) GetOrCreateGroup(groupID string) (*Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if group, exists := s.groups[groupID]; exists {
		return group, nil
	}

	group := &Group{
		ID:        groupID,
		State:     "empty",
		Members:   make(map[string]Member),
		Offsets:   make(map[string]int64),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	if err := s.saveGroup(group); err != nil {
		return nil, err
	}

	s.groups[groupID] = group
	return group, nil
}

// GetGroup gets a consumer group
func (s *GroupStore) GetGroup(groupID string) (*Group, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	group, exists := s.groups[groupID]
	return group, exists
}

// ListGroups returns all group IDs
func (s *GroupStore) ListGroups() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]string, 0, len(s.groups))
	for id := range s.groups {
		ids = append(ids, id)
	}
	return ids
}

// AddMember adds a member to a group
func (s *GroupStore) AddMember(groupID, memberID, clientID string, metadata []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	group.Members[memberID] = Member{
		ID:            memberID,
		ClientID:      clientID,
		LastHeartbeat: time.Now(),
		Metadata:      metadata,
	}
	group.UpdatedAt = time.Now()

	// If first member, make them leader
	if len(group.Members) == 1 {
		group.LeaderID = memberID
	}

	// Update state
	if group.State == "empty" {
		group.State = "forming"
	}

	return s.saveGroup(group)
}

// RemoveMember removes a member from a group
func (s *GroupStore) RemoveMember(groupID, memberID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	delete(group.Members, memberID)
	group.UpdatedAt = time.Now()

	// If leader left, elect new leader
	if group.LeaderID == memberID && len(group.Members) > 0 {
		for id := range group.Members {
			group.LeaderID = id
			break
		}
	}

	// Update state
	if len(group.Members) == 0 {
		group.State = "empty"
		group.LeaderID = ""
	}

	return s.saveGroup(group)
}

// UpdateHeartbeat updates a member's heartbeat
func (s *GroupStore) UpdateHeartbeat(groupID, memberID string) error {
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

	member.LastHeartbeat = time.Now()
	group.Members[memberID] = member
	group.UpdatedAt = time.Now()

	return s.saveGroup(group)
}

// SetMemberAssignment sets a member's assignment
func (s *GroupStore) SetMemberAssignment(groupID, memberID string, assignment []byte) error {
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

	member.Assignment = assignment
	group.Members[memberID] = member
	group.UpdatedAt = time.Now()

	return s.saveGroup(group)
}

// IncrementGeneration increments the group generation
func (s *GroupStore) IncrementGeneration(groupID string) (int32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return 0, fmt.Errorf("group not found: %s", groupID)
	}

	group.Generation++
	group.State = "stable"
	group.UpdatedAt = time.Now()

	if err := s.saveGroup(group); err != nil {
		return 0, err
	}

	return group.Generation, nil
}

// CommitOffset commits an offset for a topic
func (s *GroupStore) CommitOffset(groupID, topic string, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupID]
	if !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	group.Offsets[topic] = offset
	group.UpdatedAt = time.Now()

	return s.saveGroup(group)
}

// FetchOffset fetches the committed offset for a topic
func (s *GroupStore) FetchOffset(groupID, topic string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, exists := s.groups[groupID]
	if !exists {
		return -1, fmt.Errorf("group not found: %s", groupID)
	}

	offset, exists := group.Offsets[topic]
	if !exists {
		return -1, nil // No committed offset
	}

	return offset, nil
}

// ExpireMembers removes members that haven't sent a heartbeat within timeout
func (s *GroupStore) ExpireMembers(timeout time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []string
	cutoff := time.Now().Add(-timeout)

	for _, group := range s.groups {
		var toRemove []string
		for memberID, member := range group.Members {
			if member.LastHeartbeat.Before(cutoff) {
				toRemove = append(toRemove, memberID)
			}
		}

		for _, memberID := range toRemove {
			delete(group.Members, memberID)
			expired = append(expired, fmt.Sprintf("%s/%s", group.ID, memberID))
		}

		if len(toRemove) > 0 {
			group.UpdatedAt = time.Now()
			if len(group.Members) == 0 {
				group.State = "empty"
				group.LeaderID = ""
			} else if group.LeaderID != "" {
				// Check if leader was removed
				if _, exists := group.Members[group.LeaderID]; !exists {
					for id := range group.Members {
						group.LeaderID = id
						break
					}
				}
			}
			s.saveGroup(group)
		}
	}

	return expired, nil
}

// DeleteGroup deletes a consumer group
func (s *GroupStore) DeleteGroup(groupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[groupID]; !exists {
		return fmt.Errorf("group not found: %s", groupID)
	}

	metaKey := fmt.Sprintf("groups:%s:meta", groupID)
	err := s.db.Badger().Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(metaKey))
	})
	if err != nil {
		return err
	}

	delete(s.groups, groupID)
	return nil
}

// saveGroup saves group to database
func (s *GroupStore) saveGroup(group *Group) error {
	metaKey := fmt.Sprintf("groups:%s:meta", group.ID)
	metaVal, err := json.Marshal(group)
	if err != nil {
		return err
	}

	return s.db.Badger().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(metaKey), metaVal)
	})
}

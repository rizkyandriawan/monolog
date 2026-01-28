package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// TopicStore handles topic and message storage
type TopicStore struct {
	db     *DB
	mu     sync.RWMutex
	topics map[string]*TopicMeta
}

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

// NewTopicStore creates a new TopicStore
func NewTopicStore(db *DB) *TopicStore {
	ts := &TopicStore{
		db:     db,
		topics: make(map[string]*TopicMeta),
	}
	ts.loadTopics()
	return ts
}

// loadTopics loads existing topic metadata from the database
func (s *TopicStore) loadTopics() {
	s.db.Badger().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("topics:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			// Look for meta keys
			if len(key) > 7 && key[len(key)-5:] == ":meta" {
				topicName := key[7 : len(key)-5]
				item.Value(func(val []byte) error {
					var meta TopicMeta
					if err := json.Unmarshal(val, &meta); err == nil {
						s.topics[topicName] = &meta
					}
					return nil
				})
			}
		}
		return nil
	})
}

// CreateTopic creates a new topic
func (s *TopicStore) CreateTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; exists {
		return fmt.Errorf("topic already exists: %s", name)
	}

	meta := &TopicMeta{
		Name:         name,
		CreatedAt:    time.Now(),
		LatestOffset: -1,
	}

	metaKey := fmt.Sprintf("topics:%s:meta", name)
	metaVal, _ := json.Marshal(meta)

	err := s.db.Badger().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(metaKey), metaVal)
	})
	if err != nil {
		return err
	}

	s.topics[name] = meta
	return nil
}

// TopicExists checks if a topic exists
func (s *TopicStore) TopicExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.topics[name]
	return exists
}

// ListTopics returns all topic names
func (s *TopicStore) ListTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0, len(s.topics))
	for name := range s.topics {
		names = append(names, name)
	}
	return names
}

// DeleteTopic deletes a topic and all its messages
func (s *TopicStore) DeleteTopic(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; !exists {
		return fmt.Errorf("topic not found: %s", name)
	}

	prefix := []byte(fmt.Sprintf("topics:%s:", name))

	err := s.db.Badger().Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	delete(s.topics, name)
	return nil
}

// Append appends records to a topic, returns the base offset
func (s *TopicStore) Append(topic string, records []Record) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, exists := s.topics[topic]
	if !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	baseOffset := meta.LatestOffset + 1

	err := s.db.Badger().Update(func(txn *badger.Txn) error {
		for i, rec := range records {
			offset := baseOffset + int64(i)
			rec.Offset = offset
			if rec.Timestamp == 0 {
				rec.Timestamp = time.Now().UnixMilli()
			}

			key := s.messageKey(topic, rec.Timestamp, offset)
			val, err := json.Marshal(rec)
			if err != nil {
				return err
			}

			if err := txn.Set(key, val); err != nil {
				return err
			}
		}

		// Update metadata
		meta.LatestOffset = baseOffset + int64(len(records)) - 1
		metaKey := fmt.Sprintf("topics:%s:meta", topic)
		metaVal, _ := json.Marshal(meta)
		return txn.Set([]byte(metaKey), metaVal)
	})

	if err != nil {
		return 0, err
	}

	return baseOffset, nil
}

// Read reads records from a topic starting at offset
func (s *TopicStore) Read(topic string, fromOffset int64, maxRecords int) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.topics[topic]; !exists {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	var records []Record

	err := s.db.Badger().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(fmt.Sprintf("topics:%s:msg:", topic))
		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Rewind(); it.Valid() && count < maxRecords; it.Next() {
			item := it.Item()
			var rec Record
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &rec)
			})
			if err != nil {
				continue
			}

			// Check if this batch contains the requested offset
			// A batch at Offset=0 with LastOffset=4 contains offsets 0,1,2,3,4
			// For legacy records without LastOffset set, use Offset as LastOffset
			lastOffset := rec.LastOffset
			if lastOffset == 0 && rec.Offset > 0 {
				lastOffset = rec.Offset // single record without batch info
			} else if lastOffset == 0 && rec.Offset == 0 {
				lastOffset = 0 // might be single record at offset 0
			}

			// Include this record if it contains or is after the requested offset
			if lastOffset >= fromOffset || rec.Offset >= fromOffset {
				records = append(records, rec)
				count++
			}
		}
		return nil
	})

	return records, err
}

// LatestOffset returns the latest offset for a topic
func (s *TopicStore) LatestOffset(topic string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, exists := s.topics[topic]
	if !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}
	return meta.LatestOffset, nil
}

// EarliestOffset returns the earliest offset for a topic
func (s *TopicStore) EarliestOffset(topic string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.topics[topic]; !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	var earliest int64 = -1

	s.db.Badger().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(fmt.Sprintf("topics:%s:msg:", topic))
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if it.Valid() {
			item := it.Item()
			var rec Record
			item.Value(func(val []byte) error {
				return json.Unmarshal(val, &rec)
			})
			earliest = rec.Offset
		}
		return nil
	})

	if earliest < 0 {
		return 0, nil
	}
	return earliest, nil
}

// DeleteBefore deletes all records before the given timestamp
func (s *TopicStore) DeleteBefore(topic string, cutoff time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[topic]; !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	cutoffMs := cutoff.UnixMilli()
	deleted := 0

	err := s.db.Badger().Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(fmt.Sprintf("topics:%s:msg:", topic))
		it := txn.NewIterator(opts)
		defer it.Close()

		var keysToDelete [][]byte

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			// Extract timestamp from key
			ts := s.extractTimestamp(key)
			if ts < cutoffMs {
				keysToDelete = append(keysToDelete, key)
			}
		}

		for _, key := range keysToDelete {
			if err := txn.Delete(key); err != nil {
				return err
			}
			deleted++
		}

		return nil
	})

	return deleted, err
}

// GetMeta returns topic metadata
func (s *TopicStore) GetMeta(topic string) (*TopicMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, exists := s.topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}
	return meta, nil
}

// messageKey creates a key for a message: topics:<topic>:msg:<timestamp>:<offset>
func (s *TopicStore) messageKey(topic string, timestamp, offset int64) []byte {
	// Format: topics:<topic>:msg:<timestamp_20digits>:<offset_20digits>
	key := fmt.Sprintf("topics:%s:msg:%020d:%020d", topic, timestamp, offset)
	return []byte(key)
}

// extractTimestamp extracts timestamp from a message key
func (s *TopicStore) extractTimestamp(key []byte) int64 {
	// Key format: topics:<topic>:msg:<timestamp>:<offset>
	// Find the timestamp portion
	keyStr := string(key)
	var ts int64
	// Simple parse: find ":msg:" then read next 20 digits
	for i := 0; i < len(keyStr)-25; i++ {
		if keyStr[i:i+5] == ":msg:" {
			tsStr := keyStr[i+5 : i+25]
			fmt.Sscanf(tsStr, "%d", &ts)
			break
		}
	}
	return ts
}

// AppendRaw appends raw record batch data (passthrough for compression)
func (s *TopicStore) AppendRaw(topic string, data []byte, codec int8, recordCount int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	meta, exists := s.topics[topic]
	if !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	baseOffset := meta.LatestOffset + 1
	timestamp := time.Now().UnixMilli()

	// Store raw batch as a single record
	lastOffset := baseOffset + int64(recordCount) - 1
	rec := Record{
		Offset:     baseOffset,
		LastOffset: lastOffset,
		Timestamp:  timestamp,
		Value:      data,
		Codec:      codec,
	}

	key := s.messageKey(topic, timestamp, baseOffset)
	val, _ := json.Marshal(rec)

	err := s.db.Badger().Update(func(txn *badger.Txn) error {
		if err := txn.Set(key, val); err != nil {
			return err
		}

		// Update metadata
		meta.LatestOffset = baseOffset + int64(recordCount) - 1
		metaKey := fmt.Sprintf("topics:%s:meta", topic)
		metaVal, _ := json.Marshal(meta)
		return txn.Set([]byte(metaKey), metaVal)
	})

	if err != nil {
		return 0, err
	}

	return baseOffset, nil
}

// Helper to encode int64 to bytes
func int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// Helper to decode bytes to int64
func bytesToInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

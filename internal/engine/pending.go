package engine

import (
	"net"
	"sync"
	"time"

	"github.com/rizkyandriawan/monolog/internal/store"
)

// PendingFetch represents a parked fetch request
type PendingFetch struct {
	Conn          net.Conn
	CorrelationID int32
	Topic         string
	Partition     int32
	Offset        int64
	MaxBytes      int32
	Deadline      time.Time
	ResponseChan  chan FetchResult
}

// FetchResult is the result of a fetch operation
type FetchResult struct {
	Records []store.Record
	Error   error
}

// PendingQueue holds parked fetch requests
type PendingQueue struct {
	mu      sync.Mutex
	pending []*PendingFetch
}

// NewPendingQueue creates a new PendingQueue
func NewPendingQueue() *PendingQueue {
	return &PendingQueue{
		pending: make([]*PendingFetch, 0),
	}
}

// Add adds a fetch request to the queue
func (q *PendingQueue) Add(req *PendingFetch) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, req)
}

// Remove removes fetch requests for a connection
func (q *PendingQueue) Remove(conn net.Conn) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var remaining []*PendingFetch
	for _, p := range q.pending {
		if p.Conn != conn {
			remaining = append(remaining, p)
		} else {
			// Close the response channel
			close(p.ResponseChan)
		}
	}
	q.pending = remaining
}

// Len returns the number of pending requests
func (q *PendingQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// GetAll returns all pending requests (for inspection)
func (q *PendingQueue) GetAll() []*PendingFetch {
	q.mu.Lock()
	defer q.mu.Unlock()

	result := make([]*PendingFetch, len(q.pending))
	copy(result, q.pending)
	return result
}

// Process processes all pending requests against the topic store
// Returns the requests that were completed (either with data or timeout)
func (q *PendingQueue) Process(topicStore store.TopicStoreInterface) []*PendingFetch {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	var stillPending []*PendingFetch
	var completed []*PendingFetch

	for _, p := range q.pending {
		// Check timeout
		if now.After(p.Deadline) {
			p.ResponseChan <- FetchResult{Records: nil, Error: nil}
			completed = append(completed, p)
			continue
		}

		// Check for data
		records, err := topicStore.Read(p.Topic, p.Offset, int(p.MaxBytes/1024)) // rough estimate
		if err != nil {
			p.ResponseChan <- FetchResult{Records: nil, Error: err}
			completed = append(completed, p)
			continue
		}

		if len(records) > 0 {
			p.ResponseChan <- FetchResult{Records: records, Error: nil}
			completed = append(completed, p)
			continue
		}

		// Keep waiting
		stillPending = append(stillPending, p)
	}

	q.pending = stillPending
	return completed
}

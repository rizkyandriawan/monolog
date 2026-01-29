package server

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/rizkyandriawan/monolog/internal/config"
	"github.com/rizkyandriawan/monolog/internal/engine"
	"github.com/rizkyandriawan/monolog/internal/store"
	"github.com/rizkyandriawan/monolog/web"
)

// HTTPServer handles HTTP API and Web UI
type HTTPServer struct {
	config *config.Config
	engine *engine.Engine
	server *http.Server
}

// NewHTTPServer creates a new HTTPServer
func NewHTTPServer(cfg *config.Config, eng *engine.Engine) *HTTPServer {
	s := &HTTPServer{
		config: cfg,
		engine: eng,
	}

	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/api/topics", s.authMiddleware(s.handleTopics))
	mux.HandleFunc("/api/topics/", s.authMiddleware(s.handleTopic))
	mux.HandleFunc("/api/groups", s.authMiddleware(s.handleGroups))
	mux.HandleFunc("/api/groups/", s.authMiddleware(s.handleGroup))
	mux.HandleFunc("/api/pending", s.authMiddleware(s.handlePending))
	mux.HandleFunc("/api/stats", s.authMiddleware(s.handleStats))

	// Health check (no auth)
	mux.HandleFunc("/health", s.handleHealth)

	// Static files (Web UI) - TODO: embed
	mux.HandleFunc("/", s.handleStatic)

	s.server = &http.Server{
		Addr:    cfg.Server.HTTPAddr,
		Handler: mux,
	}

	return s
}

// ListenAndServe starts the HTTP server
func (s *HTTPServer) ListenAndServe() error {
	return s.server.ListenAndServe()
}

// Close closes the HTTP server
func (s *HTTPServer) Close() error {
	return s.server.Close()
}

func (s *HTTPServer) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.config.Security.Enabled {
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, "Bearer ") {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			token := strings.TrimPrefix(auth, "Bearer ")
			if token != s.config.Security.Token {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *HTTPServer) handleTopics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		topics := s.engine.ListTopics()
		result := make([]map[string]interface{}, 0)
		for _, name := range topics {
			meta, _ := s.engine.GetTopicMeta(name)
			latest, _ := s.engine.LatestOffset(name)
			result = append(result, map[string]interface{}{
				"name":          name,
				"latest_offset": latest,
				"created_at":    meta.CreatedAt,
			})
		}
		json.NewEncoder(w).Encode(result)

	case http.MethodPost:
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.engine.CreateTopic(req.Name); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"name": req.Name})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) handleTopic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Parse path: /api/topics/{name} or /api/topics/{name}/messages
	path := strings.TrimPrefix(r.URL.Path, "/api/topics/")
	parts := strings.Split(path, "/")
	topicName := parts[0]

	if len(parts) > 1 && parts[1] == "messages" {
		s.handleMessages(w, r, topicName)
		return
	}

	switch r.Method {
	case http.MethodGet:
		if !s.engine.TopicExists(topicName) {
			http.Error(w, "Topic not found", http.StatusNotFound)
			return
		}
		meta, _ := s.engine.GetTopicMeta(topicName)
		latest, _ := s.engine.LatestOffset(topicName)
		earliest, _ := s.engine.EarliestOffset(topicName)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"name":            topicName,
			"latest_offset":   latest,
			"earliest_offset": earliest,
			"created_at":      meta.CreatedAt,
		})

	case http.MethodDelete:
		if err := s.engine.DeleteTopic(topicName); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) handleMessages(w http.ResponseWriter, r *http.Request, topicName string) {
	switch r.Method {
	case http.MethodGet:
		offset := int64(0)
		limit := 100
		if v := r.URL.Query().Get("offset"); v != "" {
			offset, _ = strconv.ParseInt(v, 10, 64)
		}
		if v := r.URL.Query().Get("limit"); v != "" {
			limit, _ = strconv.Atoi(v)
		}

		records, err := s.engine.Fetch(topicName, offset, limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		result := make([]map[string]interface{}, 0)
		for _, rec := range records {
			// Check if this is a raw Kafka record batch (stored via ProduceRaw)
			// Record batches have magic byte at position 16, should be 2
			if len(rec.Value) >= 61 && rec.Value[16] == 2 {
				// Parse Kafka record batch to extract actual messages
				messages, err := parseRecordBatch(rec.Value)
				if err == nil && len(messages) > 0 {
					for _, msg := range messages {
						result = append(result, map[string]interface{}{
							"offset":    msg.Offset,
							"timestamp": msg.Timestamp,
							"key":       string(msg.Key),
							"value":     string(msg.Value),
							"codec":     rec.Codec,
						})
					}
					continue
				}
			}

			// Fallback: treat as simple record (produced via HTTP API)
			result = append(result, map[string]interface{}{
				"offset":    rec.Offset,
				"timestamp": rec.Timestamp,
				"key":       string(rec.Key),
				"value":     string(rec.Value),
				"codec":     rec.Codec,
			})
		}
		json.NewEncoder(w).Encode(result)

	case http.MethodPost:
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		records := []store.Record{{
			Key:   []byte(req.Key),
			Value: []byte(req.Value),
		}}
		offset, err := s.engine.Produce(topicName, records)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]int64{"offset": offset})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) handleGroups(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		groups := s.engine.ListGroups()
		result := make([]map[string]interface{}, 0)
		for _, id := range groups {
			group, _ := s.engine.GetGroup(id)
			result = append(result, map[string]interface{}{
				"id":         id,
				"state":      group.State,
				"generation": group.Generation,
				"members":    len(group.Members),
			})
		}
		json.NewEncoder(w).Encode(result)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) handleGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Parse path: /api/groups/{id} or /api/groups/{id}/offsets/{topic}
	path := strings.TrimPrefix(r.URL.Path, "/api/groups/")
	parts := strings.Split(path, "/")
	groupID := parts[0]

	if len(parts) > 2 && parts[1] == "offsets" {
		s.handleGroupOffset(w, r, groupID, parts[2])
		return
	}

	switch r.Method {
	case http.MethodGet:
		group, exists := s.engine.GetGroup(groupID)
		if !exists {
			http.Error(w, "Group not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(group)

	case http.MethodDelete:
		if err := s.engine.DeleteGroup(groupID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) handleGroupOffset(w http.ResponseWriter, r *http.Request, groupID, topic string) {
	switch r.Method {
	case http.MethodGet:
		offset, err := s.engine.FetchOffset(groupID, topic)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]int64{"offset": offset})

	case http.MethodPost:
		var req struct {
			Offset int64 `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.engine.CommitOffset(groupID, topic, req.Offset); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]int64{"offset": req.Offset})

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *HTTPServer) handlePending(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	pending := s.engine.GetPendingQueue().GetAll()
	result := make([]map[string]interface{}, 0)
	for _, p := range pending {
		result = append(result, map[string]interface{}{
			"topic":          p.Topic,
			"partition":      p.Partition,
			"offset":         p.Offset,
			"deadline":       p.Deadline,
			"correlation_id": p.CorrelationID,
		})
	}
	json.NewEncoder(w).Encode(result)
}

func (s *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	topics := s.engine.ListTopics()
	groups := s.engine.ListGroups()
	pending := s.engine.GetPendingQueue().Len()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"topics":   len(topics),
		"groups":   len(groups),
		"pending":  pending,
	})
}

// decompress decompresses data based on Kafka codec
// 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd
func decompress(data []byte, codec int8) ([]byte, error) {
	switch codec {
	case 0: // none
		return data, nil
	case 1: // gzip
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case 2: // snappy
		return snappy.Decode(nil, data)
	case 3: // lz4
		r := lz4.NewReader(bytes.NewReader(data))
		return io.ReadAll(r)
	case 4: // zstd
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer decoder.Close()
		return decoder.DecodeAll(data, nil)
	default:
		return data, nil
	}
}

// ParsedMessage represents a message extracted from a Kafka record batch
type ParsedMessage struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// parseRecordBatch parses a Kafka record batch and extracts messages
// Record batch format (v2, magic=2):
// Offset 0:  baseOffset (8 bytes)
// Offset 8:  batchLength (4 bytes)
// Offset 12: partitionLeaderEpoch (4 bytes)
// Offset 16: magic (1 byte) - should be 2
// Offset 17: crc (4 bytes)
// Offset 21: attributes (2 bytes) - bits 0-2 = codec
// Offset 23: lastOffsetDelta (4 bytes)
// Offset 27: firstTimestamp (8 bytes)
// Offset 35: maxTimestamp (8 bytes)
// Offset 43: producerId (8 bytes)
// Offset 51: producerEpoch (2 bytes)
// Offset 53: baseSequence (4 bytes)
// Offset 57: recordCount (4 bytes)
// Offset 61: records start
func parseRecordBatch(data []byte) ([]ParsedMessage, error) {
	if len(data) < 61 {
		return nil, fmt.Errorf("record batch too short: %d bytes", len(data))
	}

	// Parse header
	baseOffset := int64(binary.BigEndian.Uint64(data[0:8]))
	// batchLength := int32(binary.BigEndian.Uint32(data[8:12]))
	// magic := data[16]
	attributes := int16(binary.BigEndian.Uint16(data[21:23]))
	firstTimestamp := int64(binary.BigEndian.Uint64(data[27:35]))
	recordCount := int32(binary.BigEndian.Uint32(data[57:61]))

	// Get codec from attributes (bits 0-2)
	codec := int8(attributes & 0x07)

	// Records start at byte 61
	recordsData := data[61:]

	// Decompress if needed
	if codec != 0 {
		decompressed, err := decompress(recordsData, codec)
		if err != nil {
			return nil, fmt.Errorf("decompress failed: %w", err)
		}
		recordsData = decompressed
	}

	// Parse individual records
	messages := make([]ParsedMessage, 0, recordCount)
	offset := 0

	for i := int32(0); i < recordCount && offset < len(recordsData); i++ {
		// Record length (varint)
		recordLen, n := readVarint(recordsData[offset:])
		if n <= 0 {
			break
		}
		offset += n

		recordEnd := offset + int(recordLen)
		if recordEnd > len(recordsData) {
			break
		}

		recordData := recordsData[offset:recordEnd]
		offset = recordEnd

		msg, err := parseRecord(recordData, baseOffset, firstTimestamp)
		if err != nil {
			continue
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

// parseRecord parses a single Kafka record from within a batch
func parseRecord(data []byte, baseOffset, firstTimestamp int64) (ParsedMessage, error) {
	if len(data) < 1 {
		return ParsedMessage{}, fmt.Errorf("record too short")
	}

	pos := 0

	// attributes: int8 (1 byte)
	// _ = data[pos]
	pos++

	// timestampDelta: varint
	timestampDelta, n := readVarint(data[pos:])
	if n <= 0 {
		return ParsedMessage{}, fmt.Errorf("invalid timestamp delta")
	}
	pos += n

	// offsetDelta: varint
	offsetDelta, n := readVarint(data[pos:])
	if n <= 0 {
		return ParsedMessage{}, fmt.Errorf("invalid offset delta")
	}
	pos += n

	// keyLength: varint
	keyLen, n := readVarint(data[pos:])
	if n <= 0 {
		return ParsedMessage{}, fmt.Errorf("invalid key length")
	}
	pos += n

	// key: bytes
	var key []byte
	if keyLen > 0 {
		if pos+int(keyLen) > len(data) {
			return ParsedMessage{}, fmt.Errorf("key overflow")
		}
		key = data[pos : pos+int(keyLen)]
		pos += int(keyLen)
	}

	// valueLength: varint
	valueLen, n := readVarint(data[pos:])
	if n <= 0 {
		return ParsedMessage{}, fmt.Errorf("invalid value length")
	}
	pos += n

	// value: bytes
	var value []byte
	if valueLen > 0 {
		if pos+int(valueLen) > len(data) {
			return ParsedMessage{}, fmt.Errorf("value overflow")
		}
		value = data[pos : pos+int(valueLen)]
	}

	return ParsedMessage{
		Offset:    baseOffset + offsetDelta,
		Timestamp: firstTimestamp + timestampDelta,
		Key:       key,
		Value:     value,
	}, nil
}

// readVarint reads a zigzag-encoded varint from data
func readVarint(data []byte) (int64, int) {
	if len(data) == 0 {
		return 0, 0
	}

	var result uint64
	var shift uint
	var i int

	for i = 0; i < len(data) && i < 10; i++ {
		b := data[i]
		result |= uint64(b&0x7F) << shift
		shift += 7
		if b&0x80 == 0 {
			// Zigzag decode
			return int64((result >> 1) ^ -(result & 1)), i + 1
		}
	}

	return 0, 0
}

func (s *HTTPServer) handleStatic(w http.ResponseWriter, r *http.Request) {
	// Serve embedded static files
	distFS, err := fs.Sub(web.DistFS, "dist")
	if err != nil {
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// For SPA: serve index.html for non-asset paths
	path := r.URL.Path
	if path == "/" || (!strings.Contains(path, ".") && !strings.HasPrefix(path, "/assets/")) {
		// Serve index.html for root and non-file paths (SPA routing)
		content, err := fs.ReadFile(distFS, "index.html")
		if err != nil {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		w.Write(content)
		return
	}

	// Serve static files
	http.FileServer(http.FS(distFS)).ServeHTTP(w, r)
}

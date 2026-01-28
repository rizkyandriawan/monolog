package server

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"strconv"
	"strings"

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
			result = append(result, map[string]interface{}{
				"offset":    rec.Offset,
				"timestamp": rec.Timestamp,
				"key":       string(rec.Key),
				"value":     string(rec.Value), // TODO: decompress for display
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

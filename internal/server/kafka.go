package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rizkyandriawan/monolog/internal/config"
	"github.com/rizkyandriawan/monolog/internal/engine"
	"github.com/rizkyandriawan/monolog/internal/protocol"
)

// KafkaServer handles Kafka protocol connections
type KafkaServer struct {
	config      *config.Config
	engine      *engine.Engine
	listener    net.Listener
	connections sync.Map
	connCount   int32
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// NewKafkaServer creates a new KafkaServer
func NewKafkaServer(cfg *config.Config, eng *engine.Engine) *KafkaServer {
	return &KafkaServer{
		config:   cfg,
		engine:   eng,
		stopChan: make(chan struct{}),
	}
}

// ListenAndServe starts the server
func (s *KafkaServer) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.config.Server.KafkaAddr)
	if err != nil {
		return err
	}
	s.listener = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stopChan:
				return nil
			default:
				log.Printf("[kafka] accept error: %v", err)
				continue
			}
		}

		// Check connection limit
		if int(atomic.LoadInt32(&s.connCount)) >= s.config.Limits.MaxConnections {
			log.Printf("[kafka] connection limit reached, rejecting")
			conn.Close()
			continue
		}

		atomic.AddInt32(&s.connCount, 1)
		s.connections.Store(conn, true)

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// Close closes the server
func (s *KafkaServer) Close() error {
	close(s.stopChan)
	if s.listener != nil {
		s.listener.Close()
	}

	// Close all connections
	s.connections.Range(func(key, value interface{}) bool {
		if conn, ok := key.(net.Conn); ok {
			conn.Close()
		}
		return true
	})

	s.wg.Wait()
	return nil
}

func (s *KafkaServer) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	log.Printf("[kafka] new connection from %s", remoteAddr)
	defer func() {
		log.Printf("[kafka] closing connection from %s", remoteAddr)
		conn.Close()
		s.connections.Delete(conn)
		atomic.AddInt32(&s.connCount, -1)
		s.engine.GetPendingQueue().Remove(conn)
		s.wg.Done()
	}()

	authenticated := !s.config.Security.Enabled

	for {
		select {
		case <-s.stopChan:
			return
		default:
		}

		// Read message size (4 bytes)
		sizeBuf := make([]byte, 4)
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := io.ReadFull(conn, sizeBuf)
		if err != nil {
			if err == io.EOF {
				log.Printf("[kafka] client closed connection (EOF)")
			} else if n > 0 {
				log.Printf("[kafka] read size error: %v (read %d bytes)", err, n)
			}
			return
		}

		size := int32(binary.BigEndian.Uint32(sizeBuf))
		if size < 0 || size > int32(s.config.Limits.MaxMessageSize) {
			log.Printf("[kafka] invalid message size: %d", size)
			return
		}

		// Read message body
		body := make([]byte, size)
		if _, err := io.ReadFull(conn, body); err != nil {
			log.Printf("[kafka] read body error: %v", err)
			return
		}

		// Decode and handle request
		response, err := s.handleRequest(conn, body, &authenticated)
		if err != nil {
			log.Printf("[kafka] handle error: %v", err)
			continue
		}

		if response == nil {
			// No response needed (e.g., async fetch)
			continue
		}

		// Write response
		conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := conn.Write(response); err != nil {
			log.Printf("[kafka] write error: %v", err)
			return
		}
	}
}

func (s *KafkaServer) handleRequest(conn net.Conn, body []byte, authenticated *bool) ([]byte, error) {
	decoder := protocol.NewDecoder(bytes.NewReader(body))

	// Read header
	header, err := decoder.ReadHeader()
	if err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}

	log.Printf("[kafka] request: api=%d version=%d corr=%d client=%s",
		header.APIKey, header.APIVersion, header.CorrelationID, header.ClientID)

	// Check authentication for non-auth APIs
	if !*authenticated && header.APIKey != protocol.APIKeySaslHandshake &&
		header.APIKey != protocol.APIKeySaslAuthenticate &&
		header.APIKey != protocol.APIKeyApiVersions {
		return s.errorResponse(header.CorrelationID, protocol.ErrSaslAuthenticationFailed), nil
	}

	// Dispatch to handler
	var resp []byte
	var handlerErr error

	switch header.APIKey {
	case protocol.APIKeyApiVersions:
		resp, handlerErr = s.handleApiVersions(header, decoder)
	case protocol.APIKeySaslHandshake:
		resp, handlerErr = s.handleSaslHandshake(header, decoder)
	case protocol.APIKeySaslAuthenticate:
		resp, handlerErr = s.handleSaslAuthenticate(header, decoder, authenticated)
	case protocol.APIKeyMetadata:
		resp, handlerErr = s.handleMetadata(header, decoder)
	case protocol.APIKeyCreateTopics:
		resp, handlerErr = s.handleCreateTopics(header, decoder)
	case protocol.APIKeyProduce:
		resp, handlerErr = s.handleProduce(header, decoder)
	case protocol.APIKeyFetch:
		resp, handlerErr = s.handleFetch(conn, header, decoder)
	case protocol.APIKeyListOffsets:
		resp, handlerErr = s.handleListOffsets(header, decoder)
	case protocol.APIKeyFindCoordinator:
		resp, handlerErr = s.handleFindCoordinator(header, decoder)
	case protocol.APIKeyJoinGroup:
		resp, handlerErr = s.handleJoinGroup(header, decoder)
	case protocol.APIKeySyncGroup:
		resp, handlerErr = s.handleSyncGroup(header, decoder)
	case protocol.APIKeyHeartbeat:
		resp, handlerErr = s.handleHeartbeat(header, decoder)
	case protocol.APIKeyLeaveGroup:
		resp, handlerErr = s.handleLeaveGroup(header, decoder)
	case protocol.APIKeyOffsetCommit:
		resp, handlerErr = s.handleOffsetCommit(header, decoder)
	case protocol.APIKeyOffsetFetch:
		resp, handlerErr = s.handleOffsetFetch(header, decoder)
	default:
		log.Printf("[kafka] unsupported API key: %d", header.APIKey)
		return s.errorResponse(header.CorrelationID, protocol.ErrUnsupportedVersion), nil
	}

	if handlerErr != nil {
		log.Printf("[kafka] handler error for api=%d: %v", header.APIKey, handlerErr)
	}
	return resp, handlerErr
}

// ============================================================================
// API Handlers
// ============================================================================

func (s *KafkaServer) handleApiVersions(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	// Decode request (we don't really need the contents for our response)
	protocol.DecodeApiVersionsRequest(dec, header.APIVersion)

	// Build response
	resp := &protocol.ApiVersionsResponse{
		ErrorCode:    protocol.ErrNone,
		ApiVersions:  protocol.DefaultApiVersions(),
		ThrottleTimeMs: 0,
	}

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)
	protocol.EncodeApiVersionsResponse(enc, header.APIVersion, resp)

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleSaslHandshake(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	mechanism, _ := dec.ReadString()

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)

	if mechanism == "PLAIN" {
		enc.WriteInt16(protocol.ErrNone)
		enc.WriteArrayLen(1)
		enc.WriteString("PLAIN")
	} else {
		enc.WriteInt16(protocol.ErrUnsupportedSaslMechanism)
		enc.WriteArrayLen(1)
		enc.WriteString("PLAIN")
	}

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleSaslAuthenticate(header protocol.RequestHeader, dec *protocol.Decoder, authenticated *bool) ([]byte, error) {
	authBytes, _ := dec.ReadBytes()

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)

	// Parse PLAIN auth: \0username\0password
	parts := bytes.Split(authBytes, []byte{0})
	var password string
	if len(parts) >= 3 {
		password = string(parts[2])
	}

	if password == s.config.Security.Token {
		*authenticated = true
		enc.WriteInt16(protocol.ErrNone)
		enc.WriteNullableString(nil)
		enc.WriteBytes(nil)
	} else {
		enc.WriteInt16(protocol.ErrSaslAuthenticationFailed)
		errMsg := "Authentication failed"
		enc.WriteNullableString(&errMsg)
		enc.WriteBytes(nil)
	}

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleMetadata(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	req, err := protocol.DecodeMetadataRequest(dec, header.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("decode metadata request: %w", err)
	}

	log.Printf("[kafka] metadata: topics=%v allowAutoCreate=%v", req.Topics, req.AllowAutoTopicCreation)

	// Get broker info
	host, port := parseAddr(s.config.Server.KafkaAddr)

	// Determine which topics to return
	var topicNames []string
	if req.Topics == nil || len(req.Topics) == 0 {
		// All topics
		topicNames = s.engine.ListTopics()
	} else {
		topicNames = req.Topics
	}

	// Build response
	resp := &protocol.MetadataResponse{
		ThrottleTimeMs: 0,
		Brokers: []protocol.MetadataBroker{
			{NodeID: 0, Host: host, Port: port, Rack: nil},
		},
		ClusterID:         strPtr("monolog-cluster"),
		ControllerID:      0,
		IncludeClusterOps: req.IncludeClusterAuthorizedOperations,
		IncludeTopicOps:   req.IncludeTopicAuthorizedOperations,
	}

	for _, name := range topicNames {
		exists := s.engine.TopicExists(name)

		// Auto-create topic if it doesn't exist and auto-creation is allowed
		if !exists && req.AllowAutoTopicCreation {
			err := s.engine.CreateTopic(name)
			if err == nil {
				exists = true
				log.Printf("[kafka] auto-created topic: %s", name)
			}
		}

		topic := protocol.MetadataTopic{
			Name:       name,
			IsInternal: false,
		}

		if exists {
			topic.ErrorCode = protocol.ErrNone
			topic.Partitions = []protocol.MetadataPartition{
				{
					ErrorCode:       protocol.ErrNone,
					PartitionIndex:  0,
					LeaderID:        0,
					LeaderEpoch:     0,
					ReplicaNodes:    []int32{0},
					IsrNodes:        []int32{0},
					OfflineReplicas: []int32{},
				},
			}
		} else {
			topic.ErrorCode = protocol.ErrUnknownTopicOrPartition
			topic.Partitions = []protocol.MetadataPartition{}
		}

		resp.Topics = append(resp.Topics, topic)
	}

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)
	protocol.EncodeMetadataResponse(enc, header.APIVersion, resp)

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleCreateTopics(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	req, err := protocol.DecodeCreateTopicsRequest(dec, header.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("decode create topics request: %w", err)
	}

	resp := &protocol.CreateTopicsResponse{
		ThrottleTimeMs: 0,
	}

	for _, t := range req.Topics {
		result := protocol.CreateTopicsResponseTopic{
			Name: t.Name,
		}

		err := s.engine.CreateTopic(t.Name)
		if err != nil {
			result.ErrorCode = protocol.ErrTopicAlreadyExists
		} else {
			result.ErrorCode = protocol.ErrNone
		}

		resp.Topics = append(resp.Topics, result)
	}

	enc := protocol.NewEncoder()
	if header.APIVersion >= 5 {
		enc.WriteResponseHeaderV1(header.CorrelationID)
	} else {
		enc.WriteResponseHeader(header.CorrelationID)
	}
	protocol.EncodeCreateTopicsResponse(enc, header.APIVersion, resp)

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleProduce(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	req, err := protocol.DecodeProduceRequest(dec, header.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("decode produce request: %w", err)
	}

	resp := &protocol.ProduceResponse{
		ThrottleTimeMs: 0,
	}

	for _, t := range req.Topics {
		topicResp := protocol.ProduceResponseTopic{
			Name: t.Name,
		}

		for _, p := range t.Partitions {
			partResp := protocol.ProduceResponsePartition{
				Index:           p.Index,
				LogAppendTimeMs: -1,
				LogStartOffset:  0,
			}

			// Extract codec from record batch attributes (bytes 21-22)
			var codec int8 = 0
			if len(p.Records) >= 23 {
				attrs := int16(p.Records[21])<<8 | int16(p.Records[22])
				codec = int8(attrs & 0x07)
			}

			// Store raw (passthrough)
			baseOffset, err := s.engine.ProduceRaw(t.Name, p.Records, codec, 1)
			if err != nil {
				partResp.ErrorCode = protocol.ErrUnknownTopicOrPartition
			} else {
				partResp.ErrorCode = protocol.ErrNone
				partResp.BaseOffset = baseOffset
			}

			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)
	protocol.EncodeProduceResponse(enc, header.APIVersion, resp)

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleFetch(conn net.Conn, header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	req, err := protocol.DecodeFetchRequest(dec, header.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("decode fetch request: %w", err)
	}

	resp := &protocol.FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:    protocol.ErrNone,
		SessionID:    0,
	}

	for _, t := range req.Topics {
		topicResp := protocol.FetchResponseTopic{
			Name: t.Name,
		}

		for _, p := range t.Partitions {
			partResp := protocol.FetchResponsePartition{
				Index:                p.Index,
				PreferredReadReplica: -1,
			}

			if !s.engine.TopicExists(t.Name) {
				partResp.ErrorCode = protocol.ErrUnknownTopicOrPartition
			} else {
				records, _ := s.engine.Fetch(t.Name, p.FetchOffset, 100)
				latest, _ := s.engine.LatestOffset(t.Name)
				earliest, _ := s.engine.EarliestOffset(t.Name)

				partResp.ErrorCode = protocol.ErrNone
				partResp.HighWatermark = latest + 1
				partResp.LastStableOffset = latest + 1
				partResp.LogStartOffset = earliest

				if len(records) > 0 {
					// Return the raw batch data with patched baseOffset
					batchData := make([]byte, len(records[0].Value))
					copy(batchData, records[0].Value)
					// Patch baseOffset (bytes 0-7) to match our assigned offset
					if len(batchData) >= 8 {
						binary.BigEndian.PutUint64(batchData[0:8], uint64(records[0].Offset))
					}
					partResp.Records = batchData
				}
			}

			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	// Long-polling is disabled to avoid out-of-order responses on the same connection.
	// Clients will retry with a short poll interval.
	// TODO: Implement proper request pipelining with ordered response delivery.

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)
	protocol.EncodeFetchResponse(enc, header.APIVersion, resp)

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleAsyncFetch(conn net.Conn, header protocol.RequestHeader, req *engine.PendingFetch, responseChan chan engine.FetchResult) {
	select {
	case result := <-responseChan:
		latest, _ := s.engine.LatestOffset(req.Topic)
		earliest, _ := s.engine.EarliestOffset(req.Topic)

		resp := &protocol.FetchResponse{
			ThrottleTimeMs: 0,
			ErrorCode:    protocol.ErrNone,
			SessionID:    0,
			Topics: []protocol.FetchResponseTopic{
				{
					Name: req.Topic,
					Partitions: []protocol.FetchResponsePartition{
						{
							Index:                req.Partition,
							ErrorCode:            protocol.ErrNone,
							HighWatermark:        latest + 1,
							LastStableOffset:     latest + 1,
							LogStartOffset:       earliest,
							PreferredReadReplica: -1,
						},
					},
				},
			},
		}

		if len(result.Records) > 0 {
			resp.Topics[0].Partitions[0].Records = result.Records[0].Value
		}

		enc := protocol.NewEncoder()
		enc.WriteResponseHeader(header.CorrelationID)
		protocol.EncodeFetchResponse(enc, header.APIVersion, resp)

		conn.Write(s.wrapResponse(enc.Bytes()))

	case <-s.stopChan:
		return
	}
}

func (s *KafkaServer) handleListOffsets(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	req, err := protocol.DecodeListOffsetsRequest(dec, header.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("decode list offsets request: %w", err)
	}

	resp := &protocol.ListOffsetsResponse{
		ThrottleTimeMs: 0,
	}

	for _, t := range req.Topics {
		topicResp := protocol.ListOffsetsResponseTopic{
			Name: t.Name,
		}

		for _, p := range t.Partitions {
			partResp := protocol.ListOffsetsResponsePartition{
				PartitionIndex: p.PartitionIndex,
				LeaderEpoch:    -1,
			}

			var offset int64
			var err error

			if p.Timestamp == protocol.OffsetLatest {
				offset, err = s.engine.LatestOffset(t.Name)
				if err == nil {
					offset++ // next offset
				}
			} else if p.Timestamp == protocol.OffsetEarliest {
				offset, err = s.engine.EarliestOffset(t.Name)
			}

			if err != nil {
				partResp.ErrorCode = protocol.ErrUnknownTopicOrPartition
			} else {
				partResp.ErrorCode = protocol.ErrNone
				partResp.Timestamp = p.Timestamp
				partResp.Offset = offset
			}

			topicResp.Partitions = append(topicResp.Partitions, partResp)
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)
	protocol.EncodeListOffsetsResponse(enc, header.APIVersion, resp)

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleFindCoordinator(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	req, err := protocol.DecodeFindCoordinatorRequest(dec, header.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("decode find coordinator request: %w", err)
	}

	_ = req // we don't really need the key for our single-node response

	host, port := parseAddr(s.config.Server.KafkaAddr)

	resp := &protocol.FindCoordinatorResponse{
		ThrottleTimeMs: 0,
		ErrorCode:    protocol.ErrNone,
		NodeID:       0,
		Host:         host,
		Port:         port,
	}

	enc := protocol.NewEncoder()
	if header.APIVersion >= 3 {
		enc.WriteResponseHeaderV1(header.CorrelationID)
	} else {
		enc.WriteResponseHeader(header.CorrelationID)
	}
	protocol.EncodeFindCoordinatorResponse(enc, header.APIVersion, resp)

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleJoinGroup(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	// Read JoinGroup request fields
	groupID, _ := dec.ReadString()
	dec.ReadInt32() // session_timeout
	if header.APIVersion >= 1 {
		dec.ReadInt32() // rebalance_timeout
	}
	memberID, _ := dec.ReadString()
	if header.APIVersion >= 5 {
		dec.ReadNullableString() // group_instance_id
	}
	protocolType, _ := dec.ReadString()
	protocolCount, _ := dec.ReadInt32()

	var protocols []string
	var firstMetadata []byte
	for i := int32(0); i < protocolCount; i++ {
		name, _ := dec.ReadString()
		metadata, _ := dec.ReadBytes()
		protocols = append(protocols, name)
		if i == 0 {
			firstMetadata = metadata
		}
	}

	log.Printf("[kafka] join group: group=%s member=%s", groupID, memberID)

	// Generate member ID if empty
	if memberID == "" {
		memberID = fmt.Sprintf("%s-%d", groupID, time.Now().UnixNano())
	}

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)

	// Throttle time (v2+)
	if header.APIVersion >= 2 {
		enc.WriteInt32(0)
	}

	enc.WriteInt16(protocol.ErrNone)      // error_code
	enc.WriteInt32(1)                     // generation_id

	// protocol_type (v7+ only, nullable string)
	if header.APIVersion >= 7 {
		enc.WriteNullableString(&protocolType)
	}

	// protocol_name
	if len(protocols) > 0 {
		enc.WriteString(protocols[0])
	} else {
		enc.WriteString("")
	}

	enc.WriteString(memberID)             // leader (this member is leader)
	enc.WriteString(memberID)             // member_id

	// Members array (only leader gets this)
	enc.WriteArrayLen(1)
	enc.WriteString(memberID)             // member_id
	if header.APIVersion >= 5 {
		enc.WriteNullableString(nil)      // group_instance_id
	}
	enc.WriteBytes(firstMetadata)         // metadata (contains subscribed topics)

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleSyncGroup(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	dec.ReadString() // group_id
	dec.ReadInt32()  // generation_id
	memberID, _ := dec.ReadString()
	if header.APIVersion >= 3 {
		dec.ReadNullableString() // group_instance_id
	}

	// Read assignments sent by leader
	var memberAssignment []byte
	assignmentCount, _ := dec.ReadInt32()
	for i := int32(0); i < assignmentCount; i++ {
		assignedMember, _ := dec.ReadString()
		assignment, _ := dec.ReadBytes()
		if assignedMember == memberID {
			memberAssignment = assignment
		}
	}

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)

	// Throttle time (v1+)
	if header.APIVersion >= 1 {
		enc.WriteInt32(0)
	}

	enc.WriteInt16(protocol.ErrNone) // error_code

	// Return the assignment for this member
	if len(memberAssignment) > 0 {
		enc.WriteBytes(memberAssignment)
	} else {
		// No assignment found - return empty
		enc.WriteBytes([]byte{})
	}

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleHeartbeat(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	groupID, _ := dec.ReadString()
	generationID, _ := dec.ReadInt32()
	memberID, _ := dec.ReadString()

	log.Printf("[kafka] heartbeat: group=%s generation=%d member=%s", groupID, generationID, memberID)

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)

	// Throttle time (v1+)
	if header.APIVersion >= 1 {
		enc.WriteInt32(0)
	}

	enc.WriteInt16(protocol.ErrNone) // error_code

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleLeaveGroup(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	groupID, _ := dec.ReadString()
	memberID, _ := dec.ReadString()

	log.Printf("[kafka] leave group: group=%s member=%s", groupID, memberID)

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)

	// Throttle time (v1+)
	if header.APIVersion >= 1 {
		enc.WriteInt32(0)
	}

	enc.WriteInt16(protocol.ErrNone) // error_code

	// v3+ has members array
	if header.APIVersion >= 3 {
		enc.WriteArrayLen(1)
		enc.WriteString(memberID)
		if header.APIVersion >= 3 {
			enc.WriteNullableString(nil) // group_instance_id
		}
		enc.WriteInt16(protocol.ErrNone) // error_code
	}

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleOffsetCommit(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	groupID, _ := dec.ReadString()
	generationID, _ := dec.ReadInt32()
	memberID, _ := dec.ReadString()

	// v2+ has retention time
	if header.APIVersion >= 2 && header.APIVersion < 5 {
		dec.ReadInt64() // retention_time_ms
	}

	// v7+ has group_instance_id
	if header.APIVersion >= 7 {
		dec.ReadNullableString() // group_instance_id
	}

	topicCount, _ := dec.ReadInt32()

	log.Printf("[kafka] offset commit: group=%s gen=%d member=%s topics=%d",
		groupID, generationID, memberID, topicCount)

	// Ensure group exists
	s.engine.GetOrCreateGroup(groupID)

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)

	// Throttle time (v3+)
	if header.APIVersion >= 3 {
		enc.WriteInt32(0)
	}

	// Topics array
	enc.WriteArrayLen(int(topicCount))
	for i := int32(0); i < topicCount; i++ {
		topicName, _ := dec.ReadString()
		partCount, _ := dec.ReadInt32()

		enc.WriteString(topicName)
		enc.WriteArrayLen(int(partCount))

		for j := int32(0); j < partCount; j++ {
			partIndex, _ := dec.ReadInt32()
			committedOffset, _ := dec.ReadInt64()

			// v6+ has committed_leader_epoch
			if header.APIVersion >= 6 {
				dec.ReadInt32() // committed_leader_epoch
			}

			// v1+ has timestamp (deprecated in v2+)
			if header.APIVersion == 1 {
				dec.ReadInt64() // commit_timestamp
			}

			dec.ReadNullableString() // metadata

			// Commit the offset (we only support partition 0)
			var errCode int16 = protocol.ErrNone
			if partIndex == 0 {
				if err := s.engine.CommitOffset(groupID, topicName, committedOffset); err != nil {
					log.Printf("[kafka] offset commit error: %v", err)
					errCode = protocol.ErrCoordinatorNotAvailable
				}
			}

			enc.WriteInt32(partIndex)
			enc.WriteInt16(errCode)
		}
	}

	return s.wrapResponse(enc.Bytes()), nil
}

func (s *KafkaServer) handleOffsetFetch(header protocol.RequestHeader, dec *protocol.Decoder) ([]byte, error) {
	groupID, _ := dec.ReadString()
	topicCount, _ := dec.ReadInt32()

	log.Printf("[kafka] offset fetch: group=%s topics=%d", groupID, topicCount)

	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(header.CorrelationID)

	// Throttle time (v3+)
	if header.APIVersion >= 3 {
		enc.WriteInt32(0)
	}

	// Topics array
	enc.WriteArrayLen(int(topicCount))
	for i := int32(0); i < topicCount; i++ {
		topicName, _ := dec.ReadString()
		partCount, _ := dec.ReadInt32()

		enc.WriteString(topicName)
		enc.WriteArrayLen(int(partCount))

		for j := int32(0); j < partCount; j++ {
			partIndex, _ := dec.ReadInt32()

			// Fetch committed offset from storage
			var committedOffset int64 = -1
			if partIndex == 0 {
				offset, err := s.engine.FetchOffset(groupID, topicName)
				if err == nil && offset >= 0 {
					committedOffset = offset
				}
			}

			enc.WriteInt32(partIndex)
			enc.WriteInt64(committedOffset)
			if header.APIVersion >= 5 {
				enc.WriteInt32(-1) // committed_leader_epoch
			}
			enc.WriteNullableString(nil) // metadata
			enc.WriteInt16(protocol.ErrNone)
		}
	}

	// Error code (v2+)
	if header.APIVersion >= 2 {
		enc.WriteInt16(protocol.ErrNone)
	}

	return s.wrapResponse(enc.Bytes()), nil
}

// ============================================================================
// Helpers
// ============================================================================

func (s *KafkaServer) errorResponse(correlationID int32, errorCode int16) []byte {
	enc := protocol.NewEncoder()
	enc.WriteResponseHeader(correlationID)
	enc.WriteInt16(errorCode)
	return s.wrapResponse(enc.Bytes())
}

func (s *KafkaServer) wrapResponse(body []byte) []byte {
	size := len(body)
	result := make([]byte, 4+size)
	binary.BigEndian.PutUint32(result[:4], uint32(size))
	copy(result[4:], body)
	return result
}

func parseAddr(addr string) (string, int32) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "localhost", 9092
	}
	if host == "" {
		host = "localhost"
	}
	var port int32 = 9092
	fmt.Sscanf(portStr, "%d", &port)
	return host, port
}

func strPtr(s string) *string {
	return &s
}

package protocol

// ============================================================================
// Metadata (API Key 3)
// Supported versions: 0-8
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type MetadataRequest struct {
	Topics                             []string // nil = all topics
	AllowAutoTopicCreation             bool     // v4+
	IncludeClusterAuthorizedOperations bool     // v8+
	IncludeTopicAuthorizedOperations   bool     // v8+
}

// Request Readers

func (r *MetadataRequest) readTopics(d *Decoder) {
	count, _ := d.ReadInt32()

	if count > 0 {
		r.Topics = make([]string, count)
		for i := range r.Topics {
			r.Topics[i], _ = d.ReadString()
		}
	} else if count == -1 {
		r.Topics = nil // all topics
	}
}

func (r *MetadataRequest) readAllowAutoTopicCreation(d *Decoder) {
	r.AllowAutoTopicCreation, _ = d.ReadBool()
}

func (r *MetadataRequest) readAuthorizedOpsFlags(d *Decoder) {
	r.IncludeClusterAuthorizedOperations, _ = d.ReadBool()
	r.IncludeTopicAuthorizedOperations, _ = d.ReadBool()
}

// Decode - the recipe

func DecodeMetadataRequest(d *Decoder, v int16) (*MetadataRequest, error) {
	r := &MetadataRequest{}

	r.readTopics(d)                             // v0+
	if v >= 4 {
		r.readAllowAutoTopicCreation(d)         // v4+
	}
	if v >= 8 {
		r.readAuthorizedOpsFlags(d)             // v8+
	}

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type MetadataResponse struct {
	ThrottleTimeMs       int32 // v3+
	Brokers              []MetadataBroker
	ClusterID            *string // v2+
	ControllerID         int32   // v1+
	Topics               []MetadataTopic
	ClusterAuthorizedOps int32 // v8+
	IncludeClusterOps    bool  // internal: whether to include cluster ops
	IncludeTopicOps      bool  // internal: whether to include topic ops
}

type MetadataBroker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   *string // v1+
}

type MetadataTopic struct {
	ErrorCode          int16
	Name               string
	IsInternal         bool // v1+
	Partitions         []MetadataPartition
	TopicAuthorizedOps int32 // v8+
}

type MetadataPartition struct {
	ErrorCode       int16
	PartitionIndex  int32
	LeaderID        int32
	LeaderEpoch     int32   // v7+
	ReplicaNodes    []int32
	IsrNodes        []int32
	OfflineReplicas []int32 // v5+
}

// Response Writers

func (r *MetadataResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *MetadataResponse) writeBrokers(e *Encoder, version int16) {
	e.WriteArrayLen(len(r.Brokers))

	for _, b := range r.Brokers {
		b.writeTo(e, version)
	}
}

func (b *MetadataBroker) writeTo(e *Encoder, version int16) {
	e.WriteInt32(b.NodeID)
	e.WriteString(b.Host)
	e.WriteInt32(b.Port)

	if version >= 1 {
		e.WriteNullableString(b.Rack)           // v1+
	}
}

func (r *MetadataResponse) writeClusterID(e *Encoder) {
	e.WriteNullableString(r.ClusterID)
}

func (r *MetadataResponse) writeControllerID(e *Encoder) {
	e.WriteInt32(r.ControllerID)
}

func (r *MetadataResponse) writeTopics(e *Encoder, version int16) {
	e.WriteArrayLen(len(r.Topics))

	for _, t := range r.Topics {
		t.writeTo(e, version, r.IncludeTopicOps)
	}
}

func (t *MetadataTopic) writeTo(e *Encoder, version int16, includeOps bool) {
	e.WriteInt16(t.ErrorCode)
	e.WriteString(t.Name)

	if version >= 1 {
		e.WriteBool(t.IsInternal)               // v1+
	}

	e.WriteArrayLen(len(t.Partitions))
	for _, p := range t.Partitions {
		p.writeTo(e, version)
	}

	if version >= 8 {
		if includeOps {
			e.WriteInt32(t.TopicAuthorizedOps)  // v8+
		} else {
			e.WriteInt32(-2147483648)           // INT32_MIN = not requested
		}
	}
}

func (p *MetadataPartition) writeTo(e *Encoder, version int16) {
	e.WriteInt16(p.ErrorCode)
	e.WriteInt32(p.PartitionIndex)
	e.WriteInt32(p.LeaderID)

	if version >= 7 {
		e.WriteInt32(p.LeaderEpoch)             // v7+
	}

	e.WriteArrayLen(len(p.ReplicaNodes))
	for _, r := range p.ReplicaNodes {
		e.WriteInt32(r)
	}

	e.WriteArrayLen(len(p.IsrNodes))
	for _, r := range p.IsrNodes {
		e.WriteInt32(r)
	}

	if version >= 5 {
		e.WriteArrayLen(len(p.OfflineReplicas)) // v5+
		for _, r := range p.OfflineReplicas {
			e.WriteInt32(r)
		}
	}
}

func (r *MetadataResponse) writeClusterAuthorizedOps(e *Encoder) {
	if r.IncludeClusterOps {
		e.WriteInt32(r.ClusterAuthorizedOps)
	} else {
		e.WriteInt32(-2147483648)               // INT32_MIN = not requested
	}
}

// Encode - the recipe

func EncodeMetadataResponse(e *Encoder, v int16, r *MetadataResponse) {
	if v >= 3 {
		r.writeThrottleTime(e)                  // v3+
	}
	r.writeBrokers(e, v)                        // v0+
	if v >= 2 {
		r.writeClusterID(e)                     // v2+
	}
	if v >= 1 {
		r.writeControllerID(e)                  // v1+
	}
	r.writeTopics(e, v)                         // v0+
	if v >= 8 {
		r.writeClusterAuthorizedOps(e)          // v8+
	}
}

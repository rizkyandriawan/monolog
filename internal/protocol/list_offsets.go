package protocol

// ============================================================================
// ListOffsets (API Key 2)
// Supported versions: 0-5
// ============================================================================

// Special timestamp values
const (
	OffsetLatest   int64 = -1 // request latest offset
	OffsetEarliest int64 = -2 // request earliest offset
)

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type ListOffsetsRequest struct {
	ReplicaID      int32
	IsolationLevel int8 // v2+
	Topics         []ListOffsetsRequestTopic
}

type ListOffsetsRequestTopic struct {
	Name       string
	Partitions []ListOffsetsRequestPartition
}

type ListOffsetsRequestPartition struct {
	PartitionIndex     int32
	CurrentLeaderEpoch int32 // v4+
	Timestamp          int64
}

// Request Readers

func (r *ListOffsetsRequest) readReplicaID(d *Decoder) {
	r.ReplicaID, _ = d.ReadInt32()
}

func (r *ListOffsetsRequest) readIsolationLevel(d *Decoder) {
	r.IsolationLevel, _ = d.ReadInt8()
}

func (r *ListOffsetsRequest) readTopics(d *Decoder, version int16) {
	count, _ := d.ReadInt32()
	r.Topics = make([]ListOffsetsRequestTopic, count)

	for i := range r.Topics {
		r.Topics[i].readFrom(d, version)
	}
}

func (t *ListOffsetsRequestTopic) readFrom(d *Decoder, version int16) {
	t.Name, _ = d.ReadString()

	count, _ := d.ReadInt32()
	t.Partitions = make([]ListOffsetsRequestPartition, count)

	for i := range t.Partitions {
		t.Partitions[i].readFrom(d, version)
	}
}

func (p *ListOffsetsRequestPartition) readFrom(d *Decoder, version int16) {
	p.PartitionIndex, _ = d.ReadInt32()

	if version >= 4 {
		p.CurrentLeaderEpoch, _ = d.ReadInt32() // v4+
	} else {
		p.CurrentLeaderEpoch = -1
	}

	p.Timestamp, _ = d.ReadInt64()

	if version == 0 {
		d.ReadInt32()                           // v0 max_num_offsets (ignored)
	}
}

// Decode - the recipe

func DecodeListOffsetsRequest(d *Decoder, v int16) (*ListOffsetsRequest, error) {
	r := &ListOffsetsRequest{}

	r.readReplicaID(d)                          // v0+
	if v >= 2 {
		r.readIsolationLevel(d)                 // v2+
	}
	r.readTopics(d, v)                          // v0+

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type ListOffsetsResponse struct {
	ThrottleTimeMs  int32 // v2+
	Topics          []ListOffsetsResponseTopic
}

type ListOffsetsResponseTopic struct {
	Name       string
	Partitions []ListOffsetsResponsePartition
}

type ListOffsetsResponsePartition struct {
	PartitionIndex  int32
	ErrorCode       int16
	Timestamp       int64   // v1+
	Offset          int64   // v1+
	LeaderEpoch     int32   // v4+
	OldStyleOffsets []int64 // v0 only
}

// Response Writers

func (r *ListOffsetsResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *ListOffsetsResponse) writeTopics(e *Encoder, version int16) {
	e.WriteArrayLen(len(r.Topics))

	for _, t := range r.Topics {
		t.writeTo(e, version)
	}
}

func (t *ListOffsetsResponseTopic) writeTo(e *Encoder, version int16) {
	e.WriteString(t.Name)
	e.WriteArrayLen(len(t.Partitions))

	for _, p := range t.Partitions {
		p.writeTo(e, version)
	}
}

func (p *ListOffsetsResponsePartition) writeTo(e *Encoder, version int16) {
	e.WriteInt32(p.PartitionIndex)
	e.WriteInt16(p.ErrorCode)

	if version == 0 {
		e.WriteArrayLen(len(p.OldStyleOffsets)) // v0 old style
		for _, o := range p.OldStyleOffsets {
			e.WriteInt64(o)
		}
	} else {
		e.WriteInt64(p.Timestamp)               // v1+
		e.WriteInt64(p.Offset)                  // v1+
		if version >= 4 {
			e.WriteInt32(p.LeaderEpoch)         // v4+
		}
	}
}

// Encode - the recipe

func EncodeListOffsetsResponse(e *Encoder, v int16, r *ListOffsetsResponse) {
	if v >= 2 {
		r.writeThrottleTime(e)                  // v2+
	}
	r.writeTopics(e, v)                         // v0+
}

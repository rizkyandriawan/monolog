package protocol

// ============================================================================
// OffsetFetch (API Key 9)
// Supported versions: 0-8
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type OffsetFetchRequest struct {
	GroupID string
	Topics  []OffsetFetchRequestTopic
}

type OffsetFetchRequestTopic struct {
	Name       string
	Partitions []int32
}

// Request Readers

func (r *OffsetFetchRequest) readGroupID(d *Decoder) {
	r.GroupID, _ = d.ReadString()
}

func (r *OffsetFetchRequest) readTopics(d *Decoder) {
	count, _ := d.ReadInt32()
	if count < 0 {
		return // null array means all topics (v2+)
	}

	r.Topics = make([]OffsetFetchRequestTopic, count)
	for i := range r.Topics {
		r.Topics[i].readFrom(d)
	}
}

func (t *OffsetFetchRequestTopic) readFrom(d *Decoder) {
	t.Name, _ = d.ReadString()

	count, _ := d.ReadInt32()
	t.Partitions = make([]int32, count)
	for i := range t.Partitions {
		t.Partitions[i], _ = d.ReadInt32()
	}
}

// Decode - the recipe

func DecodeOffsetFetchRequest(d *Decoder, v int16) (*OffsetFetchRequest, error) {
	r := &OffsetFetchRequest{}

	r.readGroupID(d)                            // v0+
	r.readTopics(d)                             // v0+

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type OffsetFetchResponse struct {
	ThrottleTimeMs int32 // v3+
	Topics         []OffsetFetchResponseTopic
	ErrorCode      int16 // v2+
}

type OffsetFetchResponseTopic struct {
	Name       string
	Partitions []OffsetFetchResponsePartition
}

type OffsetFetchResponsePartition struct {
	Index           int32
	CommittedOffset int64
	LeaderEpoch     int32   // v5+
	Metadata        *string
	ErrorCode       int16
}

// Response Writers

func (r *OffsetFetchResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *OffsetFetchResponse) writeTopics(e *Encoder, version int16) {
	e.WriteArrayLen(len(r.Topics))

	for _, t := range r.Topics {
		t.writeTo(e, version)
	}
}

func (t *OffsetFetchResponseTopic) writeTo(e *Encoder, version int16) {
	e.WriteString(t.Name)
	e.WriteArrayLen(len(t.Partitions))

	for _, p := range t.Partitions {
		p.writeTo(e, version)
	}
}

func (p *OffsetFetchResponsePartition) writeTo(e *Encoder, version int16) {
	e.WriteInt32(p.Index)
	e.WriteInt64(p.CommittedOffset)

	if version >= 5 {
		e.WriteInt32(p.LeaderEpoch)              // v5+
	}

	e.WriteNullableString(p.Metadata)
	e.WriteInt16(p.ErrorCode)
}

func (r *OffsetFetchResponse) writeErrorCode(e *Encoder) {
	e.WriteInt16(r.ErrorCode)
}

// Encode - the recipe

func EncodeOffsetFetchResponse(e *Encoder, v int16, r *OffsetFetchResponse) {
	if v >= 3 {
		r.writeThrottleTime(e)                  // v3+
	}
	r.writeTopics(e, v)                         // v0+
	if v >= 2 {
		r.writeErrorCode(e)                     // v2+
	}
}

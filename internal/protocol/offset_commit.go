package protocol

// ============================================================================
// OffsetCommit (API Key 8)
// Supported versions: 0-8
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type OffsetCommitRequest struct {
	GroupID         string
	GenerationID    int32  // v1+
	MemberID        string // v1+
	GroupInstanceID string // v7+
	RetentionTimeMs int64  // v2-v4 only
	Topics          []OffsetCommitRequestTopic
}

type OffsetCommitRequestTopic struct {
	Name       string
	Partitions []OffsetCommitRequestPartition
}

type OffsetCommitRequestPartition struct {
	Index           int32
	CommittedOffset int64
	LeaderEpoch     int32   // v6+
	CommitTimestamp int64   // v1 only
	Metadata        *string
}

// Request Readers

func (r *OffsetCommitRequest) readGroupID(d *Decoder) {
	r.GroupID, _ = d.ReadString()
}

func (r *OffsetCommitRequest) readMemberInfo(d *Decoder) {
	r.GenerationID, _ = d.ReadInt32()
	r.MemberID, _ = d.ReadString()
}

func (r *OffsetCommitRequest) readGroupInstanceID(d *Decoder) {
	s, _ := d.ReadNullableString()
	if s != nil {
		r.GroupInstanceID = *s
	}
}

func (r *OffsetCommitRequest) readRetentionTime(d *Decoder) {
	r.RetentionTimeMs, _ = d.ReadInt64()
}

func (r *OffsetCommitRequest) readTopics(d *Decoder, version int16) {
	count, _ := d.ReadInt32()
	r.Topics = make([]OffsetCommitRequestTopic, count)

	for i := range r.Topics {
		r.Topics[i].readFrom(d, version)
	}
}

func (t *OffsetCommitRequestTopic) readFrom(d *Decoder, version int16) {
	t.Name, _ = d.ReadString()

	count, _ := d.ReadInt32()
	t.Partitions = make([]OffsetCommitRequestPartition, count)

	for i := range t.Partitions {
		t.Partitions[i].readFrom(d, version)
	}
}

func (p *OffsetCommitRequestPartition) readFrom(d *Decoder, version int16) {
	p.Index, _ = d.ReadInt32()
	p.CommittedOffset, _ = d.ReadInt64()

	if version >= 6 {
		p.LeaderEpoch, _ = d.ReadInt32()
	}

	if version == 1 {
		p.CommitTimestamp, _ = d.ReadInt64()
	}

	p.Metadata, _ = d.ReadNullableString()
}

// Decode - the recipe

func DecodeOffsetCommitRequest(d *Decoder, v int16) (*OffsetCommitRequest, error) {
	r := &OffsetCommitRequest{}

	r.readGroupID(d)                            // v0+
	if v >= 1 {
		r.readMemberInfo(d)                     // v1+
	}
	if v >= 2 && v <= 4 {
		r.readRetentionTime(d)                  // v2-v4 only
	}
	if v >= 7 {
		r.readGroupInstanceID(d)                // v7+
	}
	r.readTopics(d, v)                          // v0+

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type OffsetCommitResponse struct {
	ThrottleTimeMs int32 // v3+
	Topics         []OffsetCommitResponseTopic
}

type OffsetCommitResponseTopic struct {
	Name       string
	Partitions []OffsetCommitResponsePartition
}

type OffsetCommitResponsePartition struct {
	Index     int32
	ErrorCode int16
}

// Response Writers

func (r *OffsetCommitResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *OffsetCommitResponse) writeTopics(e *Encoder) {
	e.WriteArrayLen(len(r.Topics))

	for _, t := range r.Topics {
		t.writeTo(e)
	}
}

func (t *OffsetCommitResponseTopic) writeTo(e *Encoder) {
	e.WriteString(t.Name)
	e.WriteArrayLen(len(t.Partitions))

	for _, p := range t.Partitions {
		p.writeTo(e)
	}
}

func (p *OffsetCommitResponsePartition) writeTo(e *Encoder) {
	e.WriteInt32(p.Index)
	e.WriteInt16(p.ErrorCode)
}

// Encode - the recipe

func EncodeOffsetCommitResponse(e *Encoder, v int16, r *OffsetCommitResponse) {
	if v >= 3 {
		r.writeThrottleTime(e)                  // v3+
	}
	r.writeTopics(e)                            // v0+
}

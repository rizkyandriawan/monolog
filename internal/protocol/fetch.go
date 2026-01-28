package protocol

// ============================================================================
// Fetch (API Key 1)
// Supported versions: 0-11
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type FetchRequest struct {
	ReplicaID      int32
	MaxWaitMs      int32
	MinBytes       int32
	MaxBytes       int32 // v3+
	IsolationLevel int8  // v4+
	SessionID      int32 // v7+
	SessionEpoch   int32 // v7+
	Topics         []FetchRequestTopic
	RackID         string // v11+
}

type FetchRequestTopic struct {
	Name       string
	Partitions []FetchRequestPartition
}

type FetchRequestPartition struct {
	Index              int32
	CurrentLeaderEpoch int32 // v9+
	FetchOffset        int64
	LogStartOffset     int64 // v5+
	MaxBytes           int32
}

// Request Readers

func (r *FetchRequest) readReplicaID(d *Decoder) {
	r.ReplicaID, _ = d.ReadInt32()
}

func (r *FetchRequest) readWaitAndBytes(d *Decoder) {
	r.MaxWaitMs, _ = d.ReadInt32()
	r.MinBytes, _ = d.ReadInt32()
}

func (r *FetchRequest) readMaxBytes(d *Decoder) {
	r.MaxBytes, _ = d.ReadInt32()
}

func (r *FetchRequest) readIsolationLevel(d *Decoder) {
	r.IsolationLevel, _ = d.ReadInt8()
}

func (r *FetchRequest) readSessionInfo(d *Decoder) {
	r.SessionID, _ = d.ReadInt32()
	r.SessionEpoch, _ = d.ReadInt32()
}

func (r *FetchRequest) readTopics(d *Decoder, version int16) {
	count, _ := d.ReadInt32()
	r.Topics = make([]FetchRequestTopic, count)

	for i := range r.Topics {
		r.Topics[i].readFrom(d, version)
	}
}

func (t *FetchRequestTopic) readFrom(d *Decoder, version int16) {
	t.Name, _ = d.ReadString()

	count, _ := d.ReadInt32()
	t.Partitions = make([]FetchRequestPartition, count)

	for i := range t.Partitions {
		t.Partitions[i].readFrom(d, version)
	}
}

func (p *FetchRequestPartition) readFrom(d *Decoder, version int16) {
	p.Index, _ = d.ReadInt32()

	if version >= 9 {
		p.CurrentLeaderEpoch, _ = d.ReadInt32() // v9+
	} else {
		p.CurrentLeaderEpoch = -1
	}

	p.FetchOffset, _ = d.ReadInt64()

	if version >= 5 {
		p.LogStartOffset, _ = d.ReadInt64()     // v5+
	}

	p.MaxBytes, _ = d.ReadInt32()
}

func (r *FetchRequest) readForgottenTopics(d *Decoder) {
	count, _ := d.ReadInt32()
	for i := int32(0); i < count; i++ {
		d.ReadString() // topic name
		partCount, _ := d.ReadInt32()
		for j := int32(0); j < partCount; j++ {
			d.ReadInt32() // partition index
		}
	}
}

func (r *FetchRequest) readRackID(d *Decoder) {
	r.RackID, _ = d.ReadString()
}

// Decode - the recipe

func DecodeFetchRequest(d *Decoder, v int16) (*FetchRequest, error) {
	r := &FetchRequest{}

	r.readReplicaID(d)                          // v0+
	r.readWaitAndBytes(d)                       // v0+
	if v >= 3 {
		r.readMaxBytes(d)                       // v3+
	}
	if v >= 4 {
		r.readIsolationLevel(d)                 // v4+
	}
	if v >= 7 {
		r.readSessionInfo(d)                    // v7+
	}
	r.readTopics(d, v)                          // v0+
	if v >= 7 {
		r.readForgottenTopics(d)                // v7+
	}
	if v >= 11 {
		r.readRackID(d)                         // v11+
	}

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type FetchResponse struct {
	ThrottleTimeMs int32 // v1+
	ErrorCode      int16 // v7+
	SessionID      int32 // v7+
	Topics         []FetchResponseTopic
}

type FetchResponseTopic struct {
	Name       string
	Partitions []FetchResponsePartition
}

type FetchResponsePartition struct {
	Index                int32
	ErrorCode            int16
	HighWatermark        int64
	LastStableOffset     int64 // v4+
	LogStartOffset       int64 // v5+
	PreferredReadReplica int32 // v11+
	Records              []byte
}

// Response Writers

func (r *FetchResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *FetchResponse) writeErrorCode(e *Encoder) {
	e.WriteInt16(r.ErrorCode)
}

func (r *FetchResponse) writeSessionID(e *Encoder) {
	e.WriteInt32(r.SessionID)
}

func (r *FetchResponse) writeTopics(e *Encoder, version int16) {
	e.WriteArrayLen(len(r.Topics))

	for _, t := range r.Topics {
		t.writeTo(e, version)
	}
}

func (t *FetchResponseTopic) writeTo(e *Encoder, version int16) {
	e.WriteString(t.Name)
	e.WriteArrayLen(len(t.Partitions))

	for _, p := range t.Partitions {
		p.writeTo(e, version)
	}
}

func (p *FetchResponsePartition) writeTo(e *Encoder, version int16) {
	e.WriteInt32(p.Index)
	e.WriteInt16(p.ErrorCode)
	e.WriteInt64(p.HighWatermark)

	if version >= 4 {
		e.WriteInt64(p.LastStableOffset)        // v4+
	}
	if version >= 5 {
		e.WriteInt64(p.LogStartOffset)          // v5+
	}
	if version >= 4 {
		e.WriteArrayLen(0)                      // v4+ aborted_transactions (empty)
	}
	if version >= 11 {
		e.WriteInt32(p.PreferredReadReplica)    // v11+
	}

	e.WriteBytes(p.Records)                     // v0+
}

// Encode - the recipe

func EncodeFetchResponse(e *Encoder, v int16, r *FetchResponse) {
	if v >= 1 {
		r.writeThrottleTime(e)                  // v1+
	}
	if v >= 7 {
		r.writeErrorCode(e)                     // v7+
		r.writeSessionID(e)                     // v7+
	}
	r.writeTopics(e, v)                         // v0+
}

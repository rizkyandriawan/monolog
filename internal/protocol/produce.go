package protocol

// ============================================================================
// Produce (API Key 0)
// Supported versions: 0-8
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type ProduceRequest struct {
	TransactionalID *string // v3+
	Acks            int16
	TimeoutMs       int32
	Topics          []ProduceRequestTopic
}

type ProduceRequestTopic struct {
	Name       string
	Partitions []ProduceRequestPartition
}

type ProduceRequestPartition struct {
	Index   int32
	Records []byte // raw record batch
}

// Request Readers

func (r *ProduceRequest) readTransactionalID(d *Decoder) {
	r.TransactionalID, _ = d.ReadNullableString()
}

func (r *ProduceRequest) readAcks(d *Decoder) {
	r.Acks, _ = d.ReadInt16()
}

func (r *ProduceRequest) readTimeout(d *Decoder) {
	r.TimeoutMs, _ = d.ReadInt32()
}

func (r *ProduceRequest) readTopics(d *Decoder) {
	count, _ := d.ReadInt32()
	r.Topics = make([]ProduceRequestTopic, count)

	for i := range r.Topics {
		r.Topics[i].readFrom(d)
	}
}

func (t *ProduceRequestTopic) readFrom(d *Decoder) {
	t.Name, _ = d.ReadString()

	count, _ := d.ReadInt32()
	t.Partitions = make([]ProduceRequestPartition, count)

	for i := range t.Partitions {
		t.Partitions[i].Index, _ = d.ReadInt32()
		t.Partitions[i].Records, _ = d.ReadBytes()
	}
}

// Decode - the recipe

func DecodeProduceRequest(d *Decoder, v int16) (*ProduceRequest, error) {
	r := &ProduceRequest{}

	if v >= 3 {
		r.readTransactionalID(d)                // v3+
	}
	r.readAcks(d)                               // v0+
	r.readTimeout(d)                            // v0+
	r.readTopics(d)                             // v0+

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type ProduceResponse struct {
	Topics         []ProduceResponseTopic
	ThrottleTimeMs int32 // v1+
}

type ProduceResponseTopic struct {
	Name       string
	Partitions []ProduceResponsePartition
}

type ProduceResponsePartition struct {
	Index           int32
	ErrorCode       int16
	BaseOffset      int64
	LogAppendTimeMs int64 // v2+
	LogStartOffset  int64 // v5+
}

// Response Writers

func (r *ProduceResponse) writeTopics(e *Encoder, version int16) {
	e.WriteArrayLen(len(r.Topics))

	for _, t := range r.Topics {
		t.writeTo(e, version)
	}
}

func (t *ProduceResponseTopic) writeTo(e *Encoder, version int16) {
	e.WriteString(t.Name)
	e.WriteArrayLen(len(t.Partitions))

	for _, p := range t.Partitions {
		p.writeTo(e, version)
	}
}

func (p *ProduceResponsePartition) writeTo(e *Encoder, version int16) {
	e.WriteInt32(p.Index)
	e.WriteInt16(p.ErrorCode)
	e.WriteInt64(p.BaseOffset)

	if version >= 2 {
		e.WriteInt64(p.LogAppendTimeMs)         // v2+
	}
	if version >= 5 {
		e.WriteInt64(p.LogStartOffset)          // v5+
	}
	if version >= 8 {
		e.WriteArrayLen(0)                      // v8+ record_errors (empty)
		e.WriteNullableString(nil)              // v8+ error_message (null)
	}
}

func (r *ProduceResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

// Encode - the recipe

func EncodeProduceResponse(e *Encoder, v int16, r *ProduceResponse) {
	r.writeTopics(e, v)                         // v0+
	if v >= 1 {
		r.writeThrottleTime(e)                  // v1+
	}
}

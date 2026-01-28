package protocol

// ============================================================================
// CreateTopics (API Key 19)
// Supported versions: 0-5
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type CreateTopicsRequest struct {
	Topics       []CreateTopicsRequestTopic
	TimeoutMs    int32
	ValidateOnly bool // v1+
}

type CreateTopicsRequestTopic struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	Assignments       map[int32][]int32 // partition -> brokers
	Configs           map[string]string // name -> value
}

// Request Readers

func (r *CreateTopicsRequest) readTopics(d *Decoder, version int16) {
	flexible := version >= 5

	var count int
	if flexible {
		n, _ := d.ReadUVarInt()
		count = int(n) - 1
	} else {
		n, _ := d.ReadInt32()
		count = int(n)
	}

	r.Topics = make([]CreateTopicsRequestTopic, count)
	for i := range r.Topics {
		r.Topics[i].readFrom(d, version)
	}
}

func (t *CreateTopicsRequestTopic) readFrom(d *Decoder, version int16) {
	flexible := version >= 5
	t.Assignments = make(map[int32][]int32)
	t.Configs = make(map[string]string)

	// Name
	if flexible {
		t.Name, _ = d.ReadCompactString()
	} else {
		t.Name, _ = d.ReadString()
	}

	t.NumPartitions, _ = d.ReadInt32()
	t.ReplicationFactor, _ = d.ReadInt16()

	// Assignments
	t.readAssignments(d, flexible)

	// Configs
	t.readConfigs(d, flexible)

	if flexible {
		d.ReadUVarInt()                         // topic tagged fields
	}
}

func (t *CreateTopicsRequestTopic) readAssignments(d *Decoder, flexible bool) {
	var count int
	if flexible {
		n, _ := d.ReadUVarInt()
		count = int(n) - 1
	} else {
		n, _ := d.ReadInt32()
		count = int(n)
	}

	for i := 0; i < count; i++ {
		partition, _ := d.ReadInt32()

		var brokerCount int
		if flexible {
			n, _ := d.ReadUVarInt()
			brokerCount = int(n) - 1
		} else {
			n, _ := d.ReadInt32()
			brokerCount = int(n)
		}

		brokers := make([]int32, brokerCount)
		for j := range brokers {
			brokers[j], _ = d.ReadInt32()
		}
		t.Assignments[partition] = brokers

		if flexible {
			d.ReadUVarInt()                     // assignment tagged fields
		}
	}
}

func (t *CreateTopicsRequestTopic) readConfigs(d *Decoder, flexible bool) {
	var count int
	if flexible {
		n, _ := d.ReadUVarInt()
		count = int(n) - 1
	} else {
		n, _ := d.ReadInt32()
		count = int(n)
	}

	for i := 0; i < count; i++ {
		var name string
		var value *string

		if flexible {
			name, _ = d.ReadCompactString()
			value, _ = d.ReadCompactNullableString()
		} else {
			name, _ = d.ReadString()
			value, _ = d.ReadNullableString()
		}

		if value != nil {
			t.Configs[name] = *value
		}

		if flexible {
			d.ReadUVarInt()                     // config tagged fields
		}
	}
}

func (r *CreateTopicsRequest) readTimeout(d *Decoder) {
	r.TimeoutMs, _ = d.ReadInt32()
}

func (r *CreateTopicsRequest) readValidateOnly(d *Decoder) {
	r.ValidateOnly, _ = d.ReadBool()
}

func (r *CreateTopicsRequest) readTaggedFields(d *Decoder) {
	d.ReadUVarInt()
}

// Decode - the recipe

func DecodeCreateTopicsRequest(d *Decoder, v int16) (*CreateTopicsRequest, error) {
	r := &CreateTopicsRequest{}

	r.readTopics(d, v)                          // v0+
	r.readTimeout(d)                            // v0+
	if v >= 1 {
		r.readValidateOnly(d)                   // v1+
	}
	if v >= 5 {
		r.readTaggedFields(d)                   // v5+
	}

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type CreateTopicsResponse struct {
	ThrottleTimeMs int32 // v2+
	Topics         []CreateTopicsResponseTopic
}

type CreateTopicsResponseTopic struct {
	Name              string
	ErrorCode         int16
	ErrorMessage      *string // v1+
	NumPartitions     int32   // v5+
	ReplicationFactor int16   // v5+
	Configs           []CreateTopicsResponseConfig // v5+
}

type CreateTopicsResponseConfig struct {
	Name         string
	Value        *string
	ReadOnly     bool
	ConfigSource int8
	IsSensitive  bool
}

// Response Writers

func (r *CreateTopicsResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *CreateTopicsResponse) writeTopics(e *Encoder, version int16) {
	flexible := version >= 5

	if flexible {
		e.WriteCompactArrayLen(len(r.Topics))
	} else {
		e.WriteArrayLen(len(r.Topics))
	}

	for _, t := range r.Topics {
		t.writeTo(e, version)
	}
}

func (t *CreateTopicsResponseTopic) writeTo(e *Encoder, version int16) {
	flexible := version >= 5

	if flexible {
		e.WriteCompactString(t.Name)
	} else {
		e.WriteString(t.Name)
	}

	e.WriteInt16(t.ErrorCode)

	if version >= 1 {
		if flexible {
			e.WriteCompactNullableString(t.ErrorMessage)
		} else {
			e.WriteNullableString(t.ErrorMessage)
		}
	}

	if version >= 5 {
		e.WriteInt32(t.NumPartitions)           // v5+
		e.WriteInt16(t.ReplicationFactor)       // v5+
		t.writeConfigs(e)                       // v5+
		e.WriteEmptyTaggedFields()              // v5+ topic tagged fields
	}
}

func (t *CreateTopicsResponseTopic) writeConfigs(e *Encoder) {
	e.WriteCompactArrayLen(len(t.Configs))

	for _, c := range t.Configs {
		e.WriteCompactString(c.Name)
		e.WriteCompactNullableString(c.Value)
		e.WriteBool(c.ReadOnly)
		e.WriteInt8(c.ConfigSource)
		e.WriteBool(c.IsSensitive)
		e.WriteEmptyTaggedFields()              // config tagged fields
	}
}

func (r *CreateTopicsResponse) writeTaggedFields(e *Encoder) {
	e.WriteEmptyTaggedFields()
}

// Encode - the recipe

func EncodeCreateTopicsResponse(e *Encoder, v int16, r *CreateTopicsResponse) {
	if v >= 2 {
		r.writeThrottleTime(e)                  // v2+
	}
	r.writeTopics(e, v)                         // v0+
	if v >= 5 {
		r.writeTaggedFields(e)                  // v5+
	}
}

package protocol

// ============================================================================
// Heartbeat (API Key 12)
// Supported versions: 0-4
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type HeartbeatRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID string // v3+
}

// Request Readers

func (r *HeartbeatRequest) readGroupID(d *Decoder) {
	r.GroupID, _ = d.ReadString()
}

func (r *HeartbeatRequest) readGenerationID(d *Decoder) {
	r.GenerationID, _ = d.ReadInt32()
}

func (r *HeartbeatRequest) readMemberID(d *Decoder) {
	r.MemberID, _ = d.ReadString()
}

func (r *HeartbeatRequest) readGroupInstanceID(d *Decoder) {
	s, _ := d.ReadNullableString()
	if s != nil {
		r.GroupInstanceID = *s
	}
}

// Decode - the recipe

func DecodeHeartbeatRequest(d *Decoder, v int16) (*HeartbeatRequest, error) {
	r := &HeartbeatRequest{}

	r.readGroupID(d)                            // v0+
	r.readGenerationID(d)                       // v0+
	r.readMemberID(d)                           // v0+
	if v >= 3 {
		r.readGroupInstanceID(d)                // v3+
	}

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type HeartbeatResponse struct {
	ThrottleTimeMs int32 // v1+
	ErrorCode      int16
}

// Response Writers

func (r *HeartbeatResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *HeartbeatResponse) writeErrorCode(e *Encoder) {
	e.WriteInt16(r.ErrorCode)
}

// Encode - the recipe

func EncodeHeartbeatResponse(e *Encoder, v int16, r *HeartbeatResponse) {
	if v >= 1 {
		r.writeThrottleTime(e)                  // v1+
	}
	r.writeErrorCode(e)                         // v0+
}

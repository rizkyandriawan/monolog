package protocol

// ============================================================================
// JoinGroup (API Key 11)
// Supported versions: 0-9
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type JoinGroupRequest struct {
	GroupID          string
	SessionTimeoutMs int32
	RebalanceTimeout int32  // v1+
	MemberID         string
	GroupInstanceID  string // v5+
	ProtocolType     string
	Protocols        []JoinGroupRequestProtocol
}

type JoinGroupRequestProtocol struct {
	Name     string
	Metadata []byte
}

// Request Readers

func (r *JoinGroupRequest) readGroupID(d *Decoder) {
	r.GroupID, _ = d.ReadString()
}

func (r *JoinGroupRequest) readSessionTimeout(d *Decoder) {
	r.SessionTimeoutMs, _ = d.ReadInt32()
}

func (r *JoinGroupRequest) readRebalanceTimeout(d *Decoder) {
	r.RebalanceTimeout, _ = d.ReadInt32()
}

func (r *JoinGroupRequest) readMemberID(d *Decoder) {
	r.MemberID, _ = d.ReadString()
}

func (r *JoinGroupRequest) readGroupInstanceID(d *Decoder) {
	s, _ := d.ReadNullableString()
	if s != nil {
		r.GroupInstanceID = *s
	}
}

func (r *JoinGroupRequest) readProtocolType(d *Decoder) {
	r.ProtocolType, _ = d.ReadString()
}

func (r *JoinGroupRequest) readProtocols(d *Decoder) {
	count, _ := d.ReadInt32()
	r.Protocols = make([]JoinGroupRequestProtocol, count)

	for i := range r.Protocols {
		r.Protocols[i].Name, _ = d.ReadString()
		r.Protocols[i].Metadata, _ = d.ReadBytes()
	}
}

// Decode - the recipe

func DecodeJoinGroupRequest(d *Decoder, v int16) (*JoinGroupRequest, error) {
	r := &JoinGroupRequest{}

	r.readGroupID(d)                            // v0+
	r.readSessionTimeout(d)                     // v0+
	if v >= 1 {
		r.readRebalanceTimeout(d)               // v1+
	}
	r.readMemberID(d)                           // v0+
	if v >= 5 {
		r.readGroupInstanceID(d)                // v5+
	}
	r.readProtocolType(d)                       // v0+
	r.readProtocols(d)                          // v0+

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type JoinGroupResponse struct {
	ThrottleTimeMs int32  // v2+
	ErrorCode      int16
	GenerationID   int32
	ProtocolType   string // v7+
	ProtocolName   string
	LeaderID       string
	MemberID       string
	Members        []JoinGroupResponseMember
}

type JoinGroupResponseMember struct {
	MemberID        string
	GroupInstanceID string // v5+
	Metadata        []byte
}

// Response Writers

func (r *JoinGroupResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *JoinGroupResponse) writeErrorCode(e *Encoder) {
	e.WriteInt16(r.ErrorCode)
}

func (r *JoinGroupResponse) writeGenerationID(e *Encoder) {
	e.WriteInt32(r.GenerationID)
}

func (r *JoinGroupResponse) writeProtocolType(e *Encoder) {
	e.WriteNullableString(&r.ProtocolType)
}

func (r *JoinGroupResponse) writeProtocolName(e *Encoder) {
	e.WriteString(r.ProtocolName)
}

func (r *JoinGroupResponse) writeLeaderAndMember(e *Encoder) {
	e.WriteString(r.LeaderID)
	e.WriteString(r.MemberID)
}

func (r *JoinGroupResponse) writeMembers(e *Encoder, version int16) {
	e.WriteArrayLen(len(r.Members))

	for _, m := range r.Members {
		m.writeTo(e, version)
	}
}

func (m *JoinGroupResponseMember) writeTo(e *Encoder, version int16) {
	e.WriteString(m.MemberID)
	if version >= 5 {
		e.WriteNullableString(&m.GroupInstanceID)   // v5+
	}
	e.WriteBytes(m.Metadata)
}

// Encode - the recipe

func EncodeJoinGroupResponse(e *Encoder, v int16, r *JoinGroupResponse) {
	if v >= 2 {
		r.writeThrottleTime(e)                  // v2+
	}
	r.writeErrorCode(e)                         // v0+
	r.writeGenerationID(e)                      // v0+
	if v >= 7 {
		r.writeProtocolType(e)                  // v7+
	}
	r.writeProtocolName(e)                      // v0+
	r.writeLeaderAndMember(e)                   // v0+
	r.writeMembers(e, v)                        // v0+
}

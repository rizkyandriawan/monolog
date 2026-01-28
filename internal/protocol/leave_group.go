package protocol

// ============================================================================
// LeaveGroup (API Key 13)
// Supported versions: 0-5
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type LeaveGroupRequest struct {
	GroupID  string
	MemberID string  // v0-v2
	Members  []LeaveGroupRequestMember // v3+
}

type LeaveGroupRequestMember struct {
	MemberID        string
	GroupInstanceID string
}

// Request Readers

func (r *LeaveGroupRequest) readGroupID(d *Decoder) {
	r.GroupID, _ = d.ReadString()
}

func (r *LeaveGroupRequest) readMemberID(d *Decoder) {
	r.MemberID, _ = d.ReadString()
}

func (r *LeaveGroupRequest) readMembers(d *Decoder) {
	count, _ := d.ReadInt32()
	r.Members = make([]LeaveGroupRequestMember, count)

	for i := range r.Members {
		r.Members[i].MemberID, _ = d.ReadString()
		s, _ := d.ReadNullableString()
		if s != nil {
			r.Members[i].GroupInstanceID = *s
		}
	}
}

// Decode - the recipe

func DecodeLeaveGroupRequest(d *Decoder, v int16) (*LeaveGroupRequest, error) {
	r := &LeaveGroupRequest{}

	r.readGroupID(d)                            // v0+
	if v <= 2 {
		r.readMemberID(d)                       // v0-v2
	} else {
		r.readMembers(d)                        // v3+
	}

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type LeaveGroupResponse struct {
	ThrottleTimeMs int32  // v1+
	ErrorCode      int16
	Members        []LeaveGroupResponseMember // v3+
}

type LeaveGroupResponseMember struct {
	MemberID        string
	GroupInstanceID string
	ErrorCode       int16
}

// Response Writers

func (r *LeaveGroupResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *LeaveGroupResponse) writeErrorCode(e *Encoder) {
	e.WriteInt16(r.ErrorCode)
}

func (r *LeaveGroupResponse) writeMembers(e *Encoder) {
	e.WriteArrayLen(len(r.Members))

	for _, m := range r.Members {
		e.WriteString(m.MemberID)
		e.WriteNullableString(&m.GroupInstanceID)
		e.WriteInt16(m.ErrorCode)
	}
}

// Encode - the recipe

func EncodeLeaveGroupResponse(e *Encoder, v int16, r *LeaveGroupResponse) {
	if v >= 1 {
		r.writeThrottleTime(e)                  // v1+
	}
	r.writeErrorCode(e)                         // v0+
	if v >= 3 {
		r.writeMembers(e)                       // v3+
	}
}

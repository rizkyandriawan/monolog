package protocol

// ============================================================================
// SyncGroup (API Key 14)
// Supported versions: 0-5
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type SyncGroupRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID string // v3+
	ProtocolType    string // v5+
	ProtocolName    string // v5+
	Assignments     []SyncGroupRequestAssignment
}

type SyncGroupRequestAssignment struct {
	MemberID   string
	Assignment []byte
}

// Request Readers

func (r *SyncGroupRequest) readGroupID(d *Decoder) {
	r.GroupID, _ = d.ReadString()
}

func (r *SyncGroupRequest) readGenerationID(d *Decoder) {
	r.GenerationID, _ = d.ReadInt32()
}

func (r *SyncGroupRequest) readMemberID(d *Decoder) {
	r.MemberID, _ = d.ReadString()
}

func (r *SyncGroupRequest) readGroupInstanceID(d *Decoder) {
	s, _ := d.ReadNullableString()
	if s != nil {
		r.GroupInstanceID = *s
	}
}

func (r *SyncGroupRequest) readProtocolInfo(d *Decoder) {
	s, _ := d.ReadNullableString()
	if s != nil {
		r.ProtocolType = *s
	}
	s, _ = d.ReadNullableString()
	if s != nil {
		r.ProtocolName = *s
	}
}

func (r *SyncGroupRequest) readAssignments(d *Decoder) {
	count, _ := d.ReadInt32()
	r.Assignments = make([]SyncGroupRequestAssignment, count)

	for i := range r.Assignments {
		r.Assignments[i].MemberID, _ = d.ReadString()
		r.Assignments[i].Assignment, _ = d.ReadBytes()
	}
}

// Decode - the recipe

func DecodeSyncGroupRequest(d *Decoder, v int16) (*SyncGroupRequest, error) {
	r := &SyncGroupRequest{}

	r.readGroupID(d)                            // v0+
	r.readGenerationID(d)                       // v0+
	r.readMemberID(d)                           // v0+
	if v >= 3 {
		r.readGroupInstanceID(d)                // v3+
	}
	if v >= 5 {
		r.readProtocolInfo(d)                   // v5+
	}
	r.readAssignments(d)                        // v0+

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type SyncGroupResponse struct {
	ThrottleTimeMs int32  // v1+
	ErrorCode      int16
	ProtocolType   string // v5+
	ProtocolName   string // v5+
	Assignment     []byte
}

// Response Writers

func (r *SyncGroupResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *SyncGroupResponse) writeErrorCode(e *Encoder) {
	e.WriteInt16(r.ErrorCode)
}

func (r *SyncGroupResponse) writeProtocolInfo(e *Encoder) {
	e.WriteNullableString(&r.ProtocolType)
	e.WriteNullableString(&r.ProtocolName)
}

func (r *SyncGroupResponse) writeAssignment(e *Encoder) {
	e.WriteBytes(r.Assignment)
}

// Encode - the recipe

func EncodeSyncGroupResponse(e *Encoder, v int16, r *SyncGroupResponse) {
	if v >= 1 {
		r.writeThrottleTime(e)                  // v1+
	}
	r.writeErrorCode(e)                         // v0+
	if v >= 5 {
		r.writeProtocolInfo(e)                  // v5+
	}
	r.writeAssignment(e)                        // v0+
}

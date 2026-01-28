package protocol

// ============================================================================
// FindCoordinator (API Key 10)
// Supported versions: 0-3
// ============================================================================

// Coordinator key types
const (
	CoordinatorKeyTypeGroup       int8 = 0
	CoordinatorKeyTypeTransaction int8 = 1
)

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type FindCoordinatorRequest struct {
	Key     string
	KeyType int8 // v1+: 0 = GROUP, 1 = TRANSACTION
}

// Request Readers

func (r *FindCoordinatorRequest) readKey(d *Decoder) {
	r.Key, _ = d.ReadString()
}

func (r *FindCoordinatorRequest) readKeyCompact(d *Decoder) {
	r.Key, _ = d.ReadCompactString()
}

func (r *FindCoordinatorRequest) readKeyType(d *Decoder) {
	r.KeyType, _ = d.ReadInt8()
}

func (r *FindCoordinatorRequest) readTaggedFields(d *Decoder) {
	d.ReadUVarInt()
}

// Decode - the recipe

func DecodeFindCoordinatorRequest(d *Decoder, v int16) (*FindCoordinatorRequest, error) {
	r := &FindCoordinatorRequest{}

	if v >= 3 {
		r.readKeyCompact(d)                     // v3+ compact
		r.readKeyType(d)                        // v3+
		r.readTaggedFields(d)                   // v3+
	} else {
		r.readKey(d)                            // v0-v2
		if v >= 1 {
			r.readKeyType(d)                    // v1+
		} else {
			r.KeyType = CoordinatorKeyTypeGroup // v0 default
		}
	}

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type FindCoordinatorResponse struct {
	ThrottleTimeMs int32   // v1+
	ErrorCode      int16
	ErrorMessage   *string // v1+
	NodeID         int32
	Host           string
	Port           int32
}

// Response Writers

func (r *FindCoordinatorResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

func (r *FindCoordinatorResponse) writeErrorCode(e *Encoder) {
	e.WriteInt16(r.ErrorCode)
}

func (r *FindCoordinatorResponse) writeErrorMessage(e *Encoder) {
	e.WriteNullableString(r.ErrorMessage)
}

func (r *FindCoordinatorResponse) writeErrorMessageCompact(e *Encoder) {
	e.WriteCompactNullableString(r.ErrorMessage)
}

func (r *FindCoordinatorResponse) writeCoordinator(e *Encoder) {
	e.WriteInt32(r.NodeID)
	e.WriteString(r.Host)
	e.WriteInt32(r.Port)
}

func (r *FindCoordinatorResponse) writeCoordinatorCompact(e *Encoder) {
	e.WriteInt32(r.NodeID)
	e.WriteCompactString(r.Host)
	e.WriteInt32(r.Port)
}

func (r *FindCoordinatorResponse) writeTaggedFields(e *Encoder) {
	e.WriteEmptyTaggedFields()
}

// Encode - the recipe

func EncodeFindCoordinatorResponse(e *Encoder, v int16, r *FindCoordinatorResponse) {
	if v >= 1 {
		r.writeThrottleTime(e)                  // v1+
	}
	r.writeErrorCode(e)                         // v0+

	if v >= 3 {
		r.writeErrorMessageCompact(e)           // v3+ compact
		r.writeCoordinatorCompact(e)            // v3+ compact
		r.writeTaggedFields(e)                  // v3+
	} else {
		if v >= 1 {
			r.writeErrorMessage(e)              // v1+
		}
		r.writeCoordinator(e)                   // v0+
	}
}

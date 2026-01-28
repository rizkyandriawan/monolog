package protocol

// ============================================================================
// ApiVersions (API Key 18)
// Supported versions: 0-3
// ============================================================================

// ----------------------------------------------------------------------------
// Request
// ----------------------------------------------------------------------------

type ApiVersionsRequest struct {
	ClientSoftwareName    string // v3+
	ClientSoftwareVersion string // v3+
}

// Request Readers

func (r *ApiVersionsRequest) readClientInfo(d *Decoder) {
	r.ClientSoftwareName, _ = d.ReadCompactString()
	r.ClientSoftwareVersion, _ = d.ReadCompactString()
	d.ReadUVarInt() // tagged fields
}

// Decode - the recipe

func DecodeApiVersionsRequest(d *Decoder, v int16) (*ApiVersionsRequest, error) {
	r := &ApiVersionsRequest{}

	// v0-v2: empty request body
	if v >= 3 {
		r.readClientInfo(d)                     // v3+
	}

	return r, nil
}

// ----------------------------------------------------------------------------
// Response
// ----------------------------------------------------------------------------

type ApiVersionsResponse struct {
	ErrorCode      int16
	ApiVersions    []ApiVersion
	ThrottleTimeMs int32 // v1+
}

// Response Writers

func (r *ApiVersionsResponse) writeErrorCode(e *Encoder) {
	e.WriteInt16(r.ErrorCode)
}

func (r *ApiVersionsResponse) writeApiVersions(e *Encoder) {
	e.WriteArrayLen(len(r.ApiVersions))

	for _, v := range r.ApiVersions {
		e.WriteInt16(v.APIKey)
		e.WriteInt16(v.MinVersion)
		e.WriteInt16(v.MaxVersion)
	}
}

func (r *ApiVersionsResponse) writeApiVersionsCompact(e *Encoder) {
	e.WriteCompactArrayLen(len(r.ApiVersions))

	for _, v := range r.ApiVersions {
		e.WriteInt16(v.APIKey)
		e.WriteInt16(v.MinVersion)
		e.WriteInt16(v.MaxVersion)
		e.WriteEmptyTaggedFields()              // per-entry tagged fields
	}
}

func (r *ApiVersionsResponse) writeThrottleTime(e *Encoder) {
	e.WriteInt32(r.ThrottleTimeMs)
}

// Encode - the recipe

func EncodeApiVersionsResponse(e *Encoder, v int16, r *ApiVersionsResponse) {
	r.writeErrorCode(e)                         // v0+

	if v >= 3 {
		r.writeApiVersionsCompact(e)            // v3+ compact
		r.writeThrottleTime(e)                  // v3+ (moved after api_keys)
		e.WriteEmptyTaggedFields()              // v3+ response tagged fields
	} else {
		r.writeApiVersions(e)                   // v0-v2 regular
		if v >= 1 {
			r.writeThrottleTime(e)              // v1+
		}
	}
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

// DefaultApiVersions returns the list of supported API versions
func DefaultApiVersions() []ApiVersion {
	return []ApiVersion{
		{APIKey: APIKeyProduce, MinVersion: 0, MaxVersion: 8},
		{APIKey: APIKeyFetch, MinVersion: 0, MaxVersion: 11},
		{APIKey: APIKeyListOffsets, MinVersion: 0, MaxVersion: 5},
		{APIKey: APIKeyMetadata, MinVersion: 0, MaxVersion: 8},
		{APIKey: APIKeyOffsetCommit, MinVersion: 0, MaxVersion: 8},
		{APIKey: APIKeyOffsetFetch, MinVersion: 0, MaxVersion: 5},
		{APIKey: APIKeyFindCoordinator, MinVersion: 0, MaxVersion: 3},
		{APIKey: APIKeyJoinGroup, MinVersion: 0, MaxVersion: 5},
		{APIKey: APIKeyHeartbeat, MinVersion: 0, MaxVersion: 3},
		{APIKey: APIKeyLeaveGroup, MinVersion: 0, MaxVersion: 3},
		{APIKey: APIKeySyncGroup, MinVersion: 0, MaxVersion: 3},
		{APIKey: APIKeySaslHandshake, MinVersion: 0, MaxVersion: 1},
		{APIKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 3},
		{APIKey: APIKeyCreateTopics, MinVersion: 0, MaxVersion: 5},
		{APIKey: APIKeySaslAuthenticate, MinVersion: 0, MaxVersion: 2},
	}
}

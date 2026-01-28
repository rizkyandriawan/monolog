package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

var (
	ErrInsufficientData = errors.New("insufficient data")
	ErrInvalidData      = errors.New("invalid data")
)

// Decoder reads Kafka protocol data
type Decoder struct {
	r   io.Reader
	buf []byte
}

// NewDecoder creates a new decoder
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r, buf: make([]byte, 8)}
}

func (d *Decoder) ReadInt8() (int8, error) {
	if _, err := io.ReadFull(d.r, d.buf[:1]); err != nil {
		return 0, err
	}
	return int8(d.buf[0]), nil
}

func (d *Decoder) ReadInt16() (int16, error) {
	if _, err := io.ReadFull(d.r, d.buf[:2]); err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(d.buf[:2])), nil
}

func (d *Decoder) ReadInt32() (int32, error) {
	if _, err := io.ReadFull(d.r, d.buf[:4]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(d.buf[:4])), nil
}

func (d *Decoder) ReadInt64() (int64, error) {
	if _, err := io.ReadFull(d.r, d.buf[:8]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(d.buf[:8])), nil
}

func (d *Decoder) ReadVarInt() (int64, error) {
	var value int64
	var shift uint
	for {
		b, err := d.ReadInt8()
		if err != nil {
			return 0, err
		}
		value |= int64(b&0x7F) << shift
		if int8(b)&-128 == 0 { // Check high bit
			break
		}
		shift += 7
	}
	// Zigzag decode
	return (value >> 1) ^ -(value & 1), nil
}

func (d *Decoder) ReadUVarInt() (uint64, error) {
	var value uint64
	var shift uint
	for {
		b, err := d.ReadInt8()
		if err != nil {
			return 0, err
		}
		value |= uint64(b&0x7F) << shift
		if int8(b)&-128 == 0 { // Check high bit
			break
		}
		shift += 7
	}
	return value, nil
}

func (d *Decoder) ReadString() (string, error) {
	length, err := d.ReadInt16()
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", nil // null string
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(d.r, data); err != nil {
		return "", err
	}
	return string(data), nil
}

func (d *Decoder) ReadNullableString() (*string, error) {
	length, err := d.ReadInt16()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(d.r, data); err != nil {
		return nil, err
	}
	s := string(data)
	return &s, nil
}

func (d *Decoder) ReadCompactString() (string, error) {
	length, err := d.ReadUVarInt()
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	data := make([]byte, length-1)
	if _, err := io.ReadFull(d.r, data); err != nil {
		return "", err
	}
	return string(data), nil
}

func (d *Decoder) ReadCompactNullableString() (*string, error) {
	length, err := d.ReadUVarInt()
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	data := make([]byte, length-1)
	if _, err := io.ReadFull(d.r, data); err != nil {
		return nil, err
	}
	s := string(data)
	return &s, nil
}

func (d *Decoder) ReadBytes() ([]byte, error) {
	length, err := d.ReadInt32()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(d.r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (d *Decoder) ReadCompactBytes() ([]byte, error) {
	length, err := d.ReadUVarInt()
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	data := make([]byte, length-1)
	if _, err := io.ReadFull(d.r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (d *Decoder) ReadRaw(n int) ([]byte, error) {
	data := make([]byte, n)
	if _, err := io.ReadFull(d.r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (d *Decoder) ReadBool() (bool, error) {
	b, err := d.ReadInt8()
	return b != 0, err
}

// Encoder writes Kafka protocol data
type Encoder struct {
	buf []byte
}

// NewEncoder creates a new encoder
func NewEncoder() *Encoder {
	return &Encoder{buf: make([]byte, 0, 1024)}
}

func (e *Encoder) Bytes() []byte {
	return e.buf
}

func (e *Encoder) Len() int {
	return len(e.buf)
}

func (e *Encoder) Reset() {
	e.buf = e.buf[:0]
}

func (e *Encoder) WriteInt8(v int8) {
	e.buf = append(e.buf, byte(v))
}

func (e *Encoder) WriteInt16(v int16) {
	e.buf = append(e.buf, byte(v>>8), byte(v))
}

func (e *Encoder) WriteInt32(v int32) {
	e.buf = append(e.buf,
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (e *Encoder) WriteInt64(v int64) {
	e.buf = append(e.buf,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (e *Encoder) WriteVarInt(v int64) {
	// Zigzag encode
	uv := uint64((v << 1) ^ (v >> 63))
	e.WriteUVarInt(uv)
}

func (e *Encoder) WriteUVarInt(v uint64) {
	for v >= 0x80 {
		e.buf = append(e.buf, byte(v)|0x80)
		v >>= 7
	}
	e.buf = append(e.buf, byte(v))
}

func (e *Encoder) WriteString(s string) {
	e.WriteInt16(int16(len(s)))
	e.buf = append(e.buf, s...)
}

func (e *Encoder) WriteNullableString(s *string) {
	if s == nil {
		e.WriteInt16(-1)
		return
	}
	e.WriteString(*s)
}

func (e *Encoder) WriteCompactString(s string) {
	e.WriteUVarInt(uint64(len(s) + 1))
	e.buf = append(e.buf, s...)
}

func (e *Encoder) WriteCompactNullableString(s *string) {
	if s == nil {
		e.WriteUVarInt(0)
		return
	}
	e.WriteCompactString(*s)
}

func (e *Encoder) WriteBytes(data []byte) {
	if data == nil {
		e.WriteInt32(-1)
		return
	}
	e.WriteInt32(int32(len(data)))
	e.buf = append(e.buf, data...)
}

func (e *Encoder) WriteCompactBytes(data []byte) {
	if data == nil {
		e.WriteUVarInt(0)
		return
	}
	e.WriteUVarInt(uint64(len(data) + 1))
	e.buf = append(e.buf, data...)
}

func (e *Encoder) WriteRaw(data []byte) {
	e.buf = append(e.buf, data...)
}

func (e *Encoder) WriteBool(v bool) {
	if v {
		e.WriteInt8(1)
	} else {
		e.WriteInt8(0)
	}
}

// WriteCompactArrayLen writes the length for a compact array
func (e *Encoder) WriteCompactArrayLen(n int) {
	e.WriteUVarInt(uint64(n + 1))
}

// WriteArrayLen writes the length for a regular array
func (e *Encoder) WriteArrayLen(n int) {
	e.WriteInt32(int32(n))
}

// WriteEmptyTaggedFields writes an empty tagged fields section
func (e *Encoder) WriteEmptyTaggedFields() {
	e.WriteUVarInt(0)
}

// isFlexibleVersion returns true if the given API and version uses flexible encoding
func isFlexibleVersion(apiKey, apiVersion int16) bool {
	switch apiKey {
	case APIKeyProduce:
		return apiVersion >= 9
	case APIKeyFetch:
		return apiVersion >= 12
	case APIKeyListOffsets:
		return apiVersion >= 6
	case APIKeyMetadata:
		return apiVersion >= 9
	case APIKeyOffsetFetch:
		return apiVersion >= 6
	case APIKeyFindCoordinator:
		return apiVersion >= 3
	case APIKeyJoinGroup:
		return apiVersion >= 6
	case APIKeyHeartbeat:
		return apiVersion >= 4
	case APIKeyLeaveGroup:
		return apiVersion >= 4
	case APIKeySyncGroup:
		return apiVersion >= 4
	case APIKeyApiVersions:
		return apiVersion >= 3
	case APIKeyCreateTopics:
		return apiVersion >= 5
	default:
		return false
	}
}

// ReadHeader reads a request header from the decoder (auto-detects flexible version)
func (d *Decoder) ReadHeader() (RequestHeader, error) {
	var h RequestHeader
	var err error

	h.APIKey, err = d.ReadInt16()
	if err != nil {
		return h, err
	}

	h.APIVersion, err = d.ReadInt16()
	if err != nil {
		return h, err
	}

	h.CorrelationID, err = d.ReadInt32()
	if err != nil {
		return h, err
	}

	// Check if this is a flexible version request
	if isFlexibleVersion(h.APIKey, h.APIVersion) {
		h.ClientID, err = d.ReadCompactString()
		if err != nil {
			return h, err
		}
		// Skip tagged fields
		_, err = d.ReadUVarInt()
		if err != nil {
			return h, err
		}
	} else {
		h.ClientID, err = d.ReadString()
		if err != nil {
			return h, err
		}
	}

	return h, nil
}

// ReadHeaderV2 reads a v2 request header (with tagged fields)
func (d *Decoder) ReadHeaderV2() (RequestHeader, error) {
	var h RequestHeader
	var err error

	h.APIKey, err = d.ReadInt16()
	if err != nil {
		return h, err
	}

	h.APIVersion, err = d.ReadInt16()
	if err != nil {
		return h, err
	}

	h.CorrelationID, err = d.ReadInt32()
	if err != nil {
		return h, err
	}

	h.ClientID, err = d.ReadCompactString()
	if err != nil {
		return h, err
	}

	// Skip tagged fields
	_, err = d.ReadUVarInt()
	if err != nil {
		return h, err
	}

	return h, nil
}

// WriteResponseHeader writes a response header
func (e *Encoder) WriteResponseHeader(correlationID int32) {
	e.WriteInt32(correlationID)
}

// WriteResponseHeaderV1 writes a v1 response header (with tagged fields for flexible versions)
func (e *Encoder) WriteResponseHeaderV1(correlationID int32) {
	e.WriteInt32(correlationID)
	e.WriteUVarInt(0) // tagged fields
}

// IsFlexibleVersion exports the flexible version check
func IsFlexibleVersion(apiKey, apiVersion int16) bool {
	return isFlexibleVersion(apiKey, apiVersion)
}

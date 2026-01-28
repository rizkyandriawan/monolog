package protocol

// API Keys
const (
	APIKeyProduce          int16 = 0
	APIKeyFetch            int16 = 1
	APIKeyListOffsets      int16 = 2
	APIKeyMetadata         int16 = 3
	APIKeyOffsetCommit     int16 = 8
	APIKeyOffsetFetch      int16 = 9
	APIKeyFindCoordinator  int16 = 10
	APIKeyJoinGroup        int16 = 11
	APIKeyHeartbeat        int16 = 12
	APIKeyLeaveGroup       int16 = 13
	APIKeySyncGroup        int16 = 14
	APIKeySaslHandshake    int16 = 17
	APIKeyApiVersions      int16 = 18
	APIKeyCreateTopics     int16 = 19
	APIKeySaslAuthenticate int16 = 36
)

// Error Codes
const (
	ErrNone                        int16 = 0
	ErrOffsetOutOfRange            int16 = 1
	ErrUnknownTopicOrPartition     int16 = 3
	ErrInvalidMessage              int16 = 4
	ErrLeaderNotAvailable          int16 = 5
	ErrMessageTooLarge             int16 = 10
	ErrCoordinatorNotAvailable     int16 = 15
	ErrNotCoordinator              int16 = 16
	ErrIllegalGeneration           int16 = 22
	ErrInconsistentGroupProtocol   int16 = 23
	ErrUnknownMemberID             int16 = 25
	ErrInvalidSessionTimeout       int16 = 26
	ErrRebalanceInProgress         int16 = 27
	ErrUnsupportedVersion          int16 = 35
	ErrTopicAlreadyExists          int16 = 36
	ErrInvalidTopicException       int16 = 17
	ErrSaslAuthenticationFailed    int16 = 31
	ErrUnsupportedSaslMechanism    int16 = 33
)

// Compression Codecs
const (
	CompressionNone   int8 = 0
	CompressionGzip   int8 = 1
	CompressionSnappy int8 = 2
	CompressionLz4    int8 = 3
	CompressionZstd   int8 = 4
)

// RequestHeader represents the common request header
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

// ResponseHeader represents the common response header
type ResponseHeader struct {
	CorrelationID int32
}

// ApiVersion represents a supported API version range
type ApiVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

// SaslHandshakeRequest represents a SASL handshake request
type SaslHandshakeRequest struct {
	Mechanism string
}

// SaslHandshakeResponse represents a SASL handshake response
type SaslHandshakeResponse struct {
	ErrorCode  int16
	Mechanisms []string
}

// SaslAuthenticateRequest represents a SASL authenticate request
type SaslAuthenticateRequest struct {
	AuthBytes []byte
}

// SaslAuthenticateResponse represents a SASL authenticate response
type SaslAuthenticateResponse struct {
	ErrorCode    int16
	ErrorMessage string
	AuthBytes    []byte
}

// Record represents a single Kafka record (message)
type Record struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []RecordHeader
}

// RecordHeader represents a record header
type RecordHeader struct {
	Key   string
	Value []byte
}

// RecordBatch represents a batch of records
type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  int32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
	RawRecords           []byte // For passthrough
	Codec                int8   // Compression codec
}

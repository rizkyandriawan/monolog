package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server    ServerConfig    `yaml:"server"`
	Storage   StorageConfig   `yaml:"storage"`
	Topics    TopicsConfig    `yaml:"topics"`
	Limits    LimitsConfig    `yaml:"limits"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Retention RetentionConfig `yaml:"retention"`
	Groups    GroupsConfig    `yaml:"groups"`
	Security  SecurityConfig  `yaml:"security"`
	Logging   LoggingConfig   `yaml:"logging"`
}

type ServerConfig struct {
	KafkaAddr string `yaml:"kafka_addr"`
	HTTPAddr  string `yaml:"http_addr"`
}

type StorageConfig struct {
	Backend    string        `yaml:"backend"` // "badger" or "sqlite"
	DataDir    string        `yaml:"data_dir"`
	SyncWrites bool          `yaml:"sync_writes"`
	GCInterval time.Duration `yaml:"gc_interval"`
}

type TopicsConfig struct {
	AutoCreate bool `yaml:"auto_create"`
}

type LimitsConfig struct {
	MaxConnections  int `yaml:"max_connections"`
	MaxMessageSize  int `yaml:"max_message_size"`
	MaxFetchBytes   int `yaml:"max_fetch_bytes"`
	MaxTopics       int `yaml:"max_topics"`
}

type SchedulerConfig struct {
	TickInterval time.Duration `yaml:"tick_interval"`
}

type RetentionConfig struct {
	Enabled       bool          `yaml:"enabled"`
	MaxAge        time.Duration `yaml:"max_age"`
	CheckInterval time.Duration `yaml:"check_interval"`
}

type GroupsConfig struct {
	SessionTimeout    time.Duration `yaml:"session_timeout"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
}

type SecurityConfig struct {
	Enabled bool      `yaml:"enabled"`
	Token   string    `yaml:"token"`
	TLS     TLSConfig `yaml:"tls"`
}

type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default returns a Config with sensible defaults
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			KafkaAddr: ":9092",
			HTTPAddr:  ":8080",
		},
		Storage: StorageConfig{
			Backend:    "sqlite", // default to sqlite
			DataDir:    "./data",
			SyncWrites: false,
			GCInterval: 5 * time.Minute,
		},
		Topics: TopicsConfig{
			AutoCreate: true,
		},
		Limits: LimitsConfig{
			MaxConnections: 100,
			MaxMessageSize: 1 << 20, // 1MB
			MaxFetchBytes:  10 << 20, // 10MB
			MaxTopics:      100,
		},
		Scheduler: SchedulerConfig{
			TickInterval: 100 * time.Millisecond,
		},
		Retention: RetentionConfig{
			Enabled:       true,
			MaxAge:        24 * time.Hour,
			CheckInterval: 1 * time.Minute,
		},
		Groups: GroupsConfig{
			SessionTimeout:    30 * time.Second,
			HeartbeatInterval: 3 * time.Second,
		},
		Security: SecurityConfig{
			Enabled: false,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
		},
	}
}

// Load loads config from file, environment, with defaults
func Load(path string) (*Config, error) {
	cfg := Default()

	// Load from file if provided
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, err
		}
	}

	// Override from environment
	cfg.loadFromEnv()

	return cfg, nil
}

func (c *Config) loadFromEnv() {
	if v := os.Getenv("MONOLOG_KAFKA_ADDR"); v != "" {
		c.Server.KafkaAddr = v
	}
	if v := os.Getenv("MONOLOG_HTTP_ADDR"); v != "" {
		c.Server.HTTPAddr = v
	}
	if v := os.Getenv("MONOLOG_DATA_DIR"); v != "" {
		c.Storage.DataDir = v
	}
	if v := os.Getenv("MONOLOG_STORAGE_BACKEND"); v != "" {
		c.Storage.Backend = v
	}
	if v := os.Getenv("MONOLOG_LOG_LEVEL"); v != "" {
		c.Logging.Level = v
	}
	if v := os.Getenv("MONOLOG_AUTH_TOKEN"); v != "" {
		c.Security.Token = v
		c.Security.Enabled = true
	}
}

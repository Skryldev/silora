package miniostorage

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Skryldev/silora/internal/storage"
)

const (
	minPartSize = int64(5 * 1024 * 1024)     // S3 minimum multipart part size
	maxPartSize = int64(5 * 1024 * 1024 * 1024) // 5 GiB
	maxParts    = 10000

	defaultMaxMultipartMemory = int64(4 * 1024 * 1024 * 1024)
)

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
	Secure    bool
	PathStyle bool
	UserAgent string

	Transport TransportConfig
	Retry     RetryConfig
	Multipart MultipartConfig
	Health    HealthConfig
	Integrity IntegrityConfig

	Logger  *slog.Logger
	Metrics storage.Metrics
	Tracer  storage.Tracer
}

type TransportConfig struct {
	DialTimeout            time.Duration
	TLSHandshakeTimeout   time.Duration
	IdleConnTimeout        time.Duration
	ResponseHeaderTimeout time.Duration
	ExpectContinueTimeout time.Duration

	MaxIdleConns        int
	MaxIdleConnsPerHost int
	MaxConnsPerHost     int

	InsecureSkipVerify bool
	DisableCompression bool
	TLSMinVersion      uint16
}

type RetryConfig struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

type MultipartConfig struct {
	PartSize     int64
	Concurrency  int
	AbortTimeout time.Duration
	MaxMemory    int64
}

type HealthConfig struct {
	Bucket  string
	Timeout time.Duration
}

type IntegrityConfig struct {
	RequirePayloadSHA256 bool
}

func DefaultConfig() Config {
	return Config{
		Endpoint:  "localhost:9000",
		Region:    "us-east-1",
		Secure:    false,
		PathStyle: true,
		UserAgent: "storagecore",

		Transport: TransportConfig{
			DialTimeout:            5 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			IdleConnTimeout:        90 * time.Second,
			ResponseHeaderTimeout: 120 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			MaxIdleConns:           256,
			MaxIdleConnsPerHost:    256,
			MaxConnsPerHost:        0,
			DisableCompression:     true,
			TLSMinVersion:          0, // normalized to TLS 1.2
		},

		Retry: RetryConfig{
			MaxAttempts:    3,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     2 * time.Second,
			Multiplier:     2.0,
		},

		Multipart: MultipartConfig{
			PartSize:     16 * 1024 * 1024,
			Concurrency:  4,
			AbortTimeout: 10 * time.Second,
			MaxMemory:    defaultMaxMultipartMemory,
		},

		Health: HealthConfig{
			Timeout: 2 * time.Second,
		},

		Integrity: IntegrityConfig{
			RequirePayloadSHA256: false,
		},

		Logger:  slog.Default(),
		Metrics: storage.NewNoopMetrics(),
		Tracer:  storage.NewNoopTracer(),
	}
}

func ConfigFromEnv() Config {
	cfg := DefaultConfig()

	if v := os.Getenv("STORAGE_MINIO_ENDPOINT"); v != "" {
		cfg.Endpoint = v
	}
	if v := os.Getenv("STORAGE_MINIO_ACCESS_KEY"); v != "" {
		cfg.AccessKey = v
	}
	if v := os.Getenv("STORAGE_MINIO_SECRET_KEY"); v != "" {
		cfg.SecretKey = v
	}
	if v := os.Getenv("STORAGE_MINIO_REGION"); v != "" {
		cfg.Region = v
	}
	if v := os.Getenv("STORAGE_MINIO_SECURE"); v != "" {
		if secure, err := strconv.ParseBool(v); err == nil {
			cfg.Secure = secure
		}
	}
	if v := os.Getenv("STORAGE_MINIO_HEALTH_BUCKET"); v != "" {
		cfg.Health.Bucket = v
	}

	return cfg
}

func (c *Config) Normalize() {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Metrics == nil {
		c.Metrics = storage.NewNoopMetrics()
	}
	if c.Tracer == nil {
		c.Tracer = storage.NewNoopTracer()
	}
	if c.UserAgent == "" {
		c.UserAgent = "storagecore"
	}

	if c.Transport.DialTimeout <= 0 {
		c.Transport.DialTimeout = 5 * time.Second
	}
	if c.Transport.TLSHandshakeTimeout <= 0 {
		c.Transport.TLSHandshakeTimeout = 10 * time.Second
	}
	if c.Transport.IdleConnTimeout <= 0 {
		c.Transport.IdleConnTimeout = 90 * time.Second
	}
	if c.Transport.ResponseHeaderTimeout <= 0 {
		c.Transport.ResponseHeaderTimeout = 120 * time.Second
	}
	if c.Transport.ExpectContinueTimeout <= 0 {
		c.Transport.ExpectContinueTimeout = 1 * time.Second
	}
	if c.Transport.MaxIdleConns <= 0 {
		c.Transport.MaxIdleConns = 256
	}
	if c.Transport.MaxIdleConnsPerHost <= 0 {
		c.Transport.MaxIdleConnsPerHost = c.Transport.MaxIdleConns
	}

	if c.Retry.MaxAttempts <= 0 {
		c.Retry.MaxAttempts = 3
	}
	if c.Retry.InitialBackoff <= 0 {
		c.Retry.InitialBackoff = 100 * time.Millisecond
	}
	if c.Retry.MaxBackoff <= 0 {
		c.Retry.MaxBackoff = 2 * time.Second
	}
	if c.Retry.Multiplier <= 1 {
		c.Retry.Multiplier = 2
	}

	if c.Multipart.PartSize <= 0 {
		c.Multipart.PartSize = 16 * 1024 * 1024
	}
	if c.Multipart.Concurrency <= 0 {
		c.Multipart.Concurrency = 4
	}
	if c.Multipart.AbortTimeout <= 0 {
		c.Multipart.AbortTimeout = 10 * time.Second
	}
	if c.Multipart.MaxMemory <= 0 {
		c.Multipart.MaxMemory = defaultMaxMultipartMemory
	}

	if c.Health.Timeout <= 0 {
		c.Health.Timeout = 2 * time.Second
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Endpoint) == "" {
		return errors.New("miniostorage: endpoint is required")
	}

	if strings.Contains(c.Endpoint, "://") {
		return errors.New("miniostorage: endpoint must be host[:port] without scheme; use Secure=true/false")
	}

	if strings.TrimSpace(c.AccessKey) == "" {
		return errors.New("miniostorage: access key is required")
	}

	if strings.TrimSpace(c.SecretKey) == "" {
		return errors.New("miniostorage: secret key is required")
	}

	if c.Multipart.PartSize < minPartSize {
		return fmt.Errorf("miniostorage: multipart part size must be at least %d bytes", minPartSize)
	}
	if c.Multipart.PartSize > maxPartSize {
		return fmt.Errorf("miniostorage: multipart part size must be at most %d bytes", maxPartSize)
	}
	if c.Multipart.Concurrency < 1 || c.Multipart.Concurrency > 1000 {
		return errors.New("miniostorage: multipart concurrency must be between 1 and 1000")
	}

	if c.Multipart.MaxMemory > 0 {
		estimated := c.Multipart.PartSize * int64(c.Multipart.Concurrency)
		if estimated > c.Multipart.MaxMemory {
			return fmt.Errorf(
				"miniostorage: multipart memory budget exceeded: part_size=%d concurrency=%d estimated=%d max=%d",
				c.Multipart.PartSize,
				c.Multipart.Concurrency,
				estimated,
				c.Multipart.MaxMemory,
			)
		}
	}

	if c.Retry.MaxAttempts < 1 || c.Retry.MaxAttempts > 10 {
		return errors.New("miniostorage: retry max attempts must be between 1 and 10")
	}

	if c.Health.Bucket != "" {
		if err := storage.ValidateBucket(c.Health.Bucket); err != nil {
			return fmt.Errorf("miniostorage: invalid health bucket: %w", err)
		}
	}

	return nil
}

// Redacted returns a copy safe for logging.
func (c Config) Redacted() Config {
	redacted := c
	redacted.SecretKey = "REDACTED"
	if redacted.AccessKey != "" {
		redacted.AccessKey = "REDACTED"
	}
	return redacted
}
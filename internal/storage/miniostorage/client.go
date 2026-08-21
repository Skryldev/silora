package miniostorage

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func newMinioClient(cfg Config) (*minio.Client, *http.Transport, error) {
	transport := newHTTPTransport(cfg)

	creds := credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")

	opts := &minio.Options{
		Creds:     creds,
		Secure:    cfg.Secure,
		Region:    cfg.Region,
		Transport: transport,
	}

	if cfg.PathStyle {
		opts.BucketLookup = minio.BucketLookupPath
	}

	client, err := minio.New(cfg.Endpoint, opts)
	if err != nil {
		return nil, transport, err
	}

	client.SetAppInfo(cfg.UserAgent, "phase1")

	return client, transport, nil
}

func newHTTPTransport(cfg Config) *http.Transport {
	tlsMinVersion := cfg.Transport.TLSMinVersion
	if tlsMinVersion == 0 {
		tlsMinVersion = tls.VersionTLS12
	}

	tlsConfig := &tls.Config{
		MinVersion:         tlsMinVersion,
		InsecureSkipVerify: cfg.Transport.InsecureSkipVerify, //nolint:gosec // explicit opt-in, defaults false
	}

	dialer := &net.Dialer{
		Timeout:   cfg.Transport.DialTimeout,
		KeepAlive: 30 * time.Second,
	}

	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,

		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.Transport.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.Transport.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.Transport.MaxConnsPerHost,
		IdleConnTimeout:       cfg.Transport.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.Transport.TLSHandshakeTimeout,
		ExpectContinueTimeout: cfg.Transport.ExpectContinueTimeout,
		ResponseHeaderTimeout: cfg.Transport.ResponseHeaderTimeout,
		DisableCompression:    cfg.Transport.DisableCompression,
		TLSClientConfig:       tlsConfig,
	}
}
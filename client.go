package mongox

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/maxbolgarin/gorder"
	"github.com/maxbolgarin/lang"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/auth"
)

// Client is a handle representing a pool of connections to a MongoDB deployment.
// The Client type opens and closes connections automatically and maintains a pool of idle connections.
// It is safe for concurrent use by multiple goroutines.
type Client struct {
	client *mongo.Client
	config Config

	dbs  map[string]*Database
	adbs map[string]*AsyncDatabase
	mu   sync.RWMutex
}

// Connect creates a new MongoDB client with the given configuration.
// It connects to the MongoDB cluster and pings the primary to validate the connection.
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	opts := options.Client().ApplyURI(buildURL(cfg))
	if cfg.URI != "" {
		opts = options.Client().ApplyURI(cfg.URI)
	}

	lang.IfV(cfg.AppName, func() { opts.SetAppName(cfg.AppName) })
	lang.IfV(cfg.ReplicaSetName, func() { opts.SetReplicaSet(cfg.ReplicaSetName) })
	lang.IfF(len(cfg.Compressors) > 0, func() { opts.SetCompressors(cfg.Compressors) })

	if cfg.Connection != nil {
		lang.IfV(cfg.Connection.ConnectTimeout, func() { opts.SetConnectTimeout(*cfg.Connection.ConnectTimeout) })
		lang.IfV(cfg.Connection.MaxConnIdleTime, func() { opts.SetMaxConnIdleTime(*cfg.Connection.MaxConnIdleTime) })
		lang.IfV(cfg.Connection.MaxConnecting, func() { opts.SetMaxConnecting(*cfg.Connection.MaxConnecting) })
		lang.IfV(cfg.Connection.MaxPoolSize, func() { opts.SetMaxPoolSize(*cfg.Connection.MaxPoolSize) })
		lang.IfV(cfg.Connection.MinPoolSize, func() { opts.SetMinPoolSize(*cfg.Connection.MinPoolSize) })
		lang.IfV(cfg.Connection.IsDirect, func() { opts.SetDirect(cfg.Connection.IsDirect) })
	}

	if cfg.Auth != nil {
		opts.SetAuth(buildCredential(cfg))
	}

	if cfg.BSONOptions != nil {
		opts.SetBSONOptions(buildBSONOptions(cfg))
	}

	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("validate options: %w", err)
	}

	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}

	out := &Client{
		client: client,
		config: cfg,
		dbs:    make(map[string]*Database),
		adbs:   make(map[string]*AsyncDatabase),
	}

	return out, nil
}

// Disconnect closes the connection to the MongoDB cluster.
func (m *Client) Disconnect(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}

// Client returns the underlying mongo client.
func (m *Client) Client() *mongo.Client {
	return m.client
}

// Ping sends a ping command to verify that the client can connect to the deployment.
func (m *Client) Ping(ctx context.Context) error {
	return m.client.Ping(ctx, nil)
}

// IsTLS returns whether the client is using TLS for its connections.
// This is a helper method to determine if the connection is secure.
func (m *Client) IsTLS() bool {
	return IsTLSConnection(m)
}

// Database returns a handle to a database.
func (m *Client) Database(name string) *Database {
	m.mu.RLock()
	db, ok := m.dbs[name]
	m.mu.RUnlock()

	if ok {
		return db
	}

	db = &Database{
		db:    m.client.Database(name),
		colls: make(map[string]*Collection),
	}

	m.mu.Lock()
	m.dbs[name] = db
	m.mu.Unlock()

	return db
}

func (m *Client) AsyncDatabase(ctx context.Context, name string, workers int, logger gorder.Logger) *AsyncDatabase {
	m.mu.RLock()
	adb, ok := m.adbs[name]
	m.mu.RUnlock()

	if ok {
		return adb
	}

	adb = &AsyncDatabase{
		db: m.Database(name),
		queue: gorder.New[string](ctx, gorder.Options{
			Workers: workers,
			Logger:  logger,
			Retries: DefaultAsyncRetries,
		}),
		log:   logger,
		colls: make(map[string]*AsyncCollection),
	}

	m.mu.Lock()
	m.adbs[name] = adb
	m.mu.Unlock()

	return adb
}

func buildURL(cfg Config) string {
	out := strings.Builder{}
	out.WriteString("mongodb://")
	if cfg.Address == "" && len(cfg.Hosts) == 0 {
		cfg.Address = "localhost:27017"
	}
	if cfg.Address != "" {
		out.WriteString(cfg.Address)
	}
	for i, host := range cfg.Hosts {
		if host == cfg.Address {
			continue
		}
		if cfg.Address == "" && i == 0 {
			out.WriteString(host)
		} else {
			out.WriteString("," + host)
		}
	}
	if cfg.Connection != nil && cfg.Connection.TLS != nil {
		out.WriteString("/?tls=true")

		if cfg.Connection.TLS.Insecure {
			out.WriteString("&tlsInsecure=true")
		}
		if cfg.Connection.TLS.CAFilePath != "" {
			out.WriteString("&tlsCAFile=" + cfg.Connection.TLS.CAFilePath)
		}
		if cfg.Connection.TLS.CertificateKeyFilePath != "" {
			out.WriteString("&tlsCertificateKeyFile=" + cfg.Connection.TLS.CertificateKeyFilePath)
		}
		if cfg.Connection.TLS.CertificateFilePath != "" {
			out.WriteString("&tlsCertificateFile=" + cfg.Connection.TLS.CertificateFilePath)
		}
		if cfg.Connection.TLS.PrivateKeyFilePath != "" {
			out.WriteString("&tlsPrivateKey=" + cfg.Connection.TLS.PrivateKeyFilePath)
		}
		if cfg.Connection.TLS.PrivateKeyPassword != "" {
			out.WriteString("&tlsCertificateKeyFilePassword=" + cfg.Connection.TLS.PrivateKeyPassword)
		}
	}

	return out.String()
}

func buildCredential(cfg Config) options.Credential {
	props := make(map[string]string)
	for k, v := range cfg.Auth.Props {
		props[k] = v
	}
	if cfg.Auth.AuthMechanism == auth.MongoDBAWS && cfg.Auth.AWSSessionToken != "" {
		props["AWS_SESSION_TOKEN"] = cfg.Auth.AWSSessionToken
	}
	if cfg.Auth.AuthMechanism == auth.GSSAPI {
		if cfg.Auth.GSSCAPICanonicalizeHostName {
			props["GSSAPI_CANONICALIZE_HOST_NAME"] = "true"
		}
		if cfg.Auth.GSSAPIServiceName != "" {
			props["GSSAPI_SERVICE_NAME"] = cfg.Auth.GSSAPIServiceName
		}
		if cfg.Auth.GSSAPIServiceRealm != "" {
			props["GSSAPI_SERVICE_REALM"] = cfg.Auth.GSSAPIServiceRealm
		}
		if cfg.Auth.GSSAPIServiceHost != "" {
			props["GSSAPI_SERVICE_HOST"] = cfg.Auth.GSSAPIServiceHost
		}
	}

	return options.Credential{
		Username:                cfg.Auth.Username,
		Password:                cfg.Auth.Password,
		AuthSource:              cfg.Auth.AuthSource,
		AuthMechanism:           cfg.Auth.AuthMechanism,
		AuthMechanismProperties: props,
		PasswordSet:             cfg.Auth.AuthMechanism == auth.GSSAPI && cfg.Auth.Password != "",
	}
}

func buildBSONOptions(cfg Config) *options.BSONOptions {
	return &options.BSONOptions{
		UseJSONStructTags:       cfg.BSONOptions.UseJSONStructTags,
		ErrorOnInlineDuplicates: cfg.BSONOptions.ErrorOnInlineDuplicates,
		IntMinSize:              cfg.BSONOptions.IntMinSize,
		NilMapAsEmpty:           cfg.BSONOptions.NilMapAsEmpty,
		NilSliceAsEmpty:         cfg.BSONOptions.NilSliceAsEmpty,
		NilByteSliceAsEmpty:     cfg.BSONOptions.NilByteSliceAsEmpty,
		OmitZeroStruct:          cfg.BSONOptions.OmitZeroStruct,
		StringifyMapKeysWithFmt: cfg.BSONOptions.StringifyMapKeysWithFmt,
		AllowTruncatingDoubles:  cfg.BSONOptions.AllowTruncatingDoubles,
		BinaryAsSlice:           cfg.BSONOptions.BinaryAsSlice,
		DefaultDocumentM:        cfg.BSONOptions.DefaultDocumentM,
		ObjectIDAsHexString:     cfg.BSONOptions.ObjectIDAsHexString,
		UseLocalTimeZone:        cfg.BSONOptions.UseLocalTimeZone,
		ZeroMaps:                cfg.BSONOptions.ZeroMaps,
		ZeroStructs:             cfg.BSONOptions.ZeroStructs,
	}
}

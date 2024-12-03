package mongox

import (
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

// DefaultAsyncRetries is the maximum number of retries for failed tasks in async mode.
const DefaultAsyncRetries = 10

// Config contains database configuration for creating MongoDB client.
type Config struct {
	// AppName that is sent to the server when creating new connections.
	// It is used by the server to log connection and profiling information (e.g. slow query logs).
	// The default is empty, meaning no app name will be sent.
	AppName string `yaml:"app_name" json:"app_name" env:"MONGO_APP_NAME"`

	// Address is the MongoDB address.The default is "localhost:27017".
	Address string `yaml:"address" json:"address" env:"MONGO_ADDRESS"`

	// Hosts is the optional list of MongoDB hosts.
	Hosts []string `yaml:"hosts" json:"hosts" env:"MONGO_HOSTS"`

	// ReplicaSetName is the name of the replica set to connect to.
	ReplicaSetName string `yaml:"replica_set_name" json:"replica_set_name" env:"MONGO_REPLICA_SET_NAME"`

	// Compressors that can be used when communicating with a server.
	// Valid values are: "snappy", "zlib", "zstd".
	Compressors []string `yaml:"compressors" json:"compressors" env:"MONGO_COMPRESSORS"`

	// Connection contains connection pool configuration for creating MongoDB client.
	Connection *ConnectionConfig `yaml:"connection" json:"connection"`

	// Auth contains authentication configuration for creating MongoDB client.
	Auth *AuthConfig `yaml:"auth" json:"auth"`

	// BSONOptions contains optional BSON marshaling and unmarshaling behaviors.
	BSONOptions *BSONOptions `yaml:"bson_options" json:"bson_options"`

	// URI is a MongoDB connection string. You can provide it insted of all other settings.
	URI string `yaml:"uri" json:"uri" env:"MONGO_URI"`
}

// ConnectionConfig contains connection pool configuration for creating MongoDB client.
type ConnectionConfig struct {
	// ConnectTimeout is the maximum amount of time to wait for a connection to be established.
	// Default is 30 seconds.
	ConnectTimeout *time.Duration `yaml:"connect_timeout" json:"connect_timeout" env:"MONGO_CONNECT_TIMEOUT"`

	// MaxConnIdleTime is the maximum amount of time a connection can sit in the idle pool.
	// Default is 0, meaning a connection can remain unused indefinitely.
	MaxConnIdleTime *time.Duration `yaml:"max_conn_idle_time" json:"max_conn_idle_time" env:"MONGO_MAX_CONN_IDLE_TIME"`

	// MaxConnecting is the maximum number of connections a connection pool may establish simultaneously.
	// Default is 2. Values greater than 100 are not recommended.
	MaxConnecting *uint64 `yaml:"max_connecting" json:"max_connecting" env:"MONGO_MAX_CONNECTING"`

	// MaxPoolSize is the maximum number of connections allowed in the driver's connection pool to each server.
	// Requests to a server will block if this maximum is reached.
	// Default is 100.
	MaxPoolSize *uint64 `yaml:"max_pool_size" json:"max_pool_size" env:"MONGO_MAX_POOL_SIZE"`

	// MinPoolSize is the minimum number of connections allowed in the driver's connection pool to each server.
	// If this is non-zero, each server's pool will be maintained in the background to ensure that the size does not
	// fall below the minimum.
	// Default is 0.
	MinPoolSize *uint64 `yaml:"min_pool_size" json:"min_pool_size" env:"MONGO_MIN_POOL_SIZE"`

	// IsDirect is a flag that enables direct connection to MongoDB server.
	IsDirect bool `yaml:"is_direct" json:"is_direct" env:"MONGO_IS_DIRECT"`

	// TLS contains TLS configuration for creating MongoDB client.
	// Provided TLS configuration means client will use TLS connection.
	TLS *TLSConfig `yaml:"tls" json:"tls" env:"MONGO_TLS"`
}

// TLSConfig contains TLS configuration for creating MongoDB client.
type TLSConfig struct {
	// Insecure specifies whether or not certificates and hostnames received from the server should be validated.
	Insecure bool `yaml:"insecure" json:"insecure" env:"MONGO_TLS_INSECURE"`

	// CAFilePath is the path to the file with either a single or bundle of certificate authorities to be considered
	// trusted when making a TLS connection. This is optional and used for authentication with MONGODB-X509.
	CAFilePath string `yaml:"ca_file_path" json:"ca_file_path" env:"MONGO_CA_FILE_PATH"`

	// CertificateKeyFilePath is the path to the client certificate file or the client private key file. In the case that
	// both are needed, the files should be concatenated.
	// This is optional and used for authentication with MONGODB-X509.
	CertificateKeyFilePath string `yaml:"certificate_key_file_path" json:"certificate_key_file_path" env:"MONGO_CERTIFICATE_KEY_FILE_PATH"`

	// CertificateFilePath is the path to the client certificate file.
	// This is optional and used for authentication with MONGODB-X509.
	CertificateFilePath string `yaml:"certificate_file_path" json:"certificate_file_path" env:"MONGO_CERTIFICATE_FILE_PATH"`

	// CertificateKeyFilePath is the path to the client private key file.
	// This is optional and used for authentication with MONGODB-X509.
	PrivateKeyFilePath string `yaml:"private_key_file_path" json:"private_key_file_path" env:"MONGO_PRIVATE_KEY_FILE_PATH"`

	// CertificateKeyPassword is the password to the client certificate file or the client private key file.
	// This is optional and used for authentication with MONGODB-X509.
	PrivateKeyPassword string `yaml:"certificate_key_password" json:"certificate_key_password" env:"MONGO_CERTIFICATE_KEY_PASSWORD"`
}

// AuthConfig contains authentication configuration for creating MongoDB client.
type AuthConfig struct {
	// Username is the username for MongoDB authentication.
	// This is optional for X509 authentication and will be extracted from the client certificate if not specified.
	Username string `yaml:"username" json:"username" env:"MONGO_USERNAME"`

	// Password is the password for MongoDB authentication.
	// This must not be specified for X509 and is optional for GSSAPI authentication.
	Password string `yaml:"password" json:"password" env:"MONGO_PASSWORD"`

	// AuthMechanism is the authentication mechanism for MongoDB.
	// Supported values include "SCRAM-SHA-256", "SCRAM-SHA-1", "MONGODB-CR", "PLAIN", "GSSAPI", "MONGODB-X509", and "MONGODB-AWS".
	// Default is SCRAM-SHA-256 authentication.
	AuthMechanism string `yaml:"auth_mechanism" json:"auth_mechanism" env:"MONGO_AUTH_MECHANISM"`

	// AuthSource is the name of the database to use for authentication.
	// This defaults to "$external" for MONGODB-AWS, MONGODB-OIDC, MONGODB-X509, GSSAPI, and PLAIN.
	// It defaults to  "admin" for all other auth mechanisms.
	AuthSource string `yaml:"auth_source" json:"auth_source" env:"MONGO_AUTH_SOURCE"`

	// AWSSessionToken is the AWS token for MONGODB-AWS authentication.
	// This is optional and used for authentication with temporary credentials.
	AWSSessionToken string `yaml:"aws_session_token" json:"aws_session_token" env:"MONGO_AWS_SESSION_TOKEN"`

	// GSSAPIServiceName is the service name to use for GSSAPI authentication.
	GSSAPIServiceName string `yaml:"gssapi_service_name" json:"gssapi_service_name" env:"MONGO_GSSAPI_SERVICE_NAME"`

	// GSSAPIServiceRealm is the service realm for GSSAPI authentication.
	GSSAPIServiceRealm string `yaml:"gssapi_service_realm" json:"gssapi_service_realm" env:"MONGO_GSSAPI_SERVICE_REALM"`

	// GSSAPIServiceHost is the host name to use for GSSAPI authentication.
	GSSAPIServiceHost string `yaml:"gssapi_service_host" json:"gssapi_service_host" env:"MONGO_GSSAPI_SERVICE_HOST"`

	// GSSCAPICanonicalizeHostName indicates whether the driver should canonicalize the host name for GSSAPI authentication.
	GSSCAPICanonicalizeHostName bool `yaml:"gssapi_canonicalize_host_name" json:"gssapi_canonicalize_host_name" env:"MONGO_GSSAPI_CANONICALIZE_HOST_NAME"`

	// Props is a map of additional authentication properties.
	Props map[string]string `yaml:"props" json:"props"`
}

// BSONOptions are optional BSON marshaling and unmarshaling behaviors.
type BSONOptions struct {
	// UseJSONStructTags causes the driver to fall back to using the "json"
	// struct tag if a "bson" struct tag is not specified.
	UseJSONStructTags bool `yaml:"use_json_struct_tags" json:"use_json_struct_tags"`

	// ErrorOnInlineDuplicates causes the driver to return an error if there is
	// a duplicate field in the marshaled BSON when the "inline" struct tag
	// option is set.
	ErrorOnInlineDuplicates bool `yaml:"error_on_inline_duplicates" json:"error_on_inline_duplicates"`

	// IntMinSize causes the driver to marshal Go integer values (int, int8,
	// int16, int32, int64, uint, uint8, uint16, uint32, or uint64) as the
	// minimum BSON int size (either 32 or 64 bits) that can represent the
	// integer value.
	IntMinSize bool `yaml:"int_min_size" json:"int_min_size"`

	// NilMapAsEmpty causes the driver to marshal nil Go maps as empty BSON
	// documents instead of BSON null.
	//
	// Empty BSON documents take up slightly more space than BSON null, but
	// preserve the ability to use document update operations like "$set" that
	// do not work on BSON null.
	NilMapAsEmpty bool `yaml:"nil_map_as_empty" json:"nil_map_as_empty"`

	// NilSliceAsEmpty causes the driver to marshal nil Go slices as empty BSON
	// arrays instead of BSON null.
	//
	// Empty BSON arrays take up slightly more space than BSON null, but
	// preserve the ability to use array update operations like "$push" or
	// "$addToSet" that do not work on BSON null.
	NilSliceAsEmpty bool `yaml:"nil_slice_as_empty" json:"nil_slice_as_empty"`

	// NilByteSliceAsEmpty causes the driver to marshal nil Go byte slices as
	// empty BSON binary values instead of BSON null.
	NilByteSliceAsEmpty bool `yaml:"nil_byte_slice_as_empty" json:"nil_byte_slice_as_empty"`

	// OmitZeroStruct causes the driver to consider the zero value for a struct
	// (e.g. MyStruct{}) as empty and omit it from the marshaled BSON when the
	// "omitempty" struct tag option is set.
	OmitZeroStruct bool `yaml:"omit_zero_struct" json:"omit_zero_struct"`

	// StringifyMapKeysWithFmt causes the driver to convert Go map keys to BSON
	// document field name strings using fmt.Sprint instead of the default
	// string conversion logic.
	StringifyMapKeysWithFmt bool `yaml:"stringify_map_keys_with_fmt" json:"stringify_map_keys_with_fmt"`

	// AllowTruncatingDoubles causes the driver to truncate the fractional part
	// of BSON "double" values when attempting to unmarshal them into a Go
	// integer (int, int8, int16, int32, or int64) struct field. The truncation
	// logic does not apply to BSON "decimal128" values.
	AllowTruncatingDoubles bool `yaml:"allow_truncating_doubles" json:"allow_truncating_doubles"`

	// BinaryAsSlice causes the driver to unmarshal BSON binary field values
	// that are the "Generic" or "Old" BSON binary subtype as a Go byte slice
	// instead of a primitive.Binary.
	BinaryAsSlice bool `yaml:"binary_as_slice" json:"binary_as_slice"`

	// DefaultDocumentD causes the driver to always unmarshal documents into the
	// primitive.D type. This behavior is restricted to data typed as
	// "interface{}" or "map[string]interface{}".
	DefaultDocumentD bool `yaml:"default_document_d" json:"default_document_d"`

	// DefaultDocumentM causes the driver to always unmarshal documents into the
	// primitive.M type. This behavior is restricted to data typed as
	// "interface{}" or "map[string]interface{}".
	DefaultDocumentM bool `yaml:"default_document_m" json:"default_document_m"`

	// ObjectIDAsHexString causes the Decoder to decode object IDs to their hex
	// representation.
	ObjectIDAsHexString bool `yaml:"object_id_as_hex_string" json:"object_id_as_hex_string"`

	// UseLocalTimeZone causes the driver to unmarshal time.Time values in the
	// local timezone instead of the UTC timezone.
	UseLocalTimeZone bool `yaml:"use_local_time_zone" json:"use_local_time_zone"`

	// ZeroMaps causes the driver to delete any existing values from Go maps in
	// the destination value before unmarshaling BSON documents into them.
	ZeroMaps bool `yaml:"zero_maps" json:"zero_maps"`

	// ZeroStructs causes the driver to delete any existing values from Go
	// structs in the destination value before unmarshaling BSON documents into
	// them.
	ZeroStructs bool `yaml:"zero_structs" json:"zero_structs"`
}

func (cfg *Config) Read(fileName ...string) error {
	if len(fileName) > 0 {
		return cleanenv.ReadConfig(fileName[0], cfg)
	}
	return cleanenv.ReadEnv(cfg)
}

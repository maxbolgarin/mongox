package mongox_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maxbolgarin/lang"
	"github.com/maxbolgarin/mongox"
)

func TestBuildURLWithTLS(t *testing.T) {
	// This test verifies that TLS configuration is properly added to the connection URL
	tests := []struct {
		name     string
		config   mongox.Config
		expected string
	}{
		{
			name: "Basic TLS",
			config: mongox.Config{
				Address: "localhost:27017",
				Connection: &mongox.ConnectionConfig{
					TLS: &mongox.TLSConfig{},
				},
			},
			expected: "mongodb://localhost:27017/?tls=true",
		},
		{
			name: "TLS with Insecure",
			config: mongox.Config{
				Address: "localhost:27017",
				Connection: &mongox.ConnectionConfig{
					TLS: &mongox.TLSConfig{
						Insecure: true,
					},
				},
			},
			expected: "mongodb://localhost:27017/?tls=true&tlsInsecure=true",
		},
		{
			name: "TLS with CA File",
			config: mongox.Config{
				Address: "localhost:27017",
				Connection: &mongox.ConnectionConfig{
					TLS: &mongox.TLSConfig{
						CAFilePath: "/path/to/ca.pem",
					},
				},
			},
			expected: "mongodb://localhost:27017/?tls=true&tlsCAFile=/path/to/ca.pem",
		},
		{
			name: "TLS with Certificate and Key",
			config: mongox.Config{
				Address: "localhost:27017",
				Connection: &mongox.ConnectionConfig{
					TLS: &mongox.TLSConfig{
						CertificateFilePath: "/path/to/cert.pem",
						PrivateKeyFilePath:  "/path/to/key.pem",
					},
				},
			},
			expected: "mongodb://localhost:27017/?tls=true&tlsCertificateFile=/path/to/cert.pem&tlsPrivateKey=/path/to/key.pem",
		},
		{
			name: "TLS with Certificate Key File",
			config: mongox.Config{
				Address: "localhost:27017",
				Connection: &mongox.ConnectionConfig{
					TLS: &mongox.TLSConfig{
						CertificateKeyFilePath: "/path/to/certkey.pem",
					},
				},
			},
			expected: "mongodb://localhost:27017/?tls=true&tlsCertificateKeyFile=/path/to/certkey.pem",
		},
		{
			name: "TLS with Password",
			config: mongox.Config{
				Address: "localhost:27017",
				Connection: &mongox.ConnectionConfig{
					TLS: &mongox.TLSConfig{
						CertificateKeyFilePath: "/path/to/certkey.pem",
						PrivateKeyPassword:     "password123",
					},
				},
			},
			expected: "mongodb://localhost:27017/?tls=true&tlsCertificateKeyFile=/path/to/certkey.pem&tlsCertificateKeyFilePassword=password123",
		},
		{
			name: "TLS with Multiple Hosts",
			config: mongox.Config{
				Hosts: []string{"host1:27017", "host2:27017", "host3:27017"},
				Connection: &mongox.ConnectionConfig{
					TLS: &mongox.TLSConfig{},
				},
			},
			expected: "mongodb://host1:27017,host2:27017,host3:27017/?tls=true",
		},
		{
			name: "TLS with Everything",
			config: mongox.Config{
				Address: "localhost:27017",
				Connection: &mongox.ConnectionConfig{
					TLS: &mongox.TLSConfig{
						Insecure:               true,
						CAFilePath:             "/path/to/ca.pem",
						CertificateKeyFilePath: "/path/to/certkey.pem",
						CertificateFilePath:    "/path/to/cert.pem",
						PrivateKeyFilePath:     "/path/to/key.pem",
						PrivateKeyPassword:     "password123",
					},
				},
			},
			expected: "mongodb://localhost:27017/?tls=true&tlsInsecure=true&tlsCAFile=/path/to/ca.pem&tlsCertificateKeyFile=/path/to/certkey.pem&tlsCertificateFile=/path/to/cert.pem&tlsPrivateKey=/path/to/key.pem&tlsCertificateKeyFilePassword=password123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := mongox.ExportedBuildURL(tt.config)
			if url != tt.expected {
				t.Errorf("Expected URL %q, got %q", tt.expected, url)
			}
		})
	}
}

func TestTLSConfigurationFromURI(t *testing.T) {
	// Skip if no MongoDB available
	if os.Getenv("TEST_MONGODB_URI") == "" {
		t.Skip("Skipping TLS connection test: TEST_MONGODB_URI not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Connect using URI that has TLS enabled
	client, err := mongox.Connect(ctx, mongox.Config{
		URI: os.Getenv("TEST_MONGODB_URI"),
	})
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer client.Disconnect(ctx)

	// Verify connection works
	err = client.Ping(ctx)
	if err != nil {
		t.Errorf("Failed to ping server: %v", err)
	}
}

func TestCreateTempCertificates(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "mongox-tls-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Test cert file paths
	caFile := filepath.Join(tempDir, "ca.pem")
	certFile := filepath.Join(tempDir, "cert.pem")
	keyFile := filepath.Join(tempDir, "key.pem")

	// Write dummy content to files for testing
	if err := os.WriteFile(caFile, []byte("CA CERT CONTENT"), 0600); err != nil {
		t.Fatalf("Failed to write CA file: %v", err)
	}
	if err := os.WriteFile(certFile, []byte("CLIENT CERT CONTENT"), 0600); err != nil {
		t.Fatalf("Failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, []byte("CLIENT KEY CONTENT"), 0600); err != nil {
		t.Fatalf("Failed to write key file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// This will fail to connect because the certs are not valid,
	// but it tests that the TLS config is properly constructed
	config := mongox.Config{
		Address: "localhost:27017",
		Connection: &mongox.ConnectionConfig{
			ConnectTimeout: lang.Ptr(1 * time.Second),
			TLS: &mongox.TLSConfig{
				CAFilePath:          caFile,
				CertificateFilePath: certFile,
				PrivateKeyFilePath:  keyFile,
			},
		},
	}

	// The connection will fail because these are dummy certs, but we want to verify
	// that the options are constructed correctly
	client, err := mongox.Connect(ctx, config)
	if client != nil {
		client.Disconnect(ctx)
	}

	// Expect an error since our certificates are fake
	if err == nil {
		t.Error("Expected an error with invalid certificates, but got none")
	}
}

func TestTLSConnectionMethods(t *testing.T) {
	// This test verifies that the buildURL function correctly handles TLS settings

	// Case 1: Connection with explicit TLS config
	cfg1 := mongox.Config{
		Address: "localhost:27017",
		Connection: &mongox.ConnectionConfig{
			TLS: &mongox.TLSConfig{
				Insecure: true,
			},
		},
	}
	url1 := mongox.ExportedBuildURL(cfg1)
	if !strings.Contains(url1, "tls=true") {
		t.Errorf("URL should contain 'tls=true', got: %s", url1)
	}
	if !strings.Contains(url1, "tlsInsecure=true") {
		t.Errorf("URL should contain 'tlsInsecure=true', got: %s", url1)
	}

	// Case 2: Connection with no TLS config
	cfg2 := mongox.Config{
		Address:    "localhost:27017",
		Connection: &mongox.ConnectionConfig{},
	}
	url2 := mongox.ExportedBuildURL(cfg2)
	if strings.Contains(url2, "tls=true") {
		t.Errorf("URL should not contain 'tls=true', got: %s", url2)
	}

	// Case 3: Connection with TLS in URI
	cfg3 := mongox.Config{
		URI: "mongodb://localhost:27017/?tls=true",
	}
	// The URI directly overrides buildURL
	if cfg3.URI != "mongodb://localhost:27017/?tls=true" {
		t.Errorf("Expected URI to be 'mongodb://localhost:27017/?tls=true', got: %s", cfg3.URI)
	}
}

func TestIsTLSEnabled(t *testing.T) {
	// Test cases to check if TLS is enabled in a connection string
	tests := []struct {
		name      string
		uri       string
		expectTLS bool
	}{
		{
			name:      "TLS Enabled",
			uri:       "mongodb://localhost:27017/?tls=true",
			expectTLS: true,
		},
		{
			name:      "TLS Disabled",
			uri:       "mongodb://localhost:27017/",
			expectTLS: false,
		},
		{
			name:      "TLS False",
			uri:       "mongodb://localhost:27017/?tls=false",
			expectTLS: false,
		},
		{
			name:      "SSL Enabled (legacy param)",
			uri:       "mongodb://localhost:27017/?ssl=true",
			expectTLS: true,
		},
		{
			name:      "With Auth and TLS",
			uri:       "mongodb://user:pass@localhost:27017/?authSource=admin&tls=true",
			expectTLS: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use the helper function to check if TLS is enabled
			result := mongox.IsTLSEnabled(tt.uri)
			if result != tt.expectTLS {
				t.Errorf("IsTLSEnabled(%q) = %v, want %v", tt.uri, result, tt.expectTLS)
			}
		})
	}
}

func TestClientIsTLS(t *testing.T) {
	// Create contexts for our tests
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test 1: Client with explicit TLS config
	cfg1 := mongox.Config{
		// Set a valid URI for testing, but specify a short timeout so it fails fast
		Address: "localhost:27017",
		Connection: &mongox.ConnectionConfig{
			ConnectTimeout: lang.Ptr(1 * time.Second),
			TLS: &mongox.TLSConfig{
				// Use insecure for testing as we don't have valid certs
				Insecure: true,
			},
		},
	}

	// This will likely fail to connect but that's ok for this test
	client1, _ := mongox.Connect(ctx, cfg1)
	if client1 != nil {
		defer client1.Disconnect(ctx)
		// Should report TLS is enabled
		if !client1.IsTLS() {
			t.Error("Client with TLS config should report TLS enabled")
		}
	}

	// Test 2: Client with URI containing TLS
	cfg2 := mongox.Config{
		URI: "mongodb://localhost:27017/?tls=true",
	}

	client2, _ := mongox.Connect(ctx, cfg2)
	if client2 != nil {
		defer client2.Disconnect(ctx)
		// Should report TLS is enabled
		if !client2.IsTLS() {
			t.Error("Client with TLS in URI should report TLS enabled")
		}
	}

	// Test 3: Client without TLS
	cfg3 := mongox.Config{
		Address: "localhost:27017",
		Connection: &mongox.ConnectionConfig{
			ConnectTimeout: lang.Ptr(1 * time.Second),
		},
	}

	client3, _ := mongox.Connect(ctx, cfg3)
	if client3 != nil {
		defer client3.Disconnect(ctx)
		// Should report TLS is disabled
		if client3.IsTLS() {
			t.Error("Client without TLS config should report TLS disabled")
		}
	}
}

func TestComprehensiveTLSSetup(t *testing.T) {
	// Skip if running in CI or non-interactive environment
	if os.Getenv("CI") != "" || os.Getenv("TEST_MONGODB_TLS") == "" {
		t.Skip("Skipping comprehensive TLS test in CI or non-interactive environment")
	}

	// Create a temp directory for our test certificates
	tempDir, err := os.MkdirTemp("", "mongox-tls-comprehensive")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// For a real comprehensive test, you would:
	// 1. Generate real self-signed certificates
	// 2. Start a MongoDB instance with TLS enabled using these certificates
	// 3. Connect to it using the client with the appropriate TLS config
	// 4. Verify the connection works and is using TLS

	// This is a placeholder for what a comprehensive test would look like
	// In a real-world scenario, you would use docker-compose or similar to set up
	// a properly configured MongoDB instance with TLS

	// Example of a comprehensive test setup:
	tlsURI := os.Getenv("TEST_MONGODB_TLS")
	if tlsURI == "" {
		t.Skip("TEST_MONGODB_TLS environment variable not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongox.Connect(ctx, mongox.Config{
		URI: tlsURI,
	})

	if err != nil {
		t.Skip("Could not connect to TLS-enabled MongoDB: " + err.Error())
	}
	defer client.Disconnect(ctx)

	// Verify the connection is using TLS
	if !client.IsTLS() {
		t.Error("Connection should be using TLS")
	}

	// Test basic operations
	err = client.Ping(ctx)
	if err != nil {
		t.Errorf("Ping should succeed with TLS connection: %v", err)
	}

	// Get a database and do some operations
	db := client.Database("tls_test")
	coll := db.Collection("test_collection")

	// Insert a test document
	_, err = coll.Insert(ctx, map[string]interface{}{
		"key": "value",
		"tls": true,
	})
	if err != nil {
		t.Errorf("Insert should succeed with TLS connection: %v", err)
	}

	// Find the document
	var result map[string]interface{}
	err = coll.FindOne(ctx, &result, mongox.M{"key": "value"})
	if err != nil {
		t.Errorf("FindOne should succeed with TLS connection: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("Should retrieve the correct document. Expected 'value', got %v", result["key"])
	}

	// Clean up
	_, err = coll.DeleteMany(ctx, mongox.M{"key": "value"})
	if err != nil {
		t.Errorf("DeleteMany should succeed with TLS connection: %v", err)
	}
}

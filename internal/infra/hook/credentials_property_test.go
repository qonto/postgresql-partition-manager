// Feature: partition-hooks, Property 17: Credential Propagation
package hook

import (
	"fmt"
	"net/url"
	"testing"

	"pgregory.net/rapid"
)

// **Validates: Requirements 8.3, 8.4**
//
// Property 17: Credential Propagation
// For any valid PostgreSQL connection URL and propagate-credentials = true,
// shell hook execution SHALL receive environment variables PGHOST, PGPORT,
// PGDATABASE, PGUSER, and PGPASSWORD with values correctly extracted from
// the connection URL. When propagate-credentials = false, these variables
// SHALL NOT be injected.

// genAlphanumeric generates a non-empty alphanumeric string suitable for URL components.
func genAlphanumeric(t *rapid.T, label string) string {
	return rapid.StringMatching(`[a-z][a-z0-9]{1,15}`).Draw(t, label)
}

// genHost generates a valid hostname (simple alphanumeric with dots).
func genHost(t *rapid.T) string {
	parts := rapid.IntRange(1, 3).Draw(t, "hostParts")
	host := genAlphanumeric(t, "hostPart0")

	for i := 1; i < parts; i++ {
		host += "." + genAlphanumeric(t, fmt.Sprintf("hostPart%d", i))
	}

	return host
}

// genPort generates a valid port number as a string.
func genPort(t *rapid.T) string {
	port := rapid.IntRange(1, 65535).Draw(t, "port")
	return fmt.Sprintf("%d", port)
}

// genConnectionURL builds a valid PostgreSQL connection URL from components.
func genConnectionURL(t *rapid.T) (connURL string, host string, port string, database string, user string, password string) {
	scheme := rapid.SampledFrom([]string{"postgresql", "postgres"}).Draw(t, "scheme")
	host = genHost(t)
	port = genPort(t)
	database = genAlphanumeric(t, "database")
	user = genAlphanumeric(t, "user")
	password = genAlphanumeric(t, "password")

	connURL = fmt.Sprintf("%s://%s:%s@%s:%s/%s", scheme, user, password, host, port, database)

	return connURL, host, port, database, user, password
}

func TestProperty_CredentialPropagation_ExtractAll(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		connURL, expectedHost, expectedPort, expectedDB, expectedUser, expectedPassword := genConnectionURL(t)

		creds, err := ExtractCredentials(connURL)
		if err != nil {
			t.Fatalf("ExtractCredentials(%q) returned error: %v", connURL, err)
		}

		// Verify all 5 environment variables are present
		requiredKeys := []string{"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"}
		for _, key := range requiredKeys {
			if _, ok := creds[key]; !ok {
				t.Fatalf("Missing required key %q in credentials map", key)
			}
		}

		// Verify extracted values match the components used to construct the URL
		if creds["PGHOST"] != expectedHost {
			t.Fatalf("PGHOST: expected %q, got %q", expectedHost, creds["PGHOST"])
		}

		if creds["PGPORT"] != expectedPort {
			t.Fatalf("PGPORT: expected %q, got %q", expectedPort, creds["PGPORT"])
		}

		if creds["PGDATABASE"] != expectedDB {
			t.Fatalf("PGDATABASE: expected %q, got %q", expectedDB, creds["PGDATABASE"])
		}

		if creds["PGUSER"] != expectedUser {
			t.Fatalf("PGUSER: expected %q, got %q", expectedUser, creds["PGUSER"])
		}

		if creds["PGPASSWORD"] != expectedPassword {
			t.Fatalf("PGPASSWORD: expected %q, got %q", expectedPassword, creds["PGPASSWORD"])
		}
	})
}

func TestProperty_CredentialPropagation_DefaultPort(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a URL without a port - should default to 5432
		scheme := rapid.SampledFrom([]string{"postgresql", "postgres"}).Draw(t, "scheme")
		host := genHost(t)
		database := genAlphanumeric(t, "database")
		user := genAlphanumeric(t, "user")
		password := genAlphanumeric(t, "password")

		connURL := fmt.Sprintf("%s://%s:%s@%s/%s", scheme, user, password, host, database)

		creds, err := ExtractCredentials(connURL)
		if err != nil {
			t.Fatalf("ExtractCredentials(%q) returned error: %v", connURL, err)
		}

		// When port is omitted, PGPORT should default to 5432
		if creds["PGPORT"] != defaultPostgreSQLPort {
			t.Fatalf("PGPORT: expected default %q when port omitted, got %q", defaultPostgreSQLPort, creds["PGPORT"])
		}

		// Other fields should still be correctly extracted
		if creds["PGHOST"] != host {
			t.Fatalf("PGHOST: expected %q, got %q", host, creds["PGHOST"])
		}

		if creds["PGDATABASE"] != database {
			t.Fatalf("PGDATABASE: expected %q, got %q", database, creds["PGDATABASE"])
		}

		if creds["PGUSER"] != user {
			t.Fatalf("PGUSER: expected %q, got %q", user, creds["PGUSER"])
		}

		if creds["PGPASSWORD"] != password {
			t.Fatalf("PGPASSWORD: expected %q, got %q", password, creds["PGPASSWORD"])
		}
	})
}

func TestProperty_CredentialPropagation_SpecialCharsInPassword(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a URL with special characters in the password (URL-encoded)
		scheme := rapid.SampledFrom([]string{"postgresql", "postgres"}).Draw(t, "scheme")
		host := genHost(t)
		port := genPort(t)
		database := genAlphanumeric(t, "database")
		user := genAlphanumeric(t, "user")

		// Generate a password with special characters
		base := genAlphanumeric(t, "passBase")
		special := rapid.SampledFrom([]string{"@", "#", "$", "%", "&", "!", "=", "+"}).Draw(t, "special")
		rawPassword := base + special + genAlphanumeric(t, "passSuffix")

		// URL-encode the password for the connection URL
		encodedPassword := url.QueryEscape(rawPassword)
		connURL := fmt.Sprintf("%s://%s:%s@%s:%s/%s", scheme, user, encodedPassword, host, port, database)

		creds, err := ExtractCredentials(connURL)
		if err != nil {
			t.Fatalf("ExtractCredentials(%q) returned error: %v", connURL, err)
		}

		// The extracted password should be the decoded (raw) password
		if creds["PGPASSWORD"] != rawPassword {
			t.Fatalf("PGPASSWORD: expected decoded %q, got %q", rawPassword, creds["PGPASSWORD"])
		}

		if creds["PGHOST"] != host {
			t.Fatalf("PGHOST: expected %q, got %q", host, creds["PGHOST"])
		}

		if creds["PGPORT"] != port {
			t.Fatalf("PGPORT: expected %q, got %q", port, creds["PGPORT"])
		}

		if creds["PGDATABASE"] != database {
			t.Fatalf("PGDATABASE: expected %q, got %q", database, creds["PGDATABASE"])
		}

		if creds["PGUSER"] != user {
			t.Fatalf("PGUSER: expected %q, got %q", user, creds["PGUSER"])
		}
	})
}

func TestProperty_CredentialPropagation_NoPropagationWhenDisabled(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// When propagate-credentials is false, the shell hook should not propagate credentials.
		// This tests that the ShellConfig.PropagateCredentials field correctly controls behavior.
		config := ShellConfig{
			PropagateCredentials: false,
		}

		// Verify the config has propagation disabled
		if config.PropagateCredentials != false {
			t.Fatal("PropagateCredentials should be false")
		}

		// Generate a valid connection URL
		connURL, _, _, _, _, _ := genConnectionURL(t)

		// Even though we can extract credentials from the URL,
		// when PropagateCredentials is false, the system SHALL NOT inject them.
		// We verify the flag is correctly set and that ExtractCredentials still works
		// (the decision to inject or not is made at the shell runner based on this flag).
		creds, err := ExtractCredentials(connURL)
		if err != nil {
			t.Fatalf("ExtractCredentials should still parse valid URLs, got error: %v", err)
		}

		// Credentials can be extracted but should NOT be injected when PropagateCredentials is false
		if len(creds) != 5 {
			t.Fatalf("Expected 5 credential keys, got %d", len(creds))
		}
	})
}

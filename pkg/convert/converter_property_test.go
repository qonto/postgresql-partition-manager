package convert

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"testing"

	"github.com/qonto/postgresql-partition-manager/internal/infra/partition"
	"pgregory.net/rapid"
)

// Feature: table-partition-conversion, Property 16: Dry-Run Output Prefix
// For any output line produced while in dry-run mode, the line SHALL be prefixed with "[DRY-RUN]".
// Validates: Requirements 13.3

// Feature: table-partition-conversion, Property 14: Credential Redaction in Logs
// For any connection URL or SQL statement containing credentials (password, secret token),
// the logged output SHALL NOT contain the literal password or secret value.
// Validates: Requirements 14.5

// --- Property 16 Tests: Dry-Run Output Prefix ---

func TestProperty16_DryRunOutputPrefix_AllLinesHavePrefix(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate an arbitrary message that could be logged in dry-run mode
		message := rapid.StringMatching(`[A-Za-z0-9 _.,:;/\-\(\)=]{1,100}`).Draw(t, "message")

		// Create a buffer to capture log output
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

		// Create a converter in dry-run mode
		converter := &Converter{
			logger:      *logger,
			dryRun:      true,
			operationID: "test-op-id",
			config:      partitionConfigForTest("public", "events"),
		}

		// Call logDryRun with the generated message
		converter.logDryRun("%s", message)

		// Verify the output contains the [DRY-RUN] prefix
		output := buf.String()
		if !strings.Contains(output, "[DRY-RUN]") {
			t.Fatalf("dry-run output does not contain [DRY-RUN] prefix: %q (message=%q)", output, message)
		}
	})
}

func TestProperty16_DryRunOutputPrefix_WithFormatArgs(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate arbitrary format arguments
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,15}`).Draw(t, "schema")
		table := rapid.StringMatching(`[a-z][a-z0-9_]{1,15}`).Draw(t, "table")
		batchSize := rapid.IntRange(1, 1000000).Draw(t, "batchSize")

		// Create a buffer to capture log output
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

		// Create a converter in dry-run mode
		converter := &Converter{
			logger:      *logger,
			dryRun:      true,
			operationID: "test-op-id",
			config:      partitionConfigForTest(schema, table),
		}

		// Call logDryRun with format arguments (simulating real usage)
		converter.logDryRun("INSERT INTO %s.%s SELECT * FROM source LIMIT %d", schema, table, batchSize)

		// Verify the output contains the [DRY-RUN] prefix
		output := buf.String()
		if !strings.Contains(output, "[DRY-RUN]") {
			t.Fatalf("dry-run output does not contain [DRY-RUN] prefix: %q", output)
		}

		// Verify the formatted message is present in the output
		expectedMsg := fmt.Sprintf("INSERT INTO %s.%s SELECT * FROM source LIMIT %d", schema, table, batchSize)
		if !strings.Contains(output, expectedMsg) {
			t.Fatalf("dry-run output does not contain formatted message: %q not in %q", expectedMsg, output)
		}
	})
}

func TestProperty16_DryRunOutputPrefix_MessageAlwaysStartsWithPrefix(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate various SQL-like messages that would appear in dry-run mode
		sqlStatements := []string{
			"CREATE TABLE %s.%s_cdc_queue (...)",
			"CREATE FUNCTION %s.ppm_cdc_trigger_%s() ...",
			"ALTER TABLE %s.%s RENAME TO %s_old",
			"DROP TABLE %s.%s_cdc_queue",
			"ANALYZE %s.%s",
			"LOCK TABLE %s.%s IN ACCESS EXCLUSIVE MODE",
		}

		idx := rapid.IntRange(0, len(sqlStatements)-1).Draw(t, "stmtIdx")
		schema := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "schema")
		table := rapid.StringMatching(`[a-z][a-z0-9_]{1,10}`).Draw(t, "table")

		// Create a buffer to capture log output
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

		converter := &Converter{
			logger:      *logger,
			dryRun:      true,
			operationID: "test-op-id",
			config:      partitionConfigForTest(schema, table),
		}

		// Call logDryRun with the selected SQL statement
		converter.logDryRun(sqlStatements[idx], schema, table, table)

		// Verify the slog message field starts with [DRY-RUN]
		output := buf.String()
		// The slog text handler outputs: time=... level=INFO msg="[DRY-RUN] ..."
		// We verify the msg field contains the [DRY-RUN] prefix
		if !strings.Contains(output, `msg="[DRY-RUN]`) {
			t.Fatalf("dry-run log message does not start with [DRY-RUN] prefix in msg field: %q", output)
		}
	})
}

// --- Property 14 Tests: Credential Redaction in Logs ---

func TestProperty14_CredentialRedaction_PasswordNotInOutput(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate arbitrary credentials
		username := rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{2,20}`).Draw(t, "username")
		password := rapid.StringMatching(`[a-zA-Z0-9!@#$%^&*]{4,30}`).Draw(t, "password")
		host := rapid.StringMatching(`[a-z][a-z0-9\-]{2,20}\.[a-z]{2,5}`).Draw(t, "host")
		port := rapid.IntRange(1024, 65535).Draw(t, "port")
		dbName := rapid.StringMatching(`[a-z][a-z0-9_]{2,15}`).Draw(t, "dbName")

		// Construct a PostgreSQL connection URL with credentials
		connectionURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			username, url.PathEscape(password), host, port, dbName)

		// Redact the URL
		redacted := RedactConnectionURL(connectionURL)

		// Verify the password is NOT present in the redacted output
		if strings.Contains(redacted, password) {
			t.Fatalf("redacted URL still contains password: redacted=%q, password=%q, original=%q",
				redacted, password, connectionURL)
		}

		// Verify the username IS preserved
		if !strings.Contains(redacted, username) {
			t.Fatalf("redacted URL lost the username: redacted=%q, username=%q",
				redacted, username)
		}

		// Verify "REDACTED" placeholder is present
		if !strings.Contains(redacted, "REDACTED") {
			t.Fatalf("redacted URL does not contain REDACTED placeholder: %q", redacted)
		}
	})
}

func TestProperty14_CredentialRedaction_PasswordNotInLogOutput(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate arbitrary credentials
		username := rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{2,15}`).Draw(t, "username")
		password := rapid.StringMatching(`[a-zA-Z0-9]{6,20}`).Draw(t, "password")
		host := rapid.StringMatching(`[a-z]{3,10}`).Draw(t, "host")
		port := rapid.IntRange(1024, 65535).Draw(t, "port")
		dbName := rapid.StringMatching(`[a-z][a-z0-9_]{2,10}`).Draw(t, "dbName")

		// Construct a PostgreSQL connection URL with credentials
		connectionURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			username, password, host, port, dbName)

		// Create a buffer to capture log output
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

		// Simulate logging the redacted connection URL (as the converter should do)
		redacted := RedactConnectionURL(connectionURL)
		logger.Info("Connecting to database", "url", redacted)

		// Verify the password is NOT present in the log output
		output := buf.String()
		if strings.Contains(output, password) {
			t.Fatalf("log output contains literal password: output=%q, password=%q",
				output, password)
		}
	})
}

func TestProperty14_CredentialRedaction_URLWithoutPasswordUnchanged(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a URL without a password
		username := rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]{2,15}`).Draw(t, "username")
		host := rapid.StringMatching(`[a-z]{3,10}`).Draw(t, "host")
		port := rapid.IntRange(1024, 65535).Draw(t, "port")
		dbName := rapid.StringMatching(`[a-z][a-z0-9_]{2,10}`).Draw(t, "dbName")

		// URL with username only (no password)
		connectionURL := fmt.Sprintf("postgres://%s@%s:%d/%s", username, host, port, dbName)

		// Redact the URL
		redacted := RedactConnectionURL(connectionURL)

		// Without a password, the URL should not contain "REDACTED"
		if strings.Contains(redacted, "REDACTED") {
			t.Fatalf("URL without password should not be redacted: original=%q, redacted=%q",
				connectionURL, redacted)
		}

		// The username should still be present
		if !strings.Contains(redacted, username) {
			t.Fatalf("redacted URL lost the username: redacted=%q, username=%q",
				redacted, username)
		}
	})
}

func TestProperty14_CredentialRedaction_SpecialCharactersInPassword(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate passwords with special characters that need URL encoding
		username := rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9]{2,10}`).Draw(t, "username")
		// Generate a password with characters that require URL encoding
		password := rapid.StringMatching(`[a-zA-Z0-9@#$!]{4,20}`).Draw(t, "password")
		host := rapid.StringMatching(`[a-z]{3,10}`).Draw(t, "host")
		port := rapid.IntRange(1024, 65535).Draw(t, "port")
		dbName := rapid.StringMatching(`[a-z][a-z0-9_]{2,10}`).Draw(t, "dbName")

		// Construct URL with URL-encoded password
		connectionURL := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			username, url.PathEscape(password), host, port, dbName)

		// Redact the URL
		redacted := RedactConnectionURL(connectionURL)

		// Verify the password (both raw and encoded forms) is NOT present
		if strings.Contains(redacted, password) {
			t.Fatalf("redacted URL contains raw password: redacted=%q, password=%q",
				redacted, password)
		}

		encodedPassword := url.PathEscape(password)
		if strings.Contains(redacted, encodedPassword) && encodedPassword != "REDACTED" {
			t.Fatalf("redacted URL contains encoded password: redacted=%q, encodedPassword=%q",
				redacted, encodedPassword)
		}

		// Verify "REDACTED" placeholder is present
		if !strings.Contains(redacted, "REDACTED") {
			t.Fatalf("redacted URL does not contain REDACTED placeholder: %q", redacted)
		}
	})
}

// --- Helper functions ---

func partitionConfigForTest(schema, table string) partition.Configuration {
	cfg := partition.Configuration{
		Schema:         schema,
		Table:          table,
		PartitionKey:   "created_at",
		Interval:       "daily",
		Retention:      90,
		PreProvisioned: 7,
		CleanupPolicy:  "drop",
	}
	cfg.ApplyConvertDefaults()

	return cfg
}

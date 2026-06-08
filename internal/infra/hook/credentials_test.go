package hook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		url         string
		expected    map[string]string
		expectError bool
	}{
		{
			name: "standard URL with all components",
			url:  "postgresql://myuser:mypassword@localhost:5432/mydb",
			expected: map[string]string{
				"PGHOST":     "localhost",
				"PGPORT":     "5432",
				"PGDATABASE": "mydb",
				"PGUSER":     "myuser",
				"PGPASSWORD": "mypassword",
			},
		},
		{
			name: "URL with custom port",
			url:  "postgresql://admin:secret@db.example.com:5433/production",
			expected: map[string]string{
				"PGHOST":     "db.example.com",
				"PGPORT":     "5433",
				"PGDATABASE": "production",
				"PGUSER":     "admin",
				"PGPASSWORD": "secret",
			},
		},
		{
			name: "URL without port defaults to 5432",
			url:  "postgresql://user:pass@host.example.com/testdb",
			expected: map[string]string{
				"PGHOST":     "host.example.com",
				"PGPORT":     "5432",
				"PGDATABASE": "testdb",
				"PGUSER":     "user",
				"PGPASSWORD": "pass",
			},
		},
		{
			name: "URL without password",
			url:  "postgresql://user@localhost:5432/mydb",
			expected: map[string]string{
				"PGHOST":     "localhost",
				"PGPORT":     "5432",
				"PGDATABASE": "mydb",
				"PGUSER":     "user",
				"PGPASSWORD": "",
			},
		},
		{
			name: "URL with special characters in password (URL-encoded)",
			url:  "postgresql://user:p%40ss%23word@localhost:5432/mydb",
			expected: map[string]string{
				"PGHOST":     "localhost",
				"PGPORT":     "5432",
				"PGDATABASE": "mydb",
				"PGUSER":     "user",
				"PGPASSWORD": "p@ss#word",
			},
		},
		{
			name: "URL with special characters in username (URL-encoded)",
			url:  "postgresql://user%40domain:password@localhost:5432/mydb",
			expected: map[string]string{
				"PGHOST":     "localhost",
				"PGPORT":     "5432",
				"PGDATABASE": "mydb",
				"PGUSER":     "user@domain",
				"PGPASSWORD": "password",
			},
		},
		{
			name: "postgres scheme (short form)",
			url:  "postgres://user:pass@localhost:5432/mydb",
			expected: map[string]string{
				"PGHOST":     "localhost",
				"PGPORT":     "5432",
				"PGDATABASE": "mydb",
				"PGUSER":     "user",
				"PGPASSWORD": "pass",
			},
		},
		{
			name:        "invalid scheme",
			url:         "mysql://user:pass@localhost:3306/mydb",
			expectError: true,
		},
		{
			name:        "empty URL",
			url:         "",
			expectError: true,
		},
		{
			name:        "not a URL",
			url:         "not-a-valid-url",
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := ExtractCredentials(tc.url)

			if tc.expectError {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrInvalidConnectionURL)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

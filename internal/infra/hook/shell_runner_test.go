package hook

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShellRunner_Run(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runner := NewShellRunner(*logger)

	tests := []struct {
		name        string
		hook        *ResolvedHook
		expectError bool
	}{
		{
			name: "successful command",
			hook: &ResolvedHook{
				Name: "test-success",
				Type: ShellType,
				Config: &ShellConfig{
					Command: "true",
				},
			},
			expectError: false,
		},
		{
			name: "failing command",
			hook: &ResolvedHook{
				Name: "test-failure",
				Type: ShellType,
				Config: &ShellConfig{
					Command: "false",
				},
			},
			expectError: true,
		},
		{
			name: "nil shell config",
			hook: &ResolvedHook{
				Name:   "test-nil-shell",
				Type:   ShellType,
				Config: nil,
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := runner.Run(context.Background(), tc.hook)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestShellRunner_EnvPropagation(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runner := NewShellRunner(*logger)

	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "env_output.txt")

	hook := &ResolvedHook{
		Name: "test-env-propagation",
		Type: ShellType,
		Config: &ShellConfig{
			Command: "sh",
			Args:    []string{"-c", "echo $MY_CUSTOM_VAR > " + outFile},
			Env: map[string]string{
				"MY_CUSTOM_VAR": "hello_from_hook",
			},
		},
	}

	err := runner.Run(context.Background(), hook)
	require.NoError(t, err)

	content, err := os.ReadFile(outFile)
	require.NoError(t, err)
	assert.Equal(t, "hello_from_hook", strings.TrimSpace(string(content)))
}

func TestShellRunner_CredentialInjection(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runner := NewShellRunner(*logger)

	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "creds_output.txt")

	hook := &ResolvedHook{
		Name: "test-credential-injection",
		Type: ShellType,
		Config: &ShellConfig{
			Command:              "sh",
			Args:                 []string{"-c", "echo $PGHOST:$PGPORT:$PGDATABASE:$PGUSER:$PGPASSWORD > " + outFile},
			PropagateCredentials: true,
		},
		ConnectionURL: "postgresql://myuser:mypassword@dbhost:5433/mydb",
	}

	err := runner.Run(context.Background(), hook)
	require.NoError(t, err)

	content, err := os.ReadFile(outFile)
	require.NoError(t, err)
	assert.Equal(t, "dbhost:5433:mydb:myuser:mypassword", strings.TrimSpace(string(content)))
}

func TestShellRunner_CredentialInjectionDisabled(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runner := NewShellRunner(*logger)

	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "no_creds_output.txt")

	hook := &ResolvedHook{
		Name: "test-no-credentials",
		Type: ShellType,
		Config: &ShellConfig{
			Command:              "sh",
			Args:                 []string{"-c", "echo \"PGHOST=${PGHOST:-unset}\" > " + outFile},
			PropagateCredentials: false,
		},
		ConnectionURL: "postgresql://myuser:mypassword@dbhost:5433/mydb",
	}

	err := runner.Run(context.Background(), hook)
	require.NoError(t, err)

	content, err := os.ReadFile(outFile)
	require.NoError(t, err)
	assert.Equal(t, "PGHOST=unset", strings.TrimSpace(string(content)))
}

func TestShellRunner_InvalidConnectionURLWithPropagateCredentials(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runner := NewShellRunner(*logger)

	hook := &ResolvedHook{
		Name: "test-invalid-url",
		Type: ShellType,
		Config: &ShellConfig{
			Command:              "echo",
			Args:                 []string{"hello"},
			PropagateCredentials: true,
		},
		ConnectionURL: "not-a-valid-url",
	}

	err := runner.Run(context.Background(), hook)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to extract credentials")
}

func TestShellRunner_ContextCancellation(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runner := NewShellRunner(*logger)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	hook := &ResolvedHook{
		Name: "test-context-timeout",
		Type: ShellType,
		Config: &ShellConfig{
			Command: "sleep",
			Args:    []string{"10"},
		},
	}

	err := runner.Run(ctx, hook)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "shell hook")
}

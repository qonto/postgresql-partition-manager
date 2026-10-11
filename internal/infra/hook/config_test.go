package hook

import (
	"errors"
	"testing"
	"time"
)

// **Validates: Requirements 2.3, 2.4, 2.5, 2.15**

func TestValidate_MissingName(t *testing.T) {
	hook := HookEntry{
		Name: "",
		Type: ShellType,
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for missing name, got nil")
	}

	if !errors.Is(err, ErrNameRequired) {
		t.Fatalf("expected ErrNameRequired, got: %v", err)
	}
}

func TestValidate_MissingType(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    "",
		Timeout: 30 * time.Second,
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for missing type, got nil")
	}

	if !errors.Is(err, ErrTypeRequired) {
		t.Fatalf("expected ErrTypeRequired, got: %v", err)
	}
}

func TestValidate_MissingTimeout(t *testing.T) {
	hook := HookEntry{
		Name: "my-hook",
		Type: ShellType,
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for missing timeout, got nil")
	}

	if !errors.Is(err, ErrTimeoutRequired) {
		t.Fatalf("expected ErrTimeoutRequired, got: %v", err)
	}
}

func TestValidate_InvalidType(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    "invalid-type",
		Timeout: 30 * time.Second,
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for invalid type, got nil")
	}

	if !errors.Is(err, ErrInvalidType) {
		t.Fatalf("expected ErrInvalidType, got: %v", err)
	}
}

func TestValidate_InvalidOnFailure(t *testing.T) {
	hook := HookEntry{
		Name:      "my-hook",
		Type:      ShellType,
		Timeout:   30 * time.Second,
		OnFailure: "invalid-policy",
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for invalid on_failure, got nil")
	}

	if !errors.Is(err, ErrInvalidOnFailure) {
		t.Fatalf("expected ErrInvalidOnFailure, got: %v", err)
	}
}

func TestValidate_InvalidBackoff(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    ShellType,
		Timeout: 30 * time.Second,
		Retry: RetryConfig{
			Backoff: "invalid-backoff",
		},
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for invalid backoff, got nil")
	}

	if !errors.Is(err, ErrInvalidBackoff) {
		t.Fatalf("expected ErrInvalidBackoff, got: %v", err)
	}
}

func TestValidate_ValidOnFailureAbort(t *testing.T) {
	hook := HookEntry{
		Name:      "my-hook",
		Type:      ShellType,
		Timeout:   30 * time.Second,
		OnFailure: OnFailureAbort,
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err != nil {
		t.Fatalf("expected no error for on_failure=abort, got: %v", err)
	}
}

func TestValidate_ValidOnFailureContinue(t *testing.T) {
	hook := HookEntry{
		Name:      "my-hook",
		Type:      ShellType,
		Timeout:   30 * time.Second,
		OnFailure: OnFailureContinue,
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err != nil {
		t.Fatalf("expected no error for on_failure=continue, got: %v", err)
	}
}

func TestValidate_ValidBackoffFixed(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    ShellType,
		Timeout: 30 * time.Second,
		Retry: RetryConfig{
			Backoff: BackoffFixed,
		},
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err != nil {
		t.Fatalf("expected no error for backoff=fixed, got: %v", err)
	}
}

func TestValidate_ValidBackoffExponential(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    ShellType,
		Timeout: 30 * time.Second,
		Retry: RetryConfig{
			Backoff: BackoffExponential,
		},
		Config: map[string]any{
			"command": "/usr/bin/echo",
		},
	}

	err := hook.Validate()
	if err != nil {
		t.Fatalf("expected no error for backoff=exponential, got: %v", err)
	}
}

// Shell config validation tests

func TestValidate_Shell_MissingConfig(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    ShellType,
		Timeout: 30 * time.Second,
		Config:  nil,
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for missing shell config, got nil")
	}

	if !errors.Is(err, ErrShellConfigRequired) {
		t.Fatalf("expected ErrShellConfigRequired, got: %v", err)
	}
}

func TestValidate_Shell_MissingCommand(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    ShellType,
		Timeout: 30 * time.Second,
		Config: map[string]any{
			"args": []string{"--verbose"},
		},
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for missing command in shell config, got nil")
	}

	if !errors.Is(err, ErrShellCommandRequired) {
		t.Fatalf("expected ErrShellCommandRequired, got: %v", err)
	}
}

func TestValidate_Shell_ValidWithArgs(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    ShellType,
		Timeout: 30 * time.Second,
		Config: map[string]any{
			"command": "/usr/local/bin/notify",
			"args":    []string{"--partition", "{{.Table}}"},
		},
	}

	err := hook.Validate()
	if err != nil {
		t.Fatalf("expected no error for valid shell config with args, got: %v", err)
	}
}

func TestValidate_Shell_ValidWithEnv(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    ShellType,
		Timeout: 30 * time.Second,
		Config: map[string]any{
			"command": "/usr/local/bin/archive",
			"env": map[string]string{
				"BUCKET": "my-bucket",
				"PREFIX": "data/{{.Table}}",
			},
		},
	}

	err := hook.Validate()
	if err != nil {
		t.Fatalf("expected no error for valid shell config with env, got: %v", err)
	}
}

func TestValidate_Shell_ValidWithArgsAndEnv(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    ShellType,
		Timeout: 30 * time.Second,
		Config: map[string]any{
			"command": "/usr/local/bin/archive",
			"args":    []string{"--schema", "{{.Schema}}", "--table", "{{.Table}}"},
			"env": map[string]string{
				"BUCKET": "my-archive-bucket",
			},
		},
	}

	err := hook.Validate()
	if err != nil {
		t.Fatalf("expected no error for valid shell config with args and env, got: %v", err)
	}
}

// PostgreSQL config validation tests

func TestValidate_PostgreSQL_MissingConfig(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    PostgreSQLType,
		Timeout: 30 * time.Second,
		Config:  nil,
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for missing postgresql config, got nil")
	}

	if !errors.Is(err, ErrPostgreSQLConfigRequired) {
		t.Fatalf("expected ErrPostgreSQLConfigRequired, got: %v", err)
	}
}

func TestValidate_PostgreSQL_MissingSQLQuery(t *testing.T) {
	hook := HookEntry{
		Name:    "my-hook",
		Type:    PostgreSQLType,
		Timeout: 30 * time.Second,
		Config: map[string]any{
			"other_field": "value",
		},
	}

	err := hook.Validate()
	if err == nil {
		t.Fatal("expected error for missing sql_query in postgresql config, got nil")
	}

	if !errors.Is(err, ErrPostgreSQLQueryRequired) {
		t.Fatalf("expected ErrPostgreSQLQueryRequired, got: %v", err)
	}
}

func TestValidate_PostgreSQL_ValidConfig(t *testing.T) {
	hook := HookEntry{
		Name:    "vacuum-hook",
		Type:    PostgreSQLType,
		Timeout: 30 * time.Second,
		Config: map[string]any{
			"sql_query": "VACUUM ANALYZE {{.Schema}}.{{.Table}}",
		},
	}

	err := hook.Validate()
	if err != nil {
		t.Fatalf("expected no error for valid postgresql config, got: %v", err)
	}
}

// HooksConfig.Validate tests

func TestHooksConfig_Validate_AllValid(t *testing.T) {
	config := HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name:    "notify",
				Type:    ShellType,
				Timeout: 30 * time.Second,
				Config: map[string]any{
					"command": "/usr/bin/notify",
				},
			},
		},
		AfterDetach: []HookEntry{
			{
				Name:    "vacuum",
				Type:    PostgreSQLType,
				Timeout: 30 * time.Second,
				Config: map[string]any{
					"sql_query": "VACUUM ANALYZE public.events",
				},
			},
		},
	}

	err := config.Validate()
	if err != nil {
		t.Fatalf("expected no error for valid HooksConfig, got: %v", err)
	}
}

func TestHooksConfig_Validate_InvalidEntryInBeforeDrop(t *testing.T) {
	config := HooksConfig{
		BeforeDrop: []HookEntry{
			{
				Name:   "",
				Type:   ShellType,
				Config: map[string]any{"command": "/bin/true"},
			},
		},
	}

	err := config.Validate()
	if err == nil {
		t.Fatal("expected error for invalid entry in HooksConfig, got nil")
	}

	if !errors.Is(err, ErrNameRequired) {
		t.Fatalf("expected ErrNameRequired, got: %v", err)
	}
}

func TestHooksConfig_Validate_EmptyConfig(t *testing.T) {
	config := HooksConfig{}

	err := config.Validate()
	if err != nil {
		t.Fatalf("expected no error for empty HooksConfig, got: %v", err)
	}
}

// Package hook provides hook configuration types and lifecycle management for partition cleanup operations.
package hook

import (
	"errors"
	"fmt"
	"time"
)

// LifecycleEvent represents when a hook executes relative to an operation.
type LifecycleEvent string

const (
	BeforeDetach LifecycleEvent = "before-detach"
	AfterDetach  LifecycleEvent = "after-detach"
	BeforeDrop   LifecycleEvent = "before-drop"
	AfterDrop    LifecycleEvent = "after-drop"
)

// HookType identifies the runner implementation.
type HookType string

const (
	ShellType      HookType = "shell"
	PostgreSQLType HookType = "postgresql"
)

// OnFailure defines behavior when a hook fails.
type OnFailure string

const (
	OnFailureAbort    OnFailure = "abort"
	OnFailureContinue OnFailure = "continue"
)

// BackoffStrategy defines the retry delay strategy.
type BackoffStrategy string

const (
	BackoffFixed       BackoffStrategy = "fixed"
	BackoffExponential BackoffStrategy = "exponential"
)

// Default values for hook configuration.
const (
	DefaultInitialDelay = 5 * time.Second
	DefaultMaxDelay     = 60 * time.Second
)

// Validation errors for hook configuration.
var (
	ErrNameRequired             = errors.New("hook name is required")
	ErrTypeRequired             = errors.New("type is required")
	ErrTimeoutRequired          = errors.New("timeout is required")
	ErrInvalidType              = errors.New("invalid type, must be one of [shell, postgresql]")
	ErrInvalidOnFailure         = errors.New("invalid on_failure, must be one of [abort, continue]")
	ErrInvalidBackoff           = errors.New("invalid retry backoff, must be one of [fixed, exponential]")
	ErrShellConfigRequired      = errors.New("config section is required for shell hooks")
	ErrShellCommandRequired     = errors.New("'command' is required in config for shell hooks")
	ErrPostgreSQLConfigRequired = errors.New("config section is required for postgresql hooks")
	ErrPostgreSQLQueryRequired  = errors.New("'sql_query' is required in config for postgresql hooks")
)

// RetryConfig defines retry behavior for a hook.
type RetryConfig struct {
	Attempts     int             `mapstructure:"attempts"`
	Backoff      BackoffStrategy `mapstructure:"backoff"`
	InitialDelay time.Duration   `mapstructure:"initial_delay"`
	MaxDelay     time.Duration   `mapstructure:"max_delay"`
}

// HookEntry represents a single hook definition in configuration.
type HookEntry struct {
	Name      string                 `mapstructure:"name"`
	Type      HookType               `mapstructure:"type"`
	Enabled   *bool                  `mapstructure:"enabled"`
	Timeout   time.Duration          `mapstructure:"timeout"`
	OnFailure OnFailure              `mapstructure:"on_failure"`
	Retry     RetryConfig            `mapstructure:"retry"`
	Config    map[string]interface{} `mapstructure:"config"`
}

// ShellConfig holds shell hook-specific configuration.
type ShellConfig struct {
	Command              string            `mapstructure:"command"`
	Args                 []string          `mapstructure:"args"`
	Env                  map[string]string `mapstructure:"env"`
	PropagateCredentials bool              `mapstructure:"propagate-credentials"`
}

// HooksConfig groups hooks by lifecycle event.
type HooksConfig struct {
	BeforeDetach []HookEntry `mapstructure:"before-detach"`
	AfterDetach  []HookEntry `mapstructure:"after-detach"`
	BeforeDrop   []HookEntry `mapstructure:"before-drop"`
	AfterDrop    []HookEntry `mapstructure:"after-drop"`
}

// ApplyDefaults sets default values on a HookEntry for any unset optional fields.
func (h *HookEntry) ApplyDefaults() {
	if h.Enabled == nil {
		enabled := true
		h.Enabled = &enabled
	}

	if h.Retry.Backoff == "" {
		h.Retry.Backoff = BackoffExponential
	}

	if h.Retry.InitialDelay == 0 {
		h.Retry.InitialDelay = DefaultInitialDelay
	}

	if h.Retry.MaxDelay == 0 {
		h.Retry.MaxDelay = DefaultMaxDelay
	}
}

// Validate checks that a HookEntry has all required fields and valid values.
func (h *HookEntry) Validate() error {
	if h.Name == "" {
		return ErrNameRequired
	}

	if h.Type == "" {
		return fmt.Errorf("hook '%s': %w", h.Name, ErrTypeRequired)
	}

	if h.Timeout == 0 {
		return fmt.Errorf("hook '%s': %w", h.Name, ErrTimeoutRequired)
	}

	if _, ok := registry[h.Type]; !ok {
		return fmt.Errorf("hook '%s': %w", h.Name, ErrInvalidType)
	}

	if h.OnFailure != "" && h.OnFailure != OnFailureAbort && h.OnFailure != OnFailureContinue {
		return fmt.Errorf("hook '%s': %w", h.Name, ErrInvalidOnFailure)
	}

	if h.Retry.Backoff != "" && h.Retry.Backoff != BackoffFixed && h.Retry.Backoff != BackoffExponential {
		return fmt.Errorf("hook '%s': %w", h.Name, ErrInvalidBackoff)
	}

	if err := h.validateTypeConfig(); err != nil {
		return fmt.Errorf("hook '%s': %w", h.Name, err)
	}

	return nil
}

func (h *HookEntry) validateTypeConfig() error {
	handler, ok := registry[h.Type]
	if !ok {
		return nil // unknown types are rejected earlier by the Type validity check
	}

	return handler.validate(h.Config)
}

// hooksFor returns the hook entries configured for the given lifecycle event.
func (c *HooksConfig) hooksFor(event LifecycleEvent) []HookEntry {
	switch event {
	case BeforeDetach:
		return c.BeforeDetach
	case AfterDetach:
		return c.AfterDetach
	case BeforeDrop:
		return c.BeforeDrop
	case AfterDrop:
		return c.AfterDrop
	default:
		return nil
	}
}

// allEvents lists the lifecycle events in execution order, for iterating over every hook slice.
var allEvents = []LifecycleEvent{BeforeDetach, AfterDetach, BeforeDrop, AfterDrop}

// ApplyDefaults sets default values on all hook entries in the configuration.
func (c *HooksConfig) ApplyDefaults() {
	for _, event := range allEvents {
		hooks := c.hooksFor(event)
		for i := range hooks {
			hooks[i].ApplyDefaults()
		}
	}
}

// Validate checks all hook entries across all lifecycle events.
func (c *HooksConfig) Validate() error {
	for _, event := range allEvents {
		hooks := c.hooksFor(event)
		for i := range hooks {
			if err := hooks[i].Validate(); err != nil {
				return fmt.Errorf("hooks.%s[%d]: %w", string(event), i, err)
			}
		}
	}

	return nil
}

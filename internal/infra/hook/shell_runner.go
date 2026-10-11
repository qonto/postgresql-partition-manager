package hook

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
)

// Compile-time checks.
var (
	_ Runner         = (*ShellRunner)(nil)
	_ RenderedConfig = (*ShellConfig)(nil)
)

// LogAttrs implements RenderedConfig, returning the resolved shell fields for structured logging.
func (c *ShellConfig) LogAttrs() []any {
	attrs := []any{
		"command", c.Command,
		"args", c.Args,
		"propagate_credentials", c.PropagateCredentials,
	}

	if len(c.Env) > 0 {
		attrs = append(attrs, "env", c.Env)
	}

	return attrs
}

// validateShellConfig checks that a shell hook's raw config has the required fields.
func validateShellConfig(config map[string]interface{}) error {
	if config == nil {
		return ErrShellConfigRequired
	}

	if _, ok := config["command"]; !ok {
		return ErrShellCommandRequired
	}

	return nil
}

// resolveShellConfig renders template variables in shell hook configuration fields.
func resolveShellConfig(config map[string]interface{}, partition PartitionContext) (RenderedConfig, error) {
	shell := &ShellConfig{}

	if cmd, ok := config["command"]; ok {
		rendered, err := Render(fmt.Sprintf("%v", cmd), partition)
		if err != nil {
			return nil, fmt.Errorf("rendering command: %w", err)
		}

		shell.Command = rendered
	}

	if args, ok := config["args"].([]interface{}); ok {
		shell.Args = make([]string, 0, len(args))

		for _, arg := range args {
			rendered, err := Render(fmt.Sprintf("%v", arg), partition)
			if err != nil {
				return nil, fmt.Errorf("rendering arg: %w", err)
			}

			shell.Args = append(shell.Args, rendered)
		}
	}

	if envMap, ok := config["env"].(map[string]interface{}); ok {
		shell.Env = make(map[string]string, len(envMap))

		for k, v := range envMap {
			rendered, err := Render(fmt.Sprintf("%v", v), partition)
			if err != nil {
				return nil, fmt.Errorf("rendering env var %q: %w", k, err)
			}

			shell.Env[k] = rendered
		}
	}

	if propagate, ok := config["propagate-credentials"].(bool); ok {
		shell.PropagateCredentials = propagate
	}

	return shell, nil
}

// ShellRunner executes shell hook commands using exec.CommandContext.
type ShellRunner struct {
	logger slog.Logger
}

// NewShellRunner creates a new ShellRunner with the given logger.
func NewShellRunner(logger slog.Logger) *ShellRunner {
	return &ShellRunner{
		logger: logger,
	}
}

// Run executes the shell command defined in the resolved hook.
// It uses exec.CommandContext to support timeout via context cancellation.
// The command inherits the parent process environment variables, with
// hook-specific env vars merged on top. When PropagateCredentials is true,
// PG* environment variables are injected from the connection URL.
func (r *ShellRunner) Run(ctx context.Context, hook *ResolvedHook) error {
	shell, ok := hook.Config.(*ShellConfig)
	if !ok {
		return fmt.Errorf("shell configuration is nil for hook %q", hook.Name)
	}

	cmd := exec.CommandContext(ctx, shell.Command, shell.Args...)

	// Start with parent process environment variables
	env := os.Environ()

	// Merge additional env vars from hook config
	for key, value := range shell.Env {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}

	// When propagateCredentials is true, inject PG* env vars
	if shell.PropagateCredentials {
		credentials, err := ExtractCredentials(hook.ConnectionURL)
		if err != nil {
			return fmt.Errorf("failed to extract credentials for hook %q: %w", hook.Name, err)
		}

		for key, value := range credentials {
			env = append(env, fmt.Sprintf("%s=%s", key, value))
		}
	}

	cmd.Env = env

	// Capture stdout and stderr for debug logging
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	r.logger.Debug("Executing shell hook",
		"hook", hook.Name,
		"command", shell.Command,
		"args", shell.Args,
	)

	err := cmd.Run()

	if stdout.Len() > 0 {
		r.logger.Debug("Shell hook stdout", "hook", hook.Name, "stdout", stdout.String())
	}

	if stderr.Len() > 0 {
		r.logger.Debug("Shell hook stderr", "hook", hook.Name, "stderr", stderr.String())
	}

	if err != nil {
		return fmt.Errorf("shell hook %q failed: %w", hook.Name, err)
	}

	return nil
}

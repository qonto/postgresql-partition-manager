package convert

import (
	"testing"

	"github.com/spf13/cobra"
)

// --- Flag Registration Tests ---

func TestSetupCmd_HasDryRunFlag(t *testing.T) {
	cmd := SetupCmd()

	flag := cmd.Flags().Lookup("dry-run")
	if flag == nil {
		t.Fatal("expected --dry-run flag to be registered on setup command")
	}

	if flag.DefValue != "false" {
		t.Errorf("expected --dry-run default to be 'false', got %q", flag.DefValue)
	}
}

func TestBackfillCmd_HasDryRunFlag(t *testing.T) {
	cmd := BackfillCmd()

	flag := cmd.Flags().Lookup("dry-run")
	if flag == nil {
		t.Fatal("expected --dry-run flag to be registered on backfill command")
	}

	if flag.DefValue != "false" {
		t.Errorf("expected --dry-run default to be 'false', got %q", flag.DefValue)
	}
}

func TestReplayCmd_HasDryRunFlag(t *testing.T) {
	cmd := ReplayCmd()

	flag := cmd.Flags().Lookup("dry-run")
	if flag == nil {
		t.Fatal("expected --dry-run flag to be registered on replay command")
	}

	if flag.DefValue != "false" {
		t.Errorf("expected --dry-run default to be 'false', got %q", flag.DefValue)
	}
}

func TestVerifyCmd_HasDryRunFlag(t *testing.T) {
	cmd := VerifyCmd()

	flag := cmd.Flags().Lookup("dry-run")
	if flag == nil {
		t.Fatal("expected --dry-run flag to be registered on verify command")
	}

	if flag.DefValue != "false" {
		t.Errorf("expected --dry-run default to be 'false', got %q", flag.DefValue)
	}
}

func TestCutoverCmd_HasDryRunFlag(t *testing.T) {
	cmd := CutoverCmd()

	flag := cmd.Flags().Lookup("dry-run")
	if flag == nil {
		t.Fatal("expected --dry-run flag to be registered on cutover command")
	}

	if flag.DefValue != "false" {
		t.Errorf("expected --dry-run default to be 'false', got %q", flag.DefValue)
	}
}

func TestRollbackCmd_HasDryRunFlag(t *testing.T) {
	cmd := RollbackCmd()

	flag := cmd.Flags().Lookup("dry-run")
	if flag == nil {
		t.Fatal("expected --dry-run flag to be registered on rollback command")
	}

	if flag.DefValue != "false" {
		t.Errorf("expected --dry-run default to be 'false', got %q", flag.DefValue)
	}
}

func TestCleanupCmd_HasDryRunFlag(t *testing.T) {
	cmd := CleanupCmd()

	flag := cmd.Flags().Lookup("dry-run")
	if flag == nil {
		t.Fatal("expected --dry-run flag to be registered on cleanup command")
	}

	if flag.DefValue != "false" {
		t.Errorf("expected --dry-run default to be 'false', got %q", flag.DefValue)
	}
}

// --- Cleanup-specific Flag Tests ---

func TestCleanupCmd_HasConfirmFlag(t *testing.T) {
	cmd := CleanupCmd()

	flag := cmd.Flags().Lookup("confirm")
	if flag == nil {
		t.Fatal("expected --confirm flag to be registered on cleanup command")
	}

	if flag.DefValue != "false" {
		t.Errorf("expected --confirm default to be 'false', got %q", flag.DefValue)
	}
}

func TestCleanupCmd_HasForceFlag(t *testing.T) {
	cmd := CleanupCmd()

	flag := cmd.Flags().Lookup("force")
	if flag == nil {
		t.Fatal("expected --force flag to be registered on cleanup command")
	}

	if flag.DefValue != "false" {
		t.Errorf("expected --force default to be 'false', got %q", flag.DefValue)
	}
}

func TestCleanupCmd_FlagParsing_ConfirmAndForce(t *testing.T) {
	cmd := CleanupCmd()
	cmd.SetArgs([]string{"my-table", "--confirm", "--force"})

	// Parse flags without executing the Run function
	err := cmd.ParseFlags([]string{"my-table", "--confirm", "--force"})
	if err != nil {
		t.Fatalf("unexpected error parsing flags: %v", err)
	}

	confirmVal, err := cmd.Flags().GetBool("confirm")
	if err != nil {
		t.Fatalf("unexpected error getting confirm flag: %v", err)
	}

	if !confirmVal {
		t.Error("expected --confirm to be true after parsing")
	}

	forceVal, err := cmd.Flags().GetBool("force")
	if err != nil {
		t.Fatalf("unexpected error getting force flag: %v", err)
	}

	if !forceVal {
		t.Error("expected --force to be true after parsing")
	}
}

func TestCleanupCmd_FlagParsing_DryRunWithConfirm(t *testing.T) {
	cmd := CleanupCmd()

	err := cmd.ParseFlags([]string{"my-table", "--dry-run", "--confirm"})
	if err != nil {
		t.Fatalf("unexpected error parsing flags: %v", err)
	}

	dryRunVal, err := cmd.Flags().GetBool("dry-run")
	if err != nil {
		t.Fatalf("unexpected error getting dry-run flag: %v", err)
	}

	if !dryRunVal {
		t.Error("expected --dry-run to be true after parsing")
	}

	confirmVal, err := cmd.Flags().GetBool("confirm")
	if err != nil {
		t.Fatalf("unexpected error getting confirm flag: %v", err)
	}

	if !confirmVal {
		t.Error("expected --confirm to be true after parsing")
	}
}

func TestCleanupCmd_FlagParsing_ForceWithoutConfirm(t *testing.T) {
	cmd := CleanupCmd()

	err := cmd.ParseFlags([]string{"my-table", "--force"})
	if err != nil {
		t.Fatalf("unexpected error parsing flags: %v", err)
	}

	forceVal, err := cmd.Flags().GetBool("force")
	if err != nil {
		t.Fatalf("unexpected error getting force flag: %v", err)
	}

	if !forceVal {
		t.Error("expected --force to be true after parsing")
	}

	confirmVal, err := cmd.Flags().GetBool("confirm")
	if err != nil {
		t.Fatalf("unexpected error getting confirm flag: %v", err)
	}

	if confirmVal {
		t.Error("expected --confirm to be false when not provided")
	}
}

// --- DryRun Flag Parsing Tests ---

func TestSetupCmd_FlagParsing_DryRunTrue(t *testing.T) {
	cmd := SetupCmd()

	err := cmd.ParseFlags([]string{"my-table", "--dry-run"})
	if err != nil {
		t.Fatalf("unexpected error parsing flags: %v", err)
	}

	dryRunVal, err := cmd.Flags().GetBool("dry-run")
	if err != nil {
		t.Fatalf("unexpected error getting dry-run flag: %v", err)
	}

	if !dryRunVal {
		t.Error("expected --dry-run to be true after parsing")
	}
}

func TestBackfillCmd_FlagParsing_DryRunTrue(t *testing.T) {
	cmd := BackfillCmd()

	err := cmd.ParseFlags([]string{"my-table", "--dry-run"})
	if err != nil {
		t.Fatalf("unexpected error parsing flags: %v", err)
	}

	dryRunVal, err := cmd.Flags().GetBool("dry-run")
	if err != nil {
		t.Fatalf("unexpected error getting dry-run flag: %v", err)
	}

	if !dryRunVal {
		t.Error("expected --dry-run to be true after parsing")
	}
}

func TestCutoverCmd_FlagParsing_DryRunTrue(t *testing.T) {
	cmd := CutoverCmd()

	err := cmd.ParseFlags([]string{"my-table", "--dry-run"})
	if err != nil {
		t.Fatalf("unexpected error parsing flags: %v", err)
	}

	dryRunVal, err := cmd.Flags().GetBool("dry-run")
	if err != nil {
		t.Fatalf("unexpected error getting dry-run flag: %v", err)
	}

	if !dryRunVal {
		t.Error("expected --dry-run to be true after parsing")
	}
}

// --- Command Structure Tests ---

func TestConvertCmd_RegistersAllSubCommands(t *testing.T) {
	cmd := ConvertCmd()

	expectedSubCommands := []string{
		"setup",
		"backfill",
		"replay",
		"verify",
		"cutover",
		"rollback",
		"cleanup",
	}

	for _, name := range expectedSubCommands {
		subCmd, _, err := cmd.Find([]string{name})
		if err != nil {
			t.Errorf("expected sub-command %q to be registered, got error: %v", name, err)
		}

		if subCmd == nil || subCmd.Name() != name {
			t.Errorf("expected sub-command %q to be found", name)
		}
	}
}

func TestConvertCmd_SubCommandsRequireExactlyOneArg(t *testing.T) {
	subCommands := []*cobra.Command{
		SetupCmd(),
		BackfillCmd(),
		ReplayCmd(),
		VerifyCmd(),
		CutoverCmd(),
		RollbackCmd(),
		CleanupCmd(),
	}

	for _, cmd := range subCommands {
		t.Run(cmd.Name(), func(t *testing.T) {
			if cmd.Args == nil {
				t.Errorf("expected %s command to have Args validation", cmd.Name())
				return
			}

			// Test with no args - should fail
			err := cmd.Args(cmd, []string{})
			if err == nil {
				t.Errorf("expected %s command to reject zero arguments", cmd.Name())
			}

			// Test with one arg - should pass
			err = cmd.Args(cmd, []string{"my-table"})
			if err != nil {
				t.Errorf("expected %s command to accept one argument, got: %v", cmd.Name(), err)
			}

			// Test with two args - should fail
			err = cmd.Args(cmd, []string{"table1", "table2"})
			if err == nil {
				t.Errorf("expected %s command to reject two arguments", cmd.Name())
			}
		})
	}
}

// --- Exit Code Constants Tests ---

func TestExitCodes_SetupDefinedValues(t *testing.T) {
	// Verify the exit codes defined in setup.go have the expected values.
	// These are the primary exit codes used across all sub-commands.
	tests := []struct {
		name     string
		code     int
		expected int
	}{
		{"InvalidConfiguration", InvalidConfigurationExitCode, 1},
		{"InternalError", InternalErrorExitCode, 2},
		{"DatabaseError", DatabaseErrorExitCode, 3},
		{"MissingTable", MissingTableExitCode, 4},
		{"NoPrimaryKey", NoPrimaryKeyExitCode, 5},
		{"SetupFailed", SetupFailedExitCode, 6},
		{"LockTimeout", LockTimeoutExitCode, 7},
		{"AssertionFailed", AssertionFailedExitCode, 8},
		{"RollbackNotApplicable", RollbackNotApplicableExitCode, 9},
		{"VerifyNotReady", VerifyNotReadyExitCode, 10},
		{"CutoverFailed", CutoverFailedExitCode, 11},
		{"RollbackFailed", RollbackFailedExitCode, 12},
		{"CleanupFailed", CleanupFailedExitCode, 13},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.code != tt.expected {
				t.Errorf("expected %s to be %d, got %d", tt.name, tt.expected, tt.code)
			}
		})
	}
}

func TestExitCodes_NonZeroForErrors(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{"InvalidConfigurationExitCode", InvalidConfigurationExitCode},
		{"InternalErrorExitCode", InternalErrorExitCode},
		{"DatabaseErrorExitCode", DatabaseErrorExitCode},
		{"MissingTableExitCode", MissingTableExitCode},
		{"NoPrimaryKeyExitCode", NoPrimaryKeyExitCode},
		{"SetupFailedExitCode", SetupFailedExitCode},
		{"LockTimeoutExitCode", LockTimeoutExitCode},
		{"AssertionFailedExitCode", AssertionFailedExitCode},
		{"RollbackNotApplicableExitCode", RollbackNotApplicableExitCode},
		{"VerifyNotReadyExitCode", VerifyNotReadyExitCode},
		{"CutoverFailedExitCode", CutoverFailedExitCode},
		{"RollbackFailedExitCode", RollbackFailedExitCode},
		{"CleanupFailedExitCode", CleanupFailedExitCode},
		{"BackfillFailedExitCode", BackfillFailedExitCode},
		{"ReplayFailedExitCode", ReplayFailedExitCode},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.code == 0 {
				t.Errorf("%s should be non-zero for error conditions", tt.name)
			}
		})
	}
}

// --- Cleanup-only Flags Not Present on Other Commands ---

func TestNonCleanupCommands_DoNotHaveConfirmFlag(t *testing.T) {
	commands := map[string]*cobra.Command{
		"setup":    SetupCmd(),
		"backfill": BackfillCmd(),
		"replay":   ReplayCmd(),
		"verify":   VerifyCmd(),
		"cutover":  CutoverCmd(),
		"rollback": RollbackCmd(),
	}

	for name, cmd := range commands {
		t.Run(name, func(t *testing.T) {
			flag := cmd.Flags().Lookup("confirm")
			if flag != nil {
				t.Errorf("expected %s command to NOT have --confirm flag", name)
			}
		})
	}
}

func TestNonCleanupCommands_DoNotHaveForceFlag(t *testing.T) {
	commands := map[string]*cobra.Command{
		"setup":    SetupCmd(),
		"backfill": BackfillCmd(),
		"replay":   ReplayCmd(),
		"verify":   VerifyCmd(),
		"cutover":  CutoverCmd(),
		"rollback": RollbackCmd(),
	}

	for name, cmd := range commands {
		t.Run(name, func(t *testing.T) {
			flag := cmd.Flags().Lookup("force")
			if flag != nil {
				t.Errorf("expected %s command to NOT have --force flag", name)
			}
		})
	}
}

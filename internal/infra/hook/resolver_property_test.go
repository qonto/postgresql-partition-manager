// Feature: partition-hooks, Property 1: Hook Resolution Override
package hook

import (
	"testing"

	"pgregory.net/rapid"
)

// **Validates: Requirements 1.3, 1.4**
//
// Property 1: Hook Resolution Override
// For any partition configuration with hooks defined at both global and partition levels,
// the resolved hooks for that partition SHALL equal the partition-level hooks, completely
// overriding the global hooks. Conversely, for any partition without its own hooks section,
// the resolved hooks SHALL equal the global hooks.

// genHooksConfig generates a random non-nil HooksConfig with random lifecycle hooks.
func genHooksConfig(t *rapid.T, label string) *HooksConfig {
	numBeforeDetach := rapid.IntRange(0, 3).Draw(t, label+"_numBeforeDetach")
	numAfterDetach := rapid.IntRange(0, 3).Draw(t, label+"_numAfterDetach")
	numBeforeDrop := rapid.IntRange(0, 3).Draw(t, label+"_numBeforeDrop")
	numAfterDrop := rapid.IntRange(0, 3).Draw(t, label+"_numAfterDrop")

	config := &HooksConfig{}

	for i := 0; i < numBeforeDetach; i++ {
		config.BeforeDetach = append(config.BeforeDetach, genRandomHookEntry(t, label+"_bd"))
	}

	for i := 0; i < numAfterDetach; i++ {
		config.AfterDetach = append(config.AfterDetach, genRandomHookEntry(t, label+"_ad"))
	}

	for i := 0; i < numBeforeDrop; i++ {
		config.BeforeDrop = append(config.BeforeDrop, genRandomHookEntry(t, label+"_bdr"))
	}

	for i := 0; i < numAfterDrop; i++ {
		config.AfterDrop = append(config.AfterDrop, genRandomHookEntry(t, label+"_adr"))
	}

	return config
}

// genRandomHookEntry generates a random HookEntry with valid required fields.
func genRandomHookEntry(t *rapid.T, label string) HookEntry {
	hookType := rapid.SampledFrom([]HookType{ShellType, PostgreSQLType}).Draw(t, label+"_type")
	name := rapid.StringMatching(`[a-z][a-z0-9\-]{1,20}`).Draw(t, label+"_name")

	var config map[string]any

	switch hookType {
	case ShellType:
		config = map[string]any{
			"command": rapid.StringMatching(`/[a-z/]{1,30}`).Draw(t, label+"_cmd"),
		}
	case PostgreSQLType:
		config = map[string]any{
			"sql_query": rapid.StringMatching(`SELECT [a-z]{1,10}`).Draw(t, label+"_sql"),
		}
	}

	return HookEntry{
		Name:   name,
		Type:   hookType,
		Config: config,
	}
}

func TestProperty_HookResolutionOverride_BothDefined(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		partitionName := rapid.StringMatching(`[a-z][a-z0-9_]{1,20}`).Draw(t, "partitionName")
		globalHooks := genHooksConfig(t, "global")
		partitionHooks := genHooksConfig(t, "partition")

		result := Resolve(partitionName, globalHooks, partitionHooks)

		// When both global and partition hooks are non-nil, Resolve must return partition hooks
		if result != partitionHooks {
			t.Fatalf("expected partition hooks to fully override global hooks, got different pointer")
		}
	})
}

func TestProperty_HookResolutionOverride_GlobalOnly(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		partitionName := rapid.StringMatching(`[a-z][a-z0-9_]{1,20}`).Draw(t, "partitionName")
		globalHooks := genHooksConfig(t, "global")

		result := Resolve(partitionName, globalHooks, nil)

		// When only global hooks are defined, Resolve must return global hooks
		if result != globalHooks {
			t.Fatalf("expected global hooks to be returned when partition hooks is nil, got different pointer")
		}
	})
}

func TestProperty_HookResolutionOverride_PartitionOnly(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		partitionName := rapid.StringMatching(`[a-z][a-z0-9_]{1,20}`).Draw(t, "partitionName")
		partitionHooks := genHooksConfig(t, "partition")

		result := Resolve(partitionName, nil, partitionHooks)

		// When only partition hooks are defined, Resolve must return partition hooks
		if result != partitionHooks {
			t.Fatalf("expected partition hooks to be returned when global hooks is nil, got different pointer")
		}
	})
}

func TestProperty_HookResolutionOverride_NeitherDefined(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		partitionName := rapid.StringMatching(`[a-z][a-z0-9_]{1,20}`).Draw(t, "partitionName")

		result := Resolve(partitionName, nil, nil)

		// When neither is defined, Resolve must return nil
		if result != nil {
			t.Fatalf("expected nil when both global and partition hooks are nil, got %v", result)
		}
	})
}

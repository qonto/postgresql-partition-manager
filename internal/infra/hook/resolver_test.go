package hook

import "testing"

// **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5**

func TestResolve_GlobalOnly(t *testing.T) {
	globalHooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name: "notify",
				Type: ShellType,
				Config: map[string]any{
					"command": "/usr/bin/notify",
				},
			},
		},
	}

	result := Resolve("events_2024_01", globalHooks, nil)

	if result != globalHooks {
		t.Fatal("expected global hooks to be returned when partition hooks is nil")
	}
}

func TestResolve_PartitionOnly(t *testing.T) {
	partitionHooks := &HooksConfig{
		BeforeDrop: []HookEntry{
			{
				Name: "archive",
				Type: ShellType,
				Config: map[string]any{
					"command": "/usr/local/bin/archive",
				},
			},
		},
	}

	result := Resolve("events_2024_01", nil, partitionHooks)

	if result != partitionHooks {
		t.Fatal("expected partition hooks to be returned when global hooks is nil")
	}
}

func TestResolve_BothDefined_PartitionOverridesGlobal(t *testing.T) {
	globalHooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name: "global-notify",
				Type: ShellType,
				Config: map[string]any{
					"command": "/usr/bin/global-notify",
				},
			},
		},
		AfterDetach: []HookEntry{
			{
				Name: "vacuum",
				Type: PostgreSQLType,
				Config: map[string]any{
					"sql_query": "VACUUM ANALYZE public.events",
				},
			},
		},
	}

	partitionHooks := &HooksConfig{
		BeforeDrop: []HookEntry{
			{
				Name: "partition-archive",
				Type: ShellType,
				Config: map[string]any{
					"command": "/usr/local/bin/archive-partition",
				},
			},
		},
	}

	result := Resolve("events_2024_01", globalHooks, partitionHooks)

	if result != partitionHooks {
		t.Fatal("expected partition hooks to fully override global hooks")
	}

	if result == globalHooks {
		t.Fatal("expected global hooks to be overridden by partition hooks")
	}
}

func TestResolve_NeitherDefined(t *testing.T) {
	result := Resolve("events_2024_01", nil, nil)

	if result != nil {
		t.Fatalf("expected nil when no hooks are defined, got %v", result)
	}
}

func TestResolve_EmptyGlobalHooks(t *testing.T) {
	globalHooks := &HooksConfig{}

	result := Resolve("events_2024_01", globalHooks, nil)

	if result != globalHooks {
		t.Fatal("expected empty global hooks to be returned (non-nil pointer)")
	}
}

func TestResolve_EmptyPartitionHooks(t *testing.T) {
	globalHooks := &HooksConfig{
		BeforeDetach: []HookEntry{
			{
				Name: "global-hook",
				Type: ShellType,
				Config: map[string]any{
					"command": "/usr/bin/echo",
				},
			},
		},
	}

	partitionHooks := &HooksConfig{}

	result := Resolve("events_2024_01", globalHooks, partitionHooks)

	if result != partitionHooks {
		t.Fatal("expected empty partition hooks to override global hooks (partition pointer returned)")
	}
}

package hook

// Resolve determines which hooks apply to a given partition based on the resolution rules:
// 1. If partition-level hooks are defined (non-nil), use them (full override of global)
// 2. If only global hooks are defined, use global hooks
// 3. If neither is defined, return nil (no hooks to execute)
func Resolve(partitionName string, globalHooks *HooksConfig, partitionHooks *HooksConfig) *HooksConfig {
	if partitionHooks != nil {
		return partitionHooks
	}

	if globalHooks != nil {
		return globalHooks
	}

	return nil
}

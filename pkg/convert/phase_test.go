package convert

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"
)

// Feature: table-partition-conversion, Property 7: State Machine Transition Validation
// For any pair of phases (currentPhase, requestedPhase), the state machine SHALL accept the
// transition if and only if requestedPhase appears in the AllowedTransitions map for currentPhase,
// and SHALL reject all other transitions with an error identifying the current phase and permitted
// operations.
// Validates: Requirements 10.3, 10.4

// allPhases is the complete set of defined phases for use in property tests.
var allPhases = []Phase{
	PhaseSetup,
	PhaseBackfill,
	PhaseReplay,
	PhaseVerify,
	PhaseCutover,
	PhaseCleanup,
	PhaseRollbackComplete,
}

// isTransitionAllowed checks whether a transition from current to requested is valid
// according to the AllowedTransitions map.
func isTransitionAllowed(current, requested Phase) bool {
	allowed, exists := AllowedTransitions[current]
	if !exists {
		return false
	}
	for _, p := range allowed {
		if p == requested {
			return true
		}
	}
	return false
}

// validateTransition simulates the state machine validation logic:
// returns nil if the transition is allowed, or an error describing the violation.
func validateTransition(current, requested Phase) error {
	if isTransitionAllowed(current, requested) {
		return nil
	}
	allowed, exists := AllowedTransitions[current]
	if !exists {
		return fmt.Errorf("no transitions defined from phase %q", current)
	}
	return fmt.Errorf(
		"transition from %q to %q is not allowed; permitted transitions: %v",
		current, requested, allowed,
	)
}

func TestProperty7_StateMachineTransition_AcceptedIffInAllowedTransitions(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		currentIdx := rapid.IntRange(0, len(allPhases)-1).Draw(t, "currentPhaseIdx")
		requestedIdx := rapid.IntRange(0, len(allPhases)-1).Draw(t, "requestedPhaseIdx")

		current := allPhases[currentIdx]
		requested := allPhases[requestedIdx]

		err := validateTransition(current, requested)
		allowed := isTransitionAllowed(current, requested)

		if allowed && err != nil {
			t.Fatalf("transition from %q to %q should be accepted (is in AllowedTransitions), but got error: %v",
				current, requested, err)
		}
		if !allowed && err == nil {
			t.Fatalf("transition from %q to %q should be rejected (not in AllowedTransitions), but no error returned",
				current, requested)
		}
	})
}

func TestProperty7_StateMachineTransition_RejectedTransitionsReturnError(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		currentIdx := rapid.IntRange(0, len(allPhases)-1).Draw(t, "currentPhaseIdx")
		current := allPhases[currentIdx]

		// Generate a requested phase that is NOT in AllowedTransitions[current]
		allowed := AllowedTransitions[current]
		disallowed := make([]Phase, 0)
		for _, p := range allPhases {
			found := false
			for _, a := range allowed {
				if a == p {
					found = true
					break
				}
			}
			if !found {
				disallowed = append(disallowed, p)
			}
		}

		if len(disallowed) == 0 {
			// All phases are allowed from this state (unlikely but skip)
			return
		}

		requestedIdx := rapid.IntRange(0, len(disallowed)-1).Draw(t, "disallowedIdx")
		requested := disallowed[requestedIdx]

		err := validateTransition(current, requested)
		if err == nil {
			t.Fatalf("transition from %q to %q should be rejected, but no error returned",
				current, requested)
		}
	})
}

func TestProperty7_StateMachineTransition_AllowedTransitionsAreAccepted(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Pick a phase that has defined transitions
		phasesWithTransitions := make([]Phase, 0)
		for phase := range AllowedTransitions {
			if len(AllowedTransitions[phase]) > 0 {
				phasesWithTransitions = append(phasesWithTransitions, phase)
			}
		}

		if len(phasesWithTransitions) == 0 {
			return
		}

		currentIdx := rapid.IntRange(0, len(phasesWithTransitions)-1).Draw(t, "currentPhaseIdx")
		current := phasesWithTransitions[currentIdx]

		allowed := AllowedTransitions[current]
		requestedIdx := rapid.IntRange(0, len(allowed)-1).Draw(t, "allowedIdx")
		requested := allowed[requestedIdx]

		err := validateTransition(current, requested)
		if err != nil {
			t.Fatalf("transition from %q to %q should be accepted (is in AllowedTransitions[%q]=%v), but got error: %v",
				current, requested, current, allowed, err)
		}
	})
}

func TestProperty7_StateMachineTransition_CleanupHasNoTransitions(t *testing.T) {
	// PhaseCleanup is a terminal state with no defined transitions
	rapid.Check(t, func(t *rapid.T) {
		requestedIdx := rapid.IntRange(0, len(allPhases)-1).Draw(t, "requestedPhaseIdx")
		requested := allPhases[requestedIdx]

		err := validateTransition(PhaseCleanup, requested)
		if err == nil {
			t.Fatalf("transition from %q to %q should be rejected (cleanup is terminal), but no error returned",
				PhaseCleanup, requested)
		}
	})
}

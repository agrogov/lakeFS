package auth

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// FieldsProvider must be consulted only for keys that are not already in Fields, and a missing
// key must evaluate to false (deny) for every operator.
func TestConditionContext_Lookup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		ctx           *ConditionContext
		key           string
		expectedValue string
		expectedOK    bool
	}{
		{
			name:          "static field",
			ctx:           NewConditionContextWithProvider("10.0.0.1", nil),
			key:           "SourceIp",
			expectedValue: "10.0.0.1",
			expectedOK:    true,
		},
		{
			name: "static field wins over provider",
			ctx: &ConditionContext{
				Fields:         map[string]string{"SourceIp": "10.0.0.1"},
				FieldsProvider: func(string) (string, bool) { return "192.168.0.1", true },
			},
			key:           "SourceIp",
			expectedValue: "10.0.0.1",
			expectedOK:    true,
		},
		{
			name: "provider resolves unknown key",
			ctx: &ConditionContext{
				Fields:         map[string]string{"SourceIp": "10.0.0.1"},
				FieldsProvider: func(key string) (string, bool) { return "value-of-" + key, true },
			},
			key:           "lakefs:RepositoryMetadata/team",
			expectedValue: "value-of-lakefs:RepositoryMetadata/team",
			expectedOK:    true,
		},
		{
			name: "missing key without provider",
			ctx:  NewConditionContext("10.0.0.1"),
			key:  "lakefs:RepositoryMetadata/team",
		},
		{
			name: "provider reports missing key",
			ctx: &ConditionContext{
				Fields:         map[string]string{"SourceIp": "10.0.0.1"},
				FieldsProvider: func(string) (string, bool) { return "", false },
			},
			key: "lakefs:RepositoryMetadata/team",
		},
		{
			name: "nil context",
			ctx:  nil,
			key:  "lakefs:RepositoryMetadata/team",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			value, ok := tt.ctx.Lookup(tt.key)
			require.Equal(t, tt.expectedOK, ok)
			require.Equal(t, tt.expectedValue, value)
		})
	}
}

// TestConditionContext_ProviderInOperators verifies that conditions on lazily provided keys are
// evaluated, and that a key the provider does not know fails the statement (deny) rather than
// passing it.
func TestConditionContext_ProviderInOperators(t *testing.T) {
	t.Parallel()
	const metadataKey = RepositoryMetadataConditionKeyPrefix + "team"
	conditions := map[string]map[string][]string{
		OperatorNameStringLike: {metadataKey: {"analytics*"}},
	}
	notConditions := map[string]map[string][]string{
		OperatorNameStringNotLike: {metadataKey: {"analytics*"}},
	}

	tests := []struct {
		name          string
		provider      func(key string) (string, bool)
		keyPresent    bool
		expectedAllow bool
	}{
		{
			name:          "provided value matches",
			provider:      func(string) (string, bool) { return "analytics-team", true },
			keyPresent:    true,
			expectedAllow: true,
		},
		{
			name:          "provided value does not match",
			provider:      func(string) (string, bool) { return "data-team", true },
			keyPresent:    true,
			expectedAllow: false,
		},
		{
			name:          "missing key denies",
			provider:      func(string) (string, bool) { return "", false },
			expectedAllow: false,
		},
		{
			name:          "no provider denies",
			provider:      nil,
			expectedAllow: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			conditionCtx := NewConditionContextWithProvider("10.0.0.1", tt.provider)

			passed, err := EvaluateConditions(conditions, conditionCtx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedAllow, passed)

			// The negated operator matches when the key is present and does not match, but a missing
			// key must fail - and so deny - for it as well.
			passed, err = EvaluateConditions(notConditions, conditionCtx)
			require.NoError(t, err)
			require.Equal(t, tt.keyPresent && !tt.expectedAllow, passed)
		})
	}
}

func TestNewRepositoryMetadataFields(t *testing.T) {
	t.Parallel()

	t.Run("lazy and memoised", func(t *testing.T) {
		t.Parallel()
		calls := 0
		fields := NewRepositoryMetadataFields(func() (map[string]string, error) {
			calls++
			return map[string]string{"team": "analytics", "owner": "alice"}, nil
		})
		require.Equal(t, 0, calls, "metadata must not be loaded before a condition asks for it")

		value, ok := fields(RepositoryMetadataConditionKeyPrefix + "team")
		require.True(t, ok)
		require.Equal(t, "analytics", value)

		value, ok = fields(RepositoryMetadataConditionKeyPrefix + "owner")
		require.True(t, ok)
		require.Equal(t, "alice", value)

		require.Equal(t, 1, calls, "metadata must be loaded once per request")
	})

	t.Run("missing key", func(t *testing.T) {
		t.Parallel()
		fields := NewRepositoryMetadataFields(func() (map[string]string, error) {
			return map[string]string{"team": "analytics"}, nil
		})

		value, ok := fields(RepositoryMetadataConditionKeyPrefix + "owner")
		require.False(t, ok)
		require.Empty(t, value)
	})

	t.Run("unrelated key does not load metadata", func(t *testing.T) {
		t.Parallel()
		calls := 0
		fields := NewRepositoryMetadataFields(func() (map[string]string, error) {
			calls++
			return nil, nil
		})

		_, ok := fields("SourceIp")
		require.False(t, ok)
		require.Equal(t, 0, calls)
	})

	t.Run("load error reports every key as missing", func(t *testing.T) {
		t.Parallel()
		calls := 0
		fields := NewRepositoryMetadataFields(func() (map[string]string, error) {
			calls++
			return map[string]string{"team": "analytics"}, errors.New("load failed")
		})

		_, ok := fields(RepositoryMetadataConditionKeyPrefix + "team")
		require.False(t, ok, "a load failure must not grant anything")
		_, ok = fields(RepositoryMetadataConditionKeyPrefix + "team")
		require.False(t, ok)
		require.Equal(t, 1, calls, "a failed load is not retried within the request")
	})
}

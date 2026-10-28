package auth

import (
	"errors"
	"testing"
)

func TestIpAddressOperators_Evaluate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		fields               map[string][]string
		contextMap           map[string]string
		shouldErr            bool
		expectedIpAddress    bool // Expected result for IpAddress (negate=false)
		expectedNotIpAddress bool // Expected result for NotIpAddress (negate=true)
	}{
		{
			name:                 "Single IP match - SourceIp field",
			fields:               map[string][]string{"SourceIp": {"192.168.1.1"}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:            false,
			expectedIpAddress:    true,  // IpAddress: match = allow
			expectedNotIpAddress: false, // NotIpAddress: match = deny
		},
		{
			name:                 "Single IP no match - SourceIp field",
			fields:               map[string][]string{"SourceIp": {"192.168.1.1"}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.2"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: no match = deny
			expectedNotIpAddress: true,  // NotIpAddress: no match = allow
		},
		{
			name:                 "CIDR block match - SourceIp field",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "10.255.255.255"},
			shouldErr:            false,
			expectedIpAddress:    true,
			expectedNotIpAddress: false,
		},
		{
			name:                 "CIDR block no match - SourceIp field",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "11.0.0.1"},
			shouldErr:            false,
			expectedIpAddress:    false,
			expectedNotIpAddress: true,
		},
		{
			name:                 "Multiple values OR logic - first matches",
			fields:               map[string][]string{"SourceIp": {"192.168.1.0/24", "10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.100"},
			shouldErr:            false,
			expectedIpAddress:    true,  // IpAddress: any match = allow
			expectedNotIpAddress: false, // NotIpAddress: any match = deny
		},
		{
			name:                 "Multiple values OR logic - second matches",
			fields:               map[string][]string{"SourceIp": {"192.168.1.0/24", "10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "10.1.2.3"},
			shouldErr:            false,
			expectedIpAddress:    true,  // IpAddress: any match = allow
			expectedNotIpAddress: false, // NotIpAddress: any match = deny
		},
		{
			name:                 "Multiple values OR logic - none match",
			fields:               map[string][]string{"SourceIp": {"192.168.1.0/24", "10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": "172.16.0.1"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: no match = deny
			expectedNotIpAddress: true,  // NotIpAddress: no match = allow
		},
		{
			name:                 "Invalid IP format in condition",
			fields:               map[string][]string{"SourceIp": {"invalid.ip"}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:            true,
			expectedIpAddress:    false, // Error cases same for both
			expectedNotIpAddress: false,
		},
		{
			name:                 "Missing field in context",
			fields:               map[string][]string{"SourceIp": {"192.168.1.1"}},
			contextMap:           map[string]string{"OtherField": "192.168.1.1"},
			shouldErr:            false,
			expectedIpAddress:    false,
			expectedNotIpAddress: false,
		},
		{
			name:                 "Empty field value in context",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIp": ""},
			shouldErr:            false,
			expectedIpAddress:    false,
			expectedNotIpAddress: false,
		},
		{
			name:                 "Dynamic field - custom IP field",
			fields:               map[string][]string{"ClientIp": {"203.0.113.0/24"}},
			contextMap:           map[string]string{"ClientIp": "203.0.113.5"},
			shouldErr:            false,
			expectedIpAddress:    true,
			expectedNotIpAddress: false,
		},
		{
			name:                 "Dynamic field - alternate naming",
			fields:               map[string][]string{"SourceIPAddress": {"10.0.0.0/8"}},
			contextMap:           map[string]string{"SourceIPAddress": "10.1.2.3"},
			shouldErr:            false,
			expectedIpAddress:    true,
			expectedNotIpAddress: false,
		},
		{
			name:                 "Multiple fields AND logic - both match",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "10.1.2.3", "ClientIp": "192.168.1.100"},
			shouldErr:            false,
			expectedIpAddress:    true,  // IpAddress: ALL fields match = allow
			expectedNotIpAddress: false, // NotIpAddress: ALL fields match = deny
		},
		{
			name:                 "Multiple fields AND logic - first matches, second missing",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "10.1.2.3"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: not ALL fields match = deny
			expectedNotIpAddress: false, // NotIpAddress: ANY field matches = deny
		},
		{
			name:                 "Multiple fields AND logic - first matches, second no match",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "10.1.2.3", "ClientIp": "172.16.0.1"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: not ALL fields match = deny
			expectedNotIpAddress: false, // NotIpAddress: ANY field matches = deny
		},
		{
			name:                 "Multiple fields AND logic - both no match",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "172.16.0.1", "ClientIp": "10.0.0.1"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: NO fields match = deny
			expectedNotIpAddress: true,  // NotIpAddress: NO fields match = allow
		},
		{
			name:                 "Multiple fields AND logic - first no match, second matches",
			fields:               map[string][]string{"SourceIp": {"10.0.0.0/8"}, "ClientIp": {"192.168.1.0/24"}},
			contextMap:           map[string]string{"SourceIp": "172.16.0.1", "ClientIp": "192.168.1.100"},
			shouldErr:            false,
			expectedIpAddress:    false, // IpAddress: not ALL fields match = deny
			expectedNotIpAddress: false, // NotIpAddress: ANY field matches = deny
		},
		{
			name:                 "Empty values list for field",
			fields:               map[string][]string{"SourceIp": {}},
			contextMap:           map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:            false,
			expectedIpAddress:    false,
			expectedNotIpAddress: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &ConditionContext{Fields: tt.contextMap}

			// Test IpAddress operator
			t.Run("IpAddress", func(t *testing.T) {
				op := &IpAddressOperator{negate: false}
				result, err := op.Evaluate(tt.fields, ctx)

				if tt.shouldErr && err == nil {
					t.Errorf("Expected error but got none")
				}
				if !tt.shouldErr && err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if !tt.shouldErr && result != tt.expectedIpAddress {
					t.Errorf("Expected %v but got %v", tt.expectedIpAddress, result)
				}
			})

			// Test NotIpAddress operator
			t.Run("NotIpAddress", func(t *testing.T) {
				op := &IpAddressOperator{negate: true}
				result, err := op.Evaluate(tt.fields, ctx)

				if tt.shouldErr && err == nil {
					t.Errorf("Expected error but got none")
				}
				if !tt.shouldErr && err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if !tt.shouldErr && result != tt.expectedNotIpAddress {
					t.Errorf("Expected %v but got %v", tt.expectedNotIpAddress, result)
				}
			})
		})
	}
}

func TestStringMatchOperators_Evaluate(t *testing.T) {
	tests := []struct {
		name                  string
		fields                map[string][]string
		contextMap            map[string]string
		expectedStringLike    bool // Expected result for StringLike (negate=false)
		expectedStringNotLike bool // Expected result for StringNotLike (negate=true)
	}{
		{
			name:                  "exact match",
			fields:                map[string][]string{"catalog:TableName": {"analytics-report"}},
			contextMap:            map[string]string{"catalog:TableName": "analytics-report"},
			expectedStringLike:    true,
			expectedStringNotLike: false,
		},
		{
			name:                  "exact no match",
			fields:                map[string][]string{"catalog:TableName": {"analytics-report"}},
			contextMap:            map[string]string{"catalog:TableName": "sales-data"},
			expectedStringLike:    false,
			expectedStringNotLike: true,
		},
		{
			name:                  "wildcard suffix match",
			fields:                map[string][]string{"catalog:TableName": {"analytics-*"}},
			contextMap:            map[string]string{"catalog:TableName": "analytics-report"},
			expectedStringLike:    true,
			expectedStringNotLike: false,
		},
		{
			name:                  "wildcard suffix no match",
			fields:                map[string][]string{"catalog:TableName": {"analytics-*"}},
			contextMap:            map[string]string{"catalog:TableName": "sales-data"},
			expectedStringLike:    false,
			expectedStringNotLike: true,
		},
		{
			name:                  "wildcard prefix match",
			fields:                map[string][]string{"catalog:TableName": {"*-report"}},
			contextMap:            map[string]string{"catalog:TableName": "analytics-report"},
			expectedStringLike:    true,
			expectedStringNotLike: false,
		},
		{
			name:                  "wildcard prefix no match",
			fields:                map[string][]string{"catalog:TableName": {"*-report"}},
			contextMap:            map[string]string{"catalog:TableName": "analytics-daily"},
			expectedStringLike:    false,
			expectedStringNotLike: true,
		},
		{
			name:                  "match all wildcard",
			fields:                map[string][]string{"catalog:TableName": {"*"}},
			contextMap:            map[string]string{"catalog:TableName": "anything"},
			expectedStringLike:    true,
			expectedStringNotLike: false,
		},
		{
			name:                  "single char wildcard match",
			fields:                map[string][]string{"catalog:TableName": {"table-?"}},
			contextMap:            map[string]string{"catalog:TableName": "table-A"},
			expectedStringLike:    true,
			expectedStringNotLike: false,
		},
		{
			name:                  "single char wildcard no match",
			fields:                map[string][]string{"catalog:TableName": {"table-?"}},
			contextMap:            map[string]string{"catalog:TableName": "table-AB"},
			expectedStringLike:    false,
			expectedStringNotLike: true,
		},
		{
			name:                  "multiple values OR logic - first matches",
			fields:                map[string][]string{"catalog:TableName": {"analytics-*", "reports-*"}},
			contextMap:            map[string]string{"catalog:TableName": "analytics-daily"},
			expectedStringLike:    true,
			expectedStringNotLike: false,
		},
		{
			name:                  "multiple values OR logic - second matches",
			fields:                map[string][]string{"catalog:TableName": {"analytics-*", "reports-*"}},
			contextMap:            map[string]string{"catalog:TableName": "reports-weekly"},
			expectedStringLike:    true,
			expectedStringNotLike: false,
		},
		{
			name:                  "multiple values OR logic - none match",
			fields:                map[string][]string{"catalog:TableName": {"analytics-*", "reports-*"}},
			contextMap:            map[string]string{"catalog:TableName": "sales-data"},
			expectedStringLike:    false,
			expectedStringNotLike: true,
		},
		{
			name:                  "missing field in context",
			fields:                map[string][]string{"catalog:TableName": {"analytics-*"}},
			contextMap:            map[string]string{"catalog:ViewName": "analytics-view"},
			expectedStringLike:    false,
			expectedStringNotLike: false,
		},
		{
			name:                  "multiple fields AND logic - both match",
			fields:                map[string][]string{"catalog:TableName": {"analytics-*"}, "catalog:repo": {"my-repo"}},
			contextMap:            map[string]string{"catalog:TableName": "analytics-daily", "catalog:repo": "my-repo"},
			expectedStringLike:    true,
			expectedStringNotLike: false,
		},
		{
			name:                  "multiple fields AND logic - one no match",
			fields:                map[string][]string{"catalog:TableName": {"analytics-*"}, "catalog:repo": {"my-repo"}},
			contextMap:            map[string]string{"catalog:TableName": "analytics-daily", "catalog:repo": "other-repo"},
			expectedStringLike:    false,
			expectedStringNotLike: false,
		},
		{
			name:                  "empty fields - always pass",
			fields:                map[string][]string{},
			contextMap:            map[string]string{"catalog:TableName": "anything"},
			expectedStringLike:    true,
			expectedStringNotLike: true,
		},
		{
			name:                  "empty values list for field",
			fields:                map[string][]string{"catalog:TableName": {}},
			contextMap:            map[string]string{"catalog:TableName": "anything"},
			expectedStringLike:    false,
			expectedStringNotLike: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &ConditionContext{Fields: tt.contextMap}

			t.Run("StringLike", func(t *testing.T) {
				op := &StringMatchOperator{negate: false, match: stringLikeMatch}
				result, err := op.Evaluate(tt.fields, ctx)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if result != tt.expectedStringLike {
					t.Errorf("Expected %v but got %v", tt.expectedStringLike, result)
				}
			})

			t.Run("StringNotLike", func(t *testing.T) {
				op := &StringMatchOperator{negate: true, match: stringLikeMatch}
				result, err := op.Evaluate(tt.fields, ctx)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if result != tt.expectedStringNotLike {
					t.Errorf("Expected %v but got %v", tt.expectedStringNotLike, result)
				}
			})
		})
	}
}

func TestStringMatchOperator_NilContext(t *testing.T) {
	op := &StringMatchOperator{negate: false}
	_, err := op.Evaluate(map[string][]string{"field": {"value"}}, nil)
	if err == nil {
		t.Error("Expected error for nil context")
	}
}

func TestStringEqualsOperators_Evaluate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                    string
		fields                  map[string][]string
		contextMap              map[string]string
		expectedStringEquals    bool // Expected result for StringEquals (negate=false)
		expectedStringNotEquals bool // Expected result for StringNotEquals (negate=true)
	}{
		{
			name:                    "exact match",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-report"}},
			contextMap:              map[string]string{"catalog:TableName": "analytics-report"},
			expectedStringEquals:    true,
			expectedStringNotEquals: false,
		},
		{
			name:                    "exact no match",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-report"}},
			contextMap:              map[string]string{"catalog:TableName": "sales-data"},
			expectedStringEquals:    false,
			expectedStringNotEquals: true,
		},
		{
			name:                    "comparison is case sensitive",
			fields:                  map[string][]string{"catalog:TableName": {"Analytics-Report"}},
			contextMap:              map[string]string{"catalog:TableName": "analytics-report"},
			expectedStringEquals:    false,
			expectedStringNotEquals: true,
		},
		{
			name:                    "comparison is whitespace sensitive",
			fields:                  map[string][]string{"catalog:TableName": {" analytics-report "}},
			contextMap:              map[string]string{"catalog:TableName": "analytics-report"},
			expectedStringEquals:    false,
			expectedStringNotEquals: true,
		},
		{
			name:                    "wildcards are literal - no match",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-*"}},
			contextMap:              map[string]string{"catalog:TableName": "analytics-report"},
			expectedStringEquals:    false,
			expectedStringNotEquals: true,
		},
		{
			name:                    "wildcards are literal - exact match",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-*"}},
			contextMap:              map[string]string{"catalog:TableName": "analytics-*"},
			expectedStringEquals:    true,
			expectedStringNotEquals: false,
		},
		{
			name:                    "question mark is literal",
			fields:                  map[string][]string{"catalog:TableName": {"table-?"}},
			contextMap:              map[string]string{"catalog:TableName": "table-A"},
			expectedStringEquals:    false,
			expectedStringNotEquals: true,
		},
		{
			name:                    "multiple values OR logic - first matches",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-report", "sales-data"}},
			contextMap:              map[string]string{"catalog:TableName": "analytics-report"},
			expectedStringEquals:    true,
			expectedStringNotEquals: false,
		},
		{
			name:                    "multiple values OR logic - second matches",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-report", "sales-data"}},
			contextMap:              map[string]string{"catalog:TableName": "sales-data"},
			expectedStringEquals:    true,
			expectedStringNotEquals: false,
		},
		{
			name:                    "multiple values OR logic - none match",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-report", "sales-data"}},
			contextMap:              map[string]string{"catalog:TableName": "marketing-data"},
			expectedStringEquals:    false,
			expectedStringNotEquals: true,
		},
		{
			name:                    "missing field in context",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-report"}},
			contextMap:              map[string]string{"catalog:ViewName": "analytics-report"},
			expectedStringEquals:    false,
			expectedStringNotEquals: false,
		},
		{
			name:                    "multiple fields AND logic - both match",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-report"}, "catalog:repo": {"my-repo"}},
			contextMap:              map[string]string{"catalog:TableName": "analytics-report", "catalog:repo": "my-repo"},
			expectedStringEquals:    true,
			expectedStringNotEquals: false,
		},
		{
			name:                    "multiple fields AND logic - one no match",
			fields:                  map[string][]string{"catalog:TableName": {"analytics-report"}, "catalog:repo": {"my-repo"}},
			contextMap:              map[string]string{"catalog:TableName": "analytics-report", "catalog:repo": "other-repo"},
			expectedStringEquals:    false,
			expectedStringNotEquals: false,
		},
		{
			name:                    "empty fields - always pass",
			fields:                  map[string][]string{},
			contextMap:              map[string]string{"catalog:TableName": "anything"},
			expectedStringEquals:    true,
			expectedStringNotEquals: true,
		},
		{
			name:                    "empty values list for field",
			fields:                  map[string][]string{"catalog:TableName": {}},
			contextMap:              map[string]string{"catalog:TableName": "anything"},
			expectedStringEquals:    false,
			expectedStringNotEquals: true,
		},
		{
			name:                    "empty context value matches empty pattern",
			fields:                  map[string][]string{"catalog:TableName": {""}},
			contextMap:              map[string]string{"catalog:TableName": ""},
			expectedStringEquals:    true,
			expectedStringNotEquals: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := &ConditionContext{Fields: tt.contextMap}

			t.Run("StringEquals", func(t *testing.T) {
				op, err := OperatorFactory(OperatorNameStringEquals)
				if err != nil {
					t.Fatalf("Unexpected factory error: %v", err)
				}
				result, err := op.Evaluate(tt.fields, ctx)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if result != tt.expectedStringEquals {
					t.Errorf("Expected %v but got %v", tt.expectedStringEquals, result)
				}
			})

			t.Run("StringNotEquals", func(t *testing.T) {
				op, err := OperatorFactory(OperatorNameStringNotEquals)
				if err != nil {
					t.Fatalf("Unexpected factory error: %v", err)
				}
				result, err := op.Evaluate(tt.fields, ctx)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if result != tt.expectedStringNotEquals {
					t.Errorf("Expected %v but got %v", tt.expectedStringNotEquals, result)
				}
			})
		})
	}
}

func TestStringMatchOperator_Validate(t *testing.T) {
	t.Parallel()
	operators := []string{
		OperatorNameStringLike,
		OperatorNameStringNotLike,
		OperatorNameStringEquals,
		OperatorNameStringNotEquals,
	}
	for _, name := range operators {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			op, err := OperatorFactory(name)
			if err != nil {
				t.Fatalf("Unexpected factory error: %v", err)
			}

			t.Run("valid fields", func(t *testing.T) {
				t.Parallel()
				fields := map[string][]string{"catalog:TableName": {"analytics-report", "analytics-*", ""}}
				if err := op.Validate(fields); err != nil {
					t.Errorf("Unexpected validation error: %v", err)
				}
			})

			t.Run("empty field name", func(t *testing.T) {
				t.Parallel()
				fields := map[string][]string{"": {"analytics-report"}}
				err := op.Validate(fields)
				if !errors.Is(err, ErrMissingFieldName) {
					t.Errorf("Expected %v, got %v", ErrMissingFieldName, err)
				}
			})

			t.Run("no fields", func(t *testing.T) {
				t.Parallel()
				if err := op.Validate(map[string][]string{}); err != nil {
					t.Errorf("Unexpected validation error: %v", err)
				}
			})
		})
	}
}

func TestOperatorFactory_StringOperators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		operatorName  string
		pattern       string
		value         string
		expectedMatch bool
	}{
		{name: "StringLike wildcard", operatorName: OperatorNameStringLike, pattern: "analytics-*", value: "analytics-report", expectedMatch: true},
		{name: "StringNotLike wildcard", operatorName: OperatorNameStringNotLike, pattern: "analytics-*", value: "analytics-report", expectedMatch: false},
		{name: "StringEquals exact", operatorName: OperatorNameStringEquals, pattern: "analytics-report", value: "analytics-report", expectedMatch: true},
		{name: "StringEquals does not wildcard", operatorName: OperatorNameStringEquals, pattern: "analytics-*", value: "analytics-report", expectedMatch: false},
		{name: "StringNotEquals exact", operatorName: OperatorNameStringNotEquals, pattern: "analytics-report", value: "sales-data", expectedMatch: true},
		{name: "StringNotEquals does not wildcard", operatorName: OperatorNameStringNotEquals, pattern: "analytics-*", value: "analytics-report", expectedMatch: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			op, err := OperatorFactory(tt.operatorName)
			if err != nil {
				t.Fatalf("Unexpected factory error: %v", err)
			}
			result, err := op.Evaluate(
				map[string][]string{"catalog:TableName": {tt.pattern}},
				&ConditionContext{Fields: map[string]string{"catalog:TableName": tt.value}},
			)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if result != tt.expectedMatch {
				t.Errorf("Expected %v but got %v", tt.expectedMatch, result)
			}
		})
	}
}

func TestOperatorFactory_UnsupportedOperator(t *testing.T) {
	t.Parallel()
	_, err := OperatorFactory("NotAnOperator")
	if !errors.Is(err, ErrUnsupportedConditionOperator) {
		t.Errorf("Expected %v, got %v", ErrUnsupportedConditionOperator, err)
	}
}

func TestEvaluateConditions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		conditions map[string]map[string][]string
		contextMap map[string]string
		shouldErr  bool
		expected   bool
	}{
		{
			name:       "No conditions - always pass",
			conditions: map[string]map[string][]string{},
			contextMap: map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "Single IP condition - match",
			conditions: map[string]map[string][]string{"IpAddress": {"SourceIp": {"192.168.1.1"}}},
			contextMap: map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "Single IP condition - no match",
			conditions: map[string]map[string][]string{"IpAddress": {"SourceIp": {"192.168.1.1"}}},
			contextMap: map[string]string{"SourceIp": "192.168.1.2"},
			shouldErr:  false,
			expected:   false,
		},
		{
			name:       "CIDR condition - match",
			conditions: map[string]map[string][]string{"IpAddress": {"SourceIp": {"10.0.0.0/8"}}},
			contextMap: map[string]string{"SourceIp": "10.5.6.7"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "Unsupported operator",
			conditions: map[string]map[string][]string{"UnsupportedOp": {"SomeField": {"value"}}},
			contextMap: map[string]string{"SomeField": "192.168.1.1"},
			shouldErr:  true,
			expected:   false,
		},
		{
			name:       "Dynamic field - custom field name",
			conditions: map[string]map[string][]string{"IpAddress": {"ClientIp": {"203.0.113.0/24"}}},
			contextMap: map[string]string{"ClientIp": "203.0.113.5"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "NotIpAddress condition - no match",
			conditions: map[string]map[string][]string{"NotIpAddress": {"SourceIp": {"192.168.1.1"}}},
			contextMap: map[string]string{"SourceIp": "192.168.1.2"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "NotIpAddress condition - match",
			conditions: map[string]map[string][]string{"NotIpAddress": {"SourceIp": {"192.168.1.1"}}},
			contextMap: map[string]string{"SourceIp": "192.168.1.1"},
			shouldErr:  false,
			expected:   false,
		},
		{
			name:       "NotIpAddress CIDR condition - no match",
			conditions: map[string]map[string][]string{"NotIpAddress": {"SourceIp": {"10.0.0.0/8"}}},
			contextMap: map[string]string{"SourceIp": "11.0.0.1"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "StringLike condition - match",
			conditions: map[string]map[string][]string{"StringLike": {"catalog:TableName": {"analytics-*"}}},
			contextMap: map[string]string{"catalog:TableName": "analytics-report"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "StringLike condition - no match",
			conditions: map[string]map[string][]string{"StringLike": {"catalog:TableName": {"analytics-*"}}},
			contextMap: map[string]string{"catalog:TableName": "sales-data"},
			shouldErr:  false,
			expected:   false,
		},
		{
			name:       "StringNotLike condition - match filters out",
			conditions: map[string]map[string][]string{"StringNotLike": {"catalog:TableName": {"temp-*"}}},
			contextMap: map[string]string{"catalog:TableName": "temp-scratch"},
			shouldErr:  false,
			expected:   false,
		},
		{
			name:       "StringNotLike condition - no match allows",
			conditions: map[string]map[string][]string{"StringNotLike": {"catalog:TableName": {"temp-*"}}},
			contextMap: map[string]string{"catalog:TableName": "analytics-report"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "StringNotEquals condition - match filters out",
			conditions: map[string]map[string][]string{"StringNotEquals": {"catalog:TableName": {"temp-scratch"}}},
			contextMap: map[string]string{"catalog:TableName": "temp-scratch"},
			shouldErr:  false,
			expected:   false,
		},
		{
			name:       "StringNotEquals condition - no match allows",
			conditions: map[string]map[string][]string{"StringNotEquals": {"catalog:TableName": {"temp-scratch"}}},
			contextMap: map[string]string{"catalog:TableName": "analytics-report"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "StringEquals condition - match",
			conditions: map[string]map[string][]string{"StringEquals": {"catalog:TableName": {"analytics-report"}}},
			contextMap: map[string]string{"catalog:TableName": "analytics-report"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name:       "StringEquals condition - wildcard is literal",
			conditions: map[string]map[string][]string{"StringEquals": {"catalog:TableName": {"analytics-*"}}},
			contextMap: map[string]string{"catalog:TableName": "analytics-report"},
			shouldErr:  false,
			expected:   false,
		},
		{
			name: "IpAddress AND StringNotEquals - both pass",
			conditions: map[string]map[string][]string{
				"IpAddress":       {"SourceIp": {"10.0.0.0/8"}},
				"StringNotEquals": {"catalog:TableName": {"temp-scratch"}},
			},
			contextMap: map[string]string{"SourceIp": "10.1.2.3", "catalog:TableName": "analytics-report"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name: "IpAddress AND StringLike - both pass",
			conditions: map[string]map[string][]string{
				"IpAddress":  {"SourceIp": {"10.0.0.0/8"}},
				"StringLike": {"catalog:TableName": {"analytics-*"}},
			},
			contextMap: map[string]string{"SourceIp": "10.1.2.3", "catalog:TableName": "analytics-report"},
			shouldErr:  false,
			expected:   true,
		},
		{
			name: "IpAddress AND StringLike - IP passes, string fails",
			conditions: map[string]map[string][]string{
				"IpAddress":  {"SourceIp": {"10.0.0.0/8"}},
				"StringLike": {"catalog:TableName": {"analytics-*"}},
			},
			contextMap: map[string]string{"SourceIp": "10.1.2.3", "catalog:TableName": "sales-data"},
			shouldErr:  false,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &ConditionContext{
				Fields: tt.contextMap,
			}

			result, err := EvaluateConditions(tt.conditions, ctx)

			if tt.shouldErr && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.shouldErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.shouldErr && result != tt.expected {
				t.Errorf("Expected %v but got %v", tt.expected, result)
			}
		})
	}
}

func TestNewConditionContext(t *testing.T) {
	t.Parallel()
	clientIP := "203.0.113.5"
	ctx := NewConditionContext(clientIP)

	if ctx.Fields["SourceIp"] != clientIP {
		t.Errorf("Expected SourceIp to be %s, got %s", clientIP, ctx.Fields["SourceIp"])
	}
}

func TestNewConditionContextWithFields(t *testing.T) {
	t.Parallel()
	fields := map[string]string{
		"SourceIp":      "10.1.2.3",
		"ClientIp":      "192.168.1.1",
		"DestinationIp": "172.16.0.1",
	}
	ctx := NewConditionContextWithFields(fields)

	for key, expectedValue := range fields {
		if ctx.Fields[key] != expectedValue {
			t.Errorf("Expected %s to be %s, got %s", key, expectedValue, ctx.Fields[key])
		}
	}
}

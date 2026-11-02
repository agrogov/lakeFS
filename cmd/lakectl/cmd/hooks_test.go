package cmd

import "testing"

// TestAPIEndpointURL pins CLI-9: the endpoint a wrapper binary composes its own API paths on is
// derived from the parsed lakeFS API root, so a deployment under a custom path keeps that path and
// a bare host loses the /api/v1 suffix exactly once.
func TestAPIEndpointURL(t *testing.T) {
	tests := []struct {
		apiRoot string
		want    string
	}{
		{apiRoot: "https://lakefs.example.com/api/v1", want: "https://lakefs.example.com"},
		{apiRoot: "https://lakefs.example.com/api/v1/", want: "https://lakefs.example.com"},
		{apiRoot: "https://lakefs.example.com/lakefs/api/v1", want: "https://lakefs.example.com/lakefs"},
		// An endpoint configured with a path is the API root itself, so nothing is trimmed.
		{apiRoot: "https://lakefs.example.com/custom", want: "https://lakefs.example.com/custom"},
		{apiRoot: "http://localhost:8000/api/v1", want: "http://localhost:8000"},
		// A root without a path stays as it is instead of gaining a "./" segment.
		{apiRoot: "http://localhost:8000", want: "http://localhost:8000"},
	}
	for _, tt := range tests {
		t.Run(tt.apiRoot, func(t *testing.T) {
			got, err := apiEndpointURL(tt.apiRoot)
			if err != nil {
				t.Fatalf("apiEndpointURL(%q): %v", tt.apiRoot, err)
			}
			if got != tt.want {
				t.Errorf("apiEndpointURL(%q) = %q, want %q", tt.apiRoot, got, tt.want)
			}
		})
	}
}

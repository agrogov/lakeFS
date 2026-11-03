package importer

// A small JSON-over-HTTP client shared by the source and target catalog clients. It is
// deliberately not a general Iceberg REST client: it sends one request at a time, decodes a JSON
// response into a caller-provided value, and turns a non-2xx response into a *CatalogError built
// from the protocol's error envelope.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	// maxJSONResponse bounds the response body the client buffers. Catalog responses are small
	// (a namespace listing page, a metadata document); a response above this is a server problem,
	// not a legitimate answer, and buffering it unbounded would be a memory hazard.
	maxJSONResponse = 256 << 20

	// errorBodySnippet bounds the raw body text kept for a response that is not an Iceberg error
	// envelope, so a proxy's HTML error page does not end up whole in a one-line report.
	errorBodySnippet = 512
)

// doer sends catalog requests. authorizer, when set, adds the credentials of the catalog to the
// request; it is a function so that a token obtained or refreshed by one request is used by the
// next without rebuilding the client.
type doer struct {
	client     *http.Client
	catalog    string
	authorizer func(*http.Request) error
}

// do sends one request. body, when not nil, is JSON-encoded. out, when not nil, receives the
// decoded JSON response. A non-2xx status is returned as a *CatalogError.
func (d *doer) do(ctx context.Context, method, rawURL string, body, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("%w: encode request body: %w", ErrInvalidOptions, err)
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
	if err != nil {
		return fmt.Errorf("%w: build %s request: %w", catalogSentinel(d.catalog), method, err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if d.authorizer != nil {
		if err := d.authorizer(req); err != nil {
			return fmt.Errorf("%w: authorize %s request: %w", catalogSentinel(d.catalog), method, err)
		}
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %s %s: %w", catalogSentinel(d.catalog), method, rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxJSONResponse+1))
	if err != nil {
		return fmt.Errorf("%w: read %s %s response: %w", catalogSentinel(d.catalog), method, rawURL, err)
	}
	if int64(len(data)) > maxJSONResponse {
		return fmt.Errorf("%w: %s %s response exceeds %d bytes", catalogSentinel(d.catalog), method, rawURL, maxJSONResponse)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return newCatalogError(d.catalog, method, rawURL, resp.StatusCode, data)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%w: decode %s %s response: %w", catalogSentinel(d.catalog), method, rawURL, err)
	}
	return nil
}

// newCatalogError builds the *CatalogError of a failed response, preferring the protocol envelope
// and falling back to the status and a snippet of the body.
func newCatalogError(catalog, method, rawURL string, status int, body []byte) error {
	catalogErr := &CatalogError{
		Catalog:    catalog,
		Method:     method,
		URL:        rawURL,
		StatusCode: status,
	}
	var envelope catalogErrorEnvelope
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Error.Message != "" {
		catalogErr.Message = envelope.Error.Message
		catalogErr.ErrorType = envelope.Error.Type
	} else {
		catalogErr.Message = snippet(body)
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("%w: %w: %w", catalogSentinel(catalog), ErrNotFound, catalogErr)
	}
	return fmt.Errorf("%w: %w", catalogSentinel(catalog), catalogErr)
}

// snippet returns body as a single-line, bounded string for an error message.
func snippet(body []byte) string {
	text := strings.Join(strings.Fields(string(body)), " ")
	if len(text) > errorBodySnippet {
		text = text[:errorBodySnippet] + "..."
	}
	return text
}

// catalogSentinel maps the catalog name to the error every failure of that side wraps.
func catalogSentinel(catalog string) error {
	if catalog == catalogTarget {
		return ErrTarget
	}
	return ErrSource
}

const (
	catalogSource = "source"
	catalogTarget = "target"
)

package remote

import (
	"context"
	"io"
	"net/http"
)

// Client HTTP client wrapper.
type Client struct {
	client *http.Client
	url    string
}

func NewClient(url string, client *http.Client) *Client {
	if client == nil {
		client = http.DefaultClient
	}
	return &Client{
		client: client,
		url:    url,
	}
}

// Store sends the request to the remote endpoint.
// It returns the HTTP status code, the payload size, and any error.
func (c *Client) Store(ctx context.Context, req []byte) (int, int, error) {
	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.url, io.NopCloser(nil))
	if err != nil {
		return 0, 0, err
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	// Read response body (or discard it)
	_, readErr := io.Copy(io.Discard, resp.Body)

	// If we got a 2xx response, the write succeeded.
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, len(req), readErr
	}

	if readErr != nil {
		return resp.StatusCode, 0, readErr
	}

	return resp.StatusCode, 0, nil
}

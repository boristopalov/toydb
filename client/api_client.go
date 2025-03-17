package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Common errors
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrTimeout     = errors.New("request timed out")
	ErrServerError = errors.New("server error")
)

// KVClient is a client for the key-value database
type RaftKVClient struct {
	baseURL        string
	httpClient     *http.Client
	clientID       string
	requestCounter atomic.Int64
}

// NewRaftKVClient creates a new client for the key-value database
func NewRaftKVClient(serverURL string, timeout time.Duration) *RaftKVClient {
	// Generate a unique client ID using timestamp
	clientID := fmt.Sprintf("client-%s", uuid.New().String())

	return &RaftKVClient{
		baseURL: serverURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		clientID: clientID,
	}
}

// generateRequestID creates a unique request ID for each operation
func (c *RaftKVClient) generateRequestID() string {
	counter := c.requestCounter.Add(1)
	return fmt.Sprintf("%s-%d", c.clientID, counter)
}

// Get retrieves a value for a given key
func (c *RaftKVClient) Get(ctx context.Context, key string) (string, error) {
	// Generate a request ID
	requestID := c.generateRequestID()

	// Create request
	url := fmt.Sprintf("%s/kv/%s", c.baseURL, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Add request ID header
	req.Header.Set("X-Request-ID", requestID)

	// Send request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Handle response
	if resp.StatusCode == http.StatusNotFound {
		return "", ErrKeyNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%w: %s", ErrServerError, resp.Status)
	}

	// Parse response
	var response map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	value, ok := response["value"]
	if !ok {
		return "", fmt.Errorf("response missing 'value' field")
	}

	return value, nil
}

// Put stores a value for a given key
func (c *RaftKVClient) Put(ctx context.Context, key, value string) error {
	// Generate a request ID
	requestID := c.generateRequestID()

	// Create request body with request ID
	payload := map[string]string{
		"value":      value,
		"request_id": requestID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/kv/%s", c.baseURL, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", requestID)

	// Send request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Handle response
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: %s - %s", ErrServerError, resp.Status, string(bodyBytes))
	}

	return nil
}

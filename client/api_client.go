package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Common errors
var (
	ErrKeyNotFound = errors.New("key not found")
	ErrTimeout     = errors.New("request timed out")
	ErrServerError = errors.New("server error")
	ErrNoLeader    = errors.New("no leader available")
)

// LeaderRedirectErr represents a redirection to the leader
type LeaderRedirectErr struct {
	LeaderID string
}

func (e LeaderRedirectErr) Error() string {
	return fmt.Sprintf("not the leader, redirect to: %s", e.LeaderID)
}

// RaftKVClient is a client for the Raft-backed key-value database
type RaftKVClient struct {
	httpClient     *http.Client
	ClientID       string
	requestCounter atomic.Int64
	ServerURLs     map[string]string // Map of node IDs to URLs
	CurrentLeader  string            // Current leader ID
}

// NewRaftKVClient creates a new client for the Raft-backed key-value database
func NewRaftKVClient(serverURLs map[string]string, leaderID string, timeout time.Duration) *RaftKVClient {
	// Generate a unique client ID
	clientID := fmt.Sprintf("client-%s", uuid.New().String())

	// Create a copy of the server URLs map
	urlsCopy := make(map[string]string)
	for id, url := range serverURLs {
		// Ensure http:// prefix exists
		if !strings.HasPrefix(url, "http://") {
			// If it starts with a colon, it's a port, so add localhost
			if strings.HasPrefix(url, ":") {
				url = "http://localhost" + url
			} else {
				url = "http://" + url
			}
		}
		urlsCopy[id] = url
	}

	return &RaftKVClient{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		ClientID:      clientID,
		ServerURLs:    urlsCopy,
		CurrentLeader: leaderID,
	}
}

// AddServer adds or updates a server in the client's server map
func (c *RaftKVClient) AddServer(nodeID, serverURL string) {
	// Ensure http:// prefix exists
	url := serverURL
	if !strings.HasPrefix(url, "http://") {
		// If it starts with a colon, it's a port, so add localhost
		if strings.HasPrefix(url, ":") {
			url = "http://localhost" + url
		} else {
			url = "http://" + url
		}
	}
	c.ServerURLs[nodeID] = url
}

// getServerURL returns the URL for the given server ID, or the default URL if the ID is unknown
func (c *RaftKVClient) getServerURL(serverID string) string {
	if url, ok := c.ServerURLs[serverID]; ok {
		return url
	}
	return ""
}

// generateRequestID creates a unique request ID for each operation
func (c *RaftKVClient) generateRequestID() string {
	counter := c.requestCounter.Add(1)
	return fmt.Sprintf("%s-%d", c.ClientID, counter)
}

// sendRequest is a helper function that sends an HTTP request with redirection handling
func (c *RaftKVClient) sendRequest(ctx context.Context, method, key string, body []byte, requestID string) (*http.Response, error) {
	// If we know who the leader is, try to send directly to them
	targetURL := ""
	if c.CurrentLeader != "" {
		targetURL = c.getServerURL(c.CurrentLeader)
	}

	// If we don't have a leader or don't know the leader's URL, try any server
	if targetURL == "" {
		// Try the first available server if no leader is known
		for _, url := range c.ServerURLs {
			targetURL = url
			break
		}

		// If we still don't have a URL, return an error
		if targetURL == "" {
			return nil, ErrNoLeader
		}
	}

	// Create the URL for the request
	url := fmt.Sprintf("%s/kv/%s", targetURL, key)

	// Create the request
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Request-ID", requestID)

	// Send the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Check for leader redirection
	if resp.StatusCode == http.StatusTemporaryRedirect {
		defer resp.Body.Close()

		// Parse the redirection response
		var redirectInfo map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&redirectInfo); err != nil {
			return nil, fmt.Errorf("failed to decode redirection response: %w", err)
		}

		// Check if we have leader information
		leaderID, ok := redirectInfo["leader_id"]
		if !ok || leaderID == "" {
			return nil, ErrNoLeader
		}

		// Update our knowledge of the current leader
		c.CurrentLeader = leaderID

		// If we received information about the current server, add it to our known servers
		if selfID, ok := redirectInfo["self_id"]; ok && selfID != "" {
			if selfAddr, ok := redirectInfo["self_addr"]; ok && selfAddr != "" {
				// Add or update the server's address
				c.AddServer(selfID, selfAddr)
			}
		}

		// If we don't know this leader's URL, we can't redirect
		if _, ok := c.ServerURLs[leaderID]; !ok {
			return nil, &LeaderRedirectErr{LeaderID: leaderID}
		}

		// Try the request again with the leader
		return c.sendRequest(ctx, method, key, body, requestID)
	}

	return resp, nil
}

// Get retrieves a value for a given key
func (c *RaftKVClient) Get(ctx context.Context, key string) (string, error) {
	// Generate a request ID
	requestID := c.generateRequestID()

	// Send the request
	resp, err := c.sendRequest(ctx, http.MethodGet, key, nil, requestID)
	if err != nil {
		// Check if this is a redirection error
		if redirectErr, ok := err.(*LeaderRedirectErr); ok {
			return "", redirectErr
		}
		return "", err
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

	// Create request body
	payload := map[string]string{
		"value":      value,
		"request_id": requestID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %w", err)
	}

	// Send the request
	resp, err := c.sendRequest(ctx, http.MethodPut, key, body, requestID)
	if err != nil {
		// Check if this is a redirection error
		if redirectErr, ok := err.(*LeaderRedirectErr); ok {
			return redirectErr
		}
		return err
	}
	defer resp.Body.Close()

	// Handle response
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: %s - %s", ErrServerError, resp.Status, string(bodyBytes))
	}

	return nil
}

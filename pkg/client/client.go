package client

import (
	"bytes"
	"context"
	"distributed_kv_store/internal/cluster"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type Option func(*Client)

type Client struct {
	httpClient    *http.Client
	addresses     map[string]string
	leaderAddress atomic.Value
	maxRetries    int
}

func WithHTTPTransport(rt http.RoundTripper) Option {
	return func(c *Client) {
		if c.httpClient == nil {
			c.httpClient = &http.Client{}
		}
		c.httpClient.Transport = rt
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		if c.httpClient == nil {
			c.httpClient = &http.Client{}
		}
		c.httpClient.Timeout = timeout
	}
}

func WithMaxRetries(maxRetries int) Option {
	return func(c *Client) {
		c.maxRetries = maxRetries
	}
}

func NewClient(addresses map[string]string, options ...Option) *Client {
	c := &Client{
		httpClient: http.DefaultClient,
		addresses:  addresses,
		maxRetries: 3,
	}

	for _, op := range options {
		op(c)
	}

	c.leaderAddress.Store("")
	if len(addresses) > 0 {
		for _, addr := range addresses {
			c.leaderAddress.Store(addr)
			break
		}
	}

	log.Printf("Raft client created with %d addresses", len(addresses))
	return c
}

func (c *Client) PUT(ctx context.Context, key string, value string) error {
	log.Printf("Putting key=%s value=%s", key, value)
	if len(c.addresses) == 0 {
		return fmt.Errorf("no addresses")
	}

	var serverAddress string
	if c.leaderAddress.Load() == "" {
		for _, addr := range c.addresses {
			c.leaderAddress.Store(addr)
			break
		}
	} else {
		serverAddress = c.leaderAddress.Load().(string)
	}
	cmd := &cluster.Command{
		Type:  cluster.CommandPut,
		Key:   key,
		Value: value,
	}

	var cmdToBytes []byte
	cmdToBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	var reqBody io.Reader
	reqBody = bytes.NewBuffer(cmdToBytes)
	url := fmt.Sprintf("http://%s/key/%s", serverAddress, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, reqBody)
	if err != nil {
		return fmt.Errorf("fail to create new HTTP request: %w", err)
	}
	log.Printf("HTTP request to %s", url)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		log.Printf("Status Code 404: Not Found")
		return fmt.Errorf("404: Not found")
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusTemporaryRedirect {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	fmt.Printf("Received body response: %v\n", bodyBytes)
	fmt.Printf("PUT %s -> %s", key, serverAddress)
	return nil
}

func (c *Client) GET(ctx context.Context, key string) (string, error) {
	if len(c.addresses) == 0 {
		return "", fmt.Errorf("no addresses")
	}
	
	var serverAddress string
	if c.leaderAddress.Load() == "" {
		for _, addr := range c.addresses {
			c.leaderAddress.Store(addr)
			break
		}
	} else {
		serverAddress = c.leaderAddress.Load().(string)
	}

	url := fmt.Sprintf("http://%s/key/%s", serverAddress, key)
	httpRequest, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

	resp, err := c.httpClient.Do(httpRequest)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var bodyString string
	if err := json.Unmarshal(body, &bodyString); err != nil {
		return "", fmt.Errorf("fail to unmarshal JSON response: %w", err)
	}

	log.Printf("%v", len(string(body)))
	return bodyString, nil
}

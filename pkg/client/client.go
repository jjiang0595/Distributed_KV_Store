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


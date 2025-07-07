package client

import (
	"bytes"
	"context"
	"distributed_kv_store/internal/cluster"
	"distributed_kv_store/internal/serverapp"
	"encoding/json"
	"fmt"
	"github.com/jonboulle/clockwork"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type Option func(*Client)

var httpErrorMessages = map[int]string{
	307: "Redirecting to Leader",
	404: "Resource Not Found",
	408: "Request Timeout",
	500: "Internal Server Error",
	503: "Service Unavailable",
	504: "Status Gateway Timeout",
}

type Client struct {
	httpClient    *http.Client
	addresses     map[string]string
	leaderAddress atomic.Value
	maxRetries    int
	clk           clockwork.Clock
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

func WithClock(clock clockwork.Clock) Option {
	return func(c *Client) {
		c.clk = clock
	}
}

func NewClient(addresses map[string]string, options ...Option) *Client {
	c := &Client{
		httpClient: http.DefaultClient,
		addresses:  addresses,
		maxRetries: 3,
		clk:        clockwork.NewRealClock(),
	}

	for _, op := range options {
		op(c)
	}

	c.httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	c.leaderAddress.Store("")
	ctx, cancel := clockwork.WithTimeout(context.Background(), c.clk, 2*time.Second)
	defer cancel()
	nodeAddr, err := c.findLeader(ctx)
	if err == nil {
		c.leaderAddress.Store(nodeAddr)
	}

	log.Printf("Raft client created with %d addresses", len(addresses))
	return c
}

func (c *Client) GetLeader(ctx context.Context) string {
	if c.leaderAddress.Load() == "" {
		ctx, cancel := clockwork.WithTimeout(ctx, c.clk, 1*time.Second)
		defer cancel()
		leaderAddr, err := c.findLeader(ctx)
		if err == nil {
			c.leaderAddress.Store(leaderAddr)
			return leaderAddr
		} else {
			for _, addr := range c.addresses {
				c.leaderAddress.Store(addr)
				return addr
			}
		}
	}
	return c.leaderAddress.Load().(string)
}

func (c *Client) PUT(ctx context.Context, key string, value string) error {
	if len(c.addresses) == 0 {
		return fmt.Errorf("no addresses")
	}

	maxRetryTime := 2 * time.Second
	retryTime := 50 * time.Millisecond

	var serverAddress string

	for i := 0; i < c.maxRetries; i++ {
		serverAddress = c.GetLeader(ctx)
		if serverAddress == "" {
			return fmt.Errorf("server address not found")
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

		var statusCode int
		statusCode, err = func() (int, error) {
			var reqBody io.Reader
			reqBody = bytes.NewBuffer(cmdToBytes)
			reqURL := fmt.Sprintf("http://%s/key/%s", serverAddress, key)
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, reqBody)
			if err != nil {
				return 0, fmt.Errorf("fail to create new HTTP request: %w", err)
			}
			log.Printf("HTTP request to %s", reqURL)
			req.Header.Set("Content-Type", "application/json")
			resp, err := c.httpClient.Do(req)
			if err != nil {
				return 0, fmt.Errorf("network/client error: %w", err)
			}
			defer resp.Body.Close()
			if statusCode == http.StatusRequestTimeout || statusCode == http.StatusServiceUnavailable || statusCode == http.StatusGatewayTimeout {
				return resp.StatusCode, nil
			}
			if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusInternalServerError {
				log.Printf("%d: %s", resp.StatusCode, httpErrorMessages[resp.StatusCode])
				return resp.StatusCode, nil
			}
			if resp.StatusCode == http.StatusOK {
				bodyBytes, err := io.ReadAll(resp.Body)
				if err != nil {
					return 0, fmt.Errorf("error reading body: %s", err)
				}
				fmt.Printf("Received body response: %v\n", bodyBytes)
				return 200, nil
			}
			if resp.StatusCode == http.StatusTemporaryRedirect {
				leaderURL := resp.Header.Get("Location")
				if leaderURL == "" {
					return 0, fmt.Errorf("no leader")
				}
				parsedURL, err := url.Parse(leaderURL)
				if err != nil {
					return 0, fmt.Errorf("fail to parse leader URL: %s", err)
				}
				c.leaderAddress.Store(parsedURL.Host)
				return 307, nil
			}
			return resp.StatusCode, nil
		}()

		if err == nil {
			if statusCode >= 200 && statusCode <= 299 {
				fmt.Printf("PUT %s -> %s", key, serverAddress)
				c.leaderAddress.Store(serverAddress)
				return nil
			}
			if statusCode >= 300 && statusCode <= 399 {
				fmt.Printf("Redirecting...")
				continue
			}
			if statusCode == http.StatusNotFound || statusCode == http.StatusInternalServerError {
				return fmt.Errorf("error: %s", httpErrorMessages[statusCode])
			}
			if statusCode == http.StatusRequestTimeout || statusCode == http.StatusServiceUnavailable || statusCode == http.StatusGatewayTimeout {
				if i == c.maxRetries-1 {
					return fmt.Errorf("%s", httpErrorMessages[statusCode])
				}
				c.leaderAddress.Store("")
				leaderAddr, err := c.findLeader(ctx)
				if err == nil {
					c.leaderAddress.Store(leaderAddr)
				}
				continue
			}
			return fmt.Errorf("unexpected status code %v", statusCode)
		} else {
			c.leaderAddress.Store("")
			leaderAddr, err := c.findLeader(ctx)
			if err == nil {
				c.leaderAddress.Store(leaderAddr)
			}
		}
		if i == c.maxRetries-1 {
			return fmt.Errorf("fail to put key=%s, value=%s %s", key, value, err)
		}
		if i < c.maxRetries {
			c.clk.Sleep(retryTime)
			retryTime = min(retryTime*2, maxRetryTime)
			continue
		}
	}
	return fmt.Errorf("fail to put key=%s, value=%s", key, value)
}

func (c *Client) GET(ctx context.Context, key string) (string, error) {
	if len(c.addresses) == 0 {
		return "", fmt.Errorf("no addresses")
	}
	var bodyString string
	retryTime := 50 * time.Millisecond
	maxRetryTime := 2 * time.Second

	for i := 0; i < c.maxRetries; i++ {
		serverAddress := c.GetLeader(ctx)

		reqURL := fmt.Sprintf("http://%s/key/%s", serverAddress, key)
		httpRequest, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		log.Printf("ADDRESS IS %s", serverAddress)
		statusCode, err := func() (int, error) {
			resp, err := c.httpClient.Do(httpRequest)
			if err != nil {
				log.Printf("fail to get key %s: %v", key, err)
				return 0, err
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					return 0, err
				}

				if err := json.Unmarshal(body, &bodyString); err != nil {
					return 0, fmt.Errorf("fail to unmarshal JSON response: %w", err)
				}
				return http.StatusOK, nil
			}
			if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
				return resp.StatusCode, nil
			}
			if resp.StatusCode >= 300 && resp.StatusCode <= 399 {
				leader := resp.Header.Get("Leader")
				if leader == "" {
					return 0, fmt.Errorf("leader not found")
				}
				parsedUrl, err := url.Parse(leader)
				if err != nil {
					return 0, fmt.Errorf("fail to parse leader URL: %s", err)
				}
				c.leaderAddress.Store(parsedUrl.Host)
				return http.StatusTemporaryRedirect, nil
			}
			if resp.StatusCode == http.StatusInternalServerError || resp.StatusCode == http.StatusNotFound {
				return resp.StatusCode, nil
			}
			return resp.StatusCode, fmt.Errorf("unexpected status code %v", resp.StatusCode)
		}()

		if err == nil {
			if statusCode >= 200 && statusCode <= 299 {
				return bodyString, nil
			}
			if statusCode >= 300 && statusCode <= 399 {
				continue
			}
			if statusCode == http.StatusNotFound || statusCode == http.StatusInternalServerError {
				return "", fmt.Errorf("%s", httpErrorMessages[statusCode])
			} else if statusCode == http.StatusRequestTimeout || statusCode == http.StatusServiceUnavailable {
				if i == c.maxRetries-1 {
					return "", fmt.Errorf("%s", httpErrorMessages[statusCode])
				}
				c.leaderAddress.Store("")
				leaderAddr, err := c.findLeader(ctx)
				if err == nil {
					c.leaderAddress.Store(leaderAddr)
				}
				continue
			}
		} else {
			c.leaderAddress.Store("")
			leaderAddr, err := c.findLeader(ctx)
			if err == nil {
				c.leaderAddress.Store(leaderAddr)
			}
		}
		if i == c.maxRetries-1 {
			return "", fmt.Errorf("fail to get key=%s, %s", key, err)
		}
		if i < c.maxRetries {
			c.clk.Sleep(retryTime)
			retryTime = min(retryTime*2, maxRetryTime)
			continue
		}
	}
	return "", fmt.Errorf("fail to get key %s", key)
}

func (c *Client) findLeader(ctx context.Context) (string, error) {
	var serverAddress string
	if c.leaderAddress.Load() != "" {
		serverAddress = c.leaderAddress.Load().(string)
		return serverAddress, nil
	}

	for _, addr := range c.addresses {
		leaderAddr, err := func() (string, error) {
			findCtx, findCancel := clockwork.WithTimeout(ctx, c.clk, time.Second*2)
			defer findCancel()
			reqURL := fmt.Sprintf("http://%s/status", addr)
			httpRequest, _ := http.NewRequestWithContext(findCtx, http.MethodGet, reqURL, nil)
			resp, err := c.httpClient.Do(httpRequest)
			if err != nil {
				return "", fmt.Errorf("fail to get leader info")
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return "", fmt.Errorf("bad status: %s", resp.Status)
			}

			if resp.StatusCode == http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					return "", fmt.Errorf("fail to read leader info: %v", err)
				}

				var leaderInfo serverapp.LeaderInfo
				err = json.Unmarshal(body, &leaderInfo)
				if err != nil {
					return "", fmt.Errorf("fail to unmarshal leader info: %v", err)
				}
				if leaderInfo.IsLead {
					if leaderAddr, ok := c.addresses[leaderInfo.LeaderID]; ok {
						return leaderAddr, nil
					}
				}
			}
			return "", fmt.Errorf("fail to get leader info")
		}()
		if err == nil {
			c.leaderAddress.Store(leaderAddr)
			return leaderAddr, nil
		}
	}
	return "", fmt.Errorf("leader not found")
}

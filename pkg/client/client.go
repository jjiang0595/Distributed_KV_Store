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

	c.httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	c.leaderAddress.Store("")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	nodeAddr, err := c.findLeader(ctx)
	if err == nil {
		c.leaderAddress.Store(nodeAddr)
	}

	log.Printf("Raft client created with %d addresses", len(addresses))
	return c
}

func (c *Client) PUT(ctx context.Context, key string, value string) error {
	if len(c.addresses) == 0 {
		return fmt.Errorf("no addresses")
	}

	retryTime := 100 * time.Millisecond
	retryCount := 0

	for retryCount < c.maxRetries {
		log.Printf("Putting key=%s value=%s", key, value)
		var serverAddress string
		serverAddress = func() string {
			if c.leaderAddress.Load() == "" {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()
				probedAddr, err := c.findLeader(ctx)
				if err == nil {
					c.leaderAddress.Store(serverAddress)
					return probedAddr
				} else {
					for _, addr := range c.addresses {
						c.leaderAddress.Store(addr)
						return addr
					}
				}
			}
			return c.leaderAddress.Load().(string)
		}()

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

		var reqBody io.Reader
		reqBody = bytes.NewBuffer(cmdToBytes)
		reqURL := fmt.Sprintf("http://%s/key/%s", serverAddress, key)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, reqBody)
		if err != nil {
			return fmt.Errorf("fail to create new HTTP request: %w", err)
		}
		log.Printf("HTTP request to %s", reqURL)
		req.Header.Set("Content-Type", "application/json")

		err = func() error {
			resp, err := c.httpClient.Do(req)
			if err != nil {
				c.leaderAddress.Store("")
				leaderID, err := c.findLeader(ctx)
				if err == nil {
					c.leaderAddress.Store(leaderID)
				}
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode == 404 {
				log.Printf("Status Code 404: Not Found")
				return fmt.Errorf("404: Not found")
			}
			if resp.StatusCode == http.StatusRequestTimeout {
				log.Printf("Status Code 401: Request Timeout")
				return fmt.Errorf("401: Request Timeout")
			}
			if resp.StatusCode == http.StatusInternalServerError {
				log.Printf("500: Internal Server Error")
				return fmt.Errorf("500: Internal Server Error")
			}
			if resp.StatusCode == http.StatusOK {
				bodyBytes, err := io.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				fmt.Printf("Received body response: %v\n", bodyBytes)
				fmt.Printf("PUT %s -> %s", key, serverAddress)
				return nil
			}
			if resp.StatusCode == http.StatusTemporaryRedirect {
				leaderURL := resp.Header.Get("Location")
				if leaderURL == "" {
					return fmt.Errorf("no leader")
				}
				parsedURL, err := url.Parse(leaderURL)
				if err != nil {
					return err
				}
				serverAddress = parsedURL.Host
				return nil
			}
			return nil
		}()

		if err == nil {
			c.leaderAddress.Store(serverAddress)
			return nil
		}
		retryCount++
		time.Sleep(retryTime)
		retryTime *= 2
	}
	return fmt.Errorf("fail to put key=%s, value=%s", key, value)
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

	reqURL := fmt.Sprintf("http://%s/key/%s", serverAddress, key)
	httpRequest, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)

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

func (c *Client) findLeader(ctx context.Context) (string, error) {
	var serverAddress string
	if c.leaderAddress.Load() != "" {
		serverAddress = c.leaderAddress.Load().(string)
		return serverAddress, nil
	}

	for _, addr := range c.addresses {
		leaderAddr, err := func() (string, error) {
			findCtx, findCancel := context.WithTimeout(ctx, time.Second*2)
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

			if resp.StatusCode == 200 {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					return "", fmt.Errorf("fail to read leader info: %v", err)
				}

				var leaderInfo serverapp.LeaderInfo
				err = json.Unmarshal(body, &leaderInfo)
				if err != nil {
					return "", fmt.Errorf("fail to unmarshal leader info: %v", err)
				}
				log.Printf("Leader info: %v", leaderInfo)
				if leaderInfo.IsLead {
					if leaderAddr, ok := c.addresses[leaderInfo.LeaderID]; ok {
						return leaderAddr, nil
					}
				}
			}
			return "", fmt.Errorf("fail to get leader info")
		}()
		if err == nil {
			return leaderAddr, nil
		}
	}
	return "", fmt.Errorf("leader not found")
}

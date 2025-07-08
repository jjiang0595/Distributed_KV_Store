package client

import (
	"context"
	"distributed_kv_store/internal/cluster"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

func classifyHTTPError(key string, resp *http.Response, err error) error {
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			if errors.Is(urlErr.Err, context.DeadlineExceeded) || errors.Is(urlErr.Err, context.Canceled) {
				return ErrTimeout{
					Key:     key,
					Message: err.Error(),
					Err:     err,
				}
			}
		}

		return ErrNetwork{
			Key:     key,
			Message: err.Error(),
			Err:     err,
		}
	}

	statusCode := resp.StatusCode
	switch statusCode {
	case http.StatusOK:
		return nil
	case http.StatusTemporaryRedirect:
		return ErrRedirect{Key: key, StatusCode: statusCode, Message: httpErrorMessages[statusCode]}
	case http.StatusRequestTimeout:
		return ErrRequestTimeout{Key: key, StatusCode: statusCode, Message: httpErrorMessages[statusCode]}
	case http.StatusNotFound:
		return ErrNotFound{Key: key, StatusCode: statusCode, Message: httpErrorMessages[statusCode]}
	case http.StatusGatewayTimeout:
		return ErrStatusGatewayTimeout{Key: key, StatusCode: statusCode, Message: httpErrorMessages[statusCode]}
	case http.StatusInternalServerError:
		return ErrInternalServer{Key: key, StatusCode: statusCode, Message: httpErrorMessages[statusCode]}
	case http.StatusServiceUnavailable:
		return ErrServiceUnavailable{Key: key, StatusCode: statusCode, Message: httpErrorMessages[statusCode]}
	default:
		return ErrUnexpectedHTTPStatus{Key: key, StatusCode: statusCode, Message: fmt.Sprintf("unexpected HTTP status code: %d", statusCode)}
	}
}

type ErrNetwork struct {
	Key     string
	Message string
	Err     error
}

func (e ErrNetwork) Error() string {
	return fmt.Sprintf("Network Error: %s", e.Message)
}

type ErrTimeout struct {
	Key     string
	Message string
	Err     error
}

func (e ErrTimeout) Error() string {
	return fmt.Sprintf("Timeout Error: %s", e.Message)
}

func (e ErrTimeout) Unwrap() error {
	return e.Err
}

type ErrRedirect struct {
	Key        string
	StatusCode int
	Message    string
}

func (e ErrRedirect) Error() string {
	return fmt.Sprintf("Redirect Error: %s", e.Message)
}

type ErrNotFound struct {
	Key        string
	StatusCode int
	Message    string
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("Error Code %d: %s", e.StatusCode, e.Message)
}

type ErrRequestTimeout struct {
	Key        string
	StatusCode int
	Message    string
}

func (e ErrRequestTimeout) Error() string {
	return fmt.Sprintf("Error Code %d: %s", e.StatusCode, e.Message)
}

type ErrInternalServer struct {
	Key        string
	StatusCode int
	Message    string
}

func (e ErrInternalServer) Error() string {
	return fmt.Sprintf("Error Code %d: %s", e.StatusCode, e.Message)
}

type ErrServiceUnavailable struct {
	Key        string
	StatusCode int
	Message    string
}

func (e ErrServiceUnavailable) Error() string {
	return fmt.Sprintf("Error Code %d: %s", e.StatusCode, e.Message)
}

type ErrStatusGatewayTimeout struct {
	Key        string
	StatusCode int
	Message    string
}

func (e ErrStatusGatewayTimeout) Error() string {
	return fmt.Sprintf("Error Code %d: %s", e.StatusCode, e.Message)
}

type ErrMaxRetries struct {
	Type    cluster.CommandType
	Retries int
	LastErr error
}

func (e ErrMaxRetries) Error() string {
	return fmt.Sprintf("Fail to complete %s request within %d retries: %s", e.Type, e.Retries, e.LastErr)
}

type ErrUnexpectedHTTPStatus struct {
	Key        string
	StatusCode int
	Message    string
}

func (e ErrUnexpectedHTTPStatus) Error() string {
	return fmt.Sprintf("Unexpected HTTP Status Code: %d %s", e.StatusCode, e.Message)
}

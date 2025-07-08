package client

import "fmt"

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

func (e *ErrRequestTimeout) Error() string {
	return fmt.Sprintf("Error Code %d: %s", e.StatusCode, e.Message)
}

type ErrInternalServer struct {
	Key        string
	StatusCode int
	Message    string
}

func (e *ErrInternalServer) Error() string {
	return fmt.Sprintf("Error Code %d: %s", e.StatusCode, e.Message)
}

type ErrServiceUnavailable struct {
	Key        string
	StatusCode int
	Message    string
}

func (e *ErrServiceUnavailable) Error() string {
	return fmt.Sprintf("Error Code %d: %s", e.StatusCode, e.Message)
}

type ErrStatusGatewayTimeout struct {
	Key        string
	StatusCode int
	Message    string
}

func (e *ErrStatusGatewayTimeout) Error() string {
	return fmt.Sprintf("Error Code %d: %s", e.StatusCode, e.Message)
}

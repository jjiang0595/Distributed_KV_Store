package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func main() {
	serverAddress := flag.String("server", "http://127.0.0.1:8080", "Server address")
	method := flag.String("method", "PUT", "HTTP method")
	key := flag.String("key", "", "Key")
	value := flag.String("value", "", "Value")
	flag.Parse()

	if key == nil {
		log.Fatalf("Key is empty")
	}

	var reqBody io.Reader
	var url string
	var httpMethod = strings.ToUpper(*method)

	switch httpMethod {
	case http.MethodPut:
		if *value == "" {
			log.Fatalf("Value is empty")
		}
		url = fmt.Sprintf("%s/%s", *serverAddress, *key)
		reqBody = bytes.NewBufferString(*value)
		log.Printf("Setup PUT Request %s: %s ", *key, reqBody)
	case http.MethodGet:
		url = fmt.Sprintf("%s/%s", *serverAddress, *key)
		log.Printf("Setup GET Request: %s", reqBody)
	default:
		log.Fatalf("Unknown HTTP method: %s", httpMethod)
	}

	req, err := http.NewRequest(httpMethod, url, reqBody)
	if err != nil {
		log.Fatalf("NewRequest Error: %s", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Error sending request: %s", err)
	}

	log.Printf("Status Code: %d\n", resp.StatusCode)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %s", err)
	}

	fmt.Println("Response Body: ", string(body))

	if resp.StatusCode == http.StatusServiceUnavailable {
		fmt.Println("Redirected")
	}
}

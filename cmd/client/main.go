package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
)

func main() {
	serverAddress := flag.String("server", "http://127.0.0.1:8080", "Server address")
	key := flag.String("key", "", "Key")
	flag.Parse()
	message := []byte("HELLO")

	url := fmt.Sprintf("%s/%s", *serverAddress, *key)
	putReq, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(message))
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}
	getReq, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(putReq)
	if err != nil {
		log.Fatalf("Error sending request: %v", err)
	}
	resp, err = client.Do(getReq)
	if err != nil {
		log.Fatalf("Error sending request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %v", err)
	}
	fmt.Println("GET Response Body: ", string(body))

	fmt.Println("Response Status: ", resp.Status)
}

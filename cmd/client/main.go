package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
)

func main() {
	server := "http://127.0.0.1:8080/test1"
	message := []byte("HELLO")

	putReq, err := http.NewRequest(http.MethodPut, server, bytes.NewBuffer(message))
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}
	getReq, err := http.NewRequest(http.MethodGet, server, nil)
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

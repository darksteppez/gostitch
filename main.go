package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

// BatchPayload is the main payload struct used to send data to stitch's batch import API endpoint
type BatchPayload struct {
	TableName string         `json:"table_name"`
	Schema    Schema         `json:"schema"`
	Messages  []SingleRecord `json:"messages"`
	KeyNames  []string       `json:"key_names"`
}

// Schema consists of several key maps that describe the structure of the SingleRecord struct's Data property
type Schema struct {
	Properties map[string]map[string]string `json:"properties"`
}

// SingleRecord is a single row of data that is transmitted to the API via the BatchPayload Messages property. The Data property structure should match the
// properties map in the Schema struct
type SingleRecord struct {
	Action   string                 `json:"action"`
	Sequence int64                  `json:"sequence"`
	Data     map[string]interface{} `json:"data"`
}

func main() {

	payload := []byte(`[{"item": "first", "another_item": 4},{"item": "second", "another_item": 6}]`)

	var bucket = []map[string]interface{}{}

	err := json.Unmarshal(payload, &bucket)

	if err != nil {
		log.Fatal(err)
	}

	var messages = []SingleRecord{}

	for _, message := range bucket {
		record := SingleRecord{
			Action:   "upsert",
			Sequence: time.Now().Unix(),
			Data:     message,
		}
		messages = append(messages, record)
	}

	schema := Schema{
		Properties: map[string]map[string]string{
			"item": map[string]string{
				"type": "string",
			},
			"another_item": map[string]string{
				"type": "integer",
			},
		},
	}

	keynames := []string{
		"item",
	}

	batchpayload := BatchPayload{
		TableName: "test",
		Schema:    schema,
		Messages:  messages,
		KeyNames:  keynames,
	}

	jsonString, err := json.Marshal(batchpayload)

	if err != nil {
		log.Fatal(err)
	}

	os.Stdout.Write(jsonString)

	var marshal = new(BatchPayload)

	json.Unmarshal(jsonString, &marshal)

	fmt.Printf("%+v", marshal)
}

// StitchPayload converts a BulkPayload struct into a JSON-formatted []byte for use in sending via POST to the stitch API
func StitchBatchPayload(payload BatchPayload) []byte {
	jsonString, err := json.Marshal(payload)

	if err != nil {
		log.Fatal(err)
	}

	return jsonString
}

// StitchSendBatchPayload sends a POST request with a JSON-encoded payload
func StitchSendBatchPayload(payload []byte, apiToken string) (string, string) {
	stitch, err := http.NewRequest("POST", "https://api.stitchdata.com/v2/import/batch", bytes.NewBuffer(payload))

	if err != nil {
		log.Fatal(err)
	}

	stitch.Header.Set("Content-Type", "application/json")
	stitch.Header.Set("Authorization", "Bearer "+apiToken)

	client := &http.Client{
		Timeout: time.Second * 10,
	}

	stitchResponse, err := client.Do(stitch)

	if err != nil {
		log.Fatal(err)
	}

	defer stitchResponse.Body.Close()

	body, err := ioutil.ReadAll(stitchResponse.Body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s\n", body)

	return stitchResponse.Status, string(body)
}

package gostitch

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// StitchAPIURL is the URL for the stitch API endpoint. NOTE: does not include trailing slash
const StitchAPIURL = "https://api.stitchdata.com"

// BatchPayload is the main payload struct used to send data to stitch's batch import API endpoint
type BatchPayload struct {
	TableName string         `json:"table_name"`
	Schema    Schema         `json:"schema"`
	Messages  []SingleRecord `json:"messages"`
	KeyNames  []string       `json:"key_names"`
}

// Schema consists of several key maps that describe the structure of the SingleRecord struct's Data property
type Schema struct {
	Properties map[string]Property `json:"properties"`
}

// Property is a struct that contains defining values for JSON schema properties
type Property struct {
	Type   string `json:"type"`
	Format string `json:"format,omitempty"`
}

// SingleRecord is a single row of data that is transmitted to the API via the BatchPayload Messages property. The Data property structure should match the
// properties map in the Schema struct
type SingleRecord struct {
	Action   string                 `json:"action"`
	Sequence int64                  `json:"sequence"`
	Data     map[string]interface{} `json:"data"`
}

// BuildSchema takes in a [string]string map of data types sent in Stitch payload and returns a Schema object
func BuildSchema(schemaTraits []map[string]string) Schema {
	properties := map[string]Property{}
	for _, v := range schemaTraits {
		property := Property{
			Type: v["type"],
		}
		if v["format"] != "" {
			property.Format = v["format"]
		}
		properties[v["name"]] = property
	}
	schema := Schema{
		Properties: properties,
	}
	return schema
}

// BuildMessageBatches takes a JSON byte slice formatted as "key":"value" and converts it to a collection of slices of SingleRecord structs for use in the Stitch batch payload
func BuildMessageBatches(jsonData []byte, sequence int64) [][]SingleRecord {
	var bucket = []map[string]interface{}{}

	err := json.Unmarshal(jsonData, &bucket)

	if err != nil {
		log.Fatal(err)
	}

	var batches = [][]SingleRecord{}
	var messages = []SingleRecord{}

	batchByteSize := 0

	for _, message := range bucket {
		record := SingleRecord{
			Action:   "upsert",
			Sequence: sequence,
			Data:     message,
		}
		byteMessage, _ := json.Marshal(message)
		messages = append(messages, record)
		batchByteSize += len(byteMessage)

		// limit batch sizes to 3.9mb or 19,500 messages
		if batchByteSize >= 3900000 || len(messages) > 19500 {
			batches = append(batches, messages)
			batchByteSize = 0
			messages = []SingleRecord{}
		}
	}

	if len(messages) > 0 {
		batches = append(batches, messages)
	}

	return batches
}

// stitchBatchPayload converts a BulkPayload struct into a JSON-formatted []byte for use in sending via POST to the stitch API
func (payload *BatchPayload) stitchBatchPayload() ([]byte, error) {

	jsonString, err := json.Marshal(payload)

	if err != nil {
		return []byte{}, err
	}

	return jsonString, nil
}

// Send sends a POST request with a JSON-encoded payload
func (payload *BatchPayload) Send(apiToken string) (string, map[string]string) {
	jsonPayload, err := payload.stitchBatchPayload()

	if err != nil {
		log.Fatal(err)
	}

	stitch, err := http.NewRequest("POST", StitchAPIURL+"/v2/import/batch", bytes.NewBuffer(jsonPayload))

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

	jsonResponse := map[string]string{}

	json.Unmarshal(body, &jsonResponse)

	return stitchResponse.Status, jsonResponse
}

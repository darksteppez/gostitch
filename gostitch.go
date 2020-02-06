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
	Properties map[string]map[string]string `json:"properties"`
}

// SingleRecord is a single row of data that is transmitted to the API via the BatchPayload Messages property. The Data property structure should match the
// properties map in the Schema struct
type SingleRecord struct {
	Action   string                 `json:"action"`
	Sequence int64                  `json:"sequence"`
	Data     map[string]interface{} `json:"data"`
}

// func main() {

// 	payload := []byte(`[{"item": "first", "another_item": 4, "floatme": 3.14},{"item": "second", "another_item": 6, "floatme": 6.02}]`)

// 	messages := BuildMessages(payload)

// 	schemaTraits := map[string]string{
// 		"item":         "string",
// 		"another_item": "integer",
// 		"float_me":     "number",
// 	}

// 	schema := BuildSchema(schemaTraits)

// 	keynames := []string{
// 		"item",
// 	}

// 	tablename := "testlibrary"

// 	jsonString := StitchBatchPayload(tablename, schema, messages, keynames)

// 	os.Stdout.Write(jsonString)

// 	var marshal = new(BatchPayload)

// 	json.Unmarshal(jsonString, &marshal)

// 	fmt.Printf("%+v\n", marshal)

// 	status, response := StitchSendBatchPayload(jsonString, "63303350b58ff62bd68966a2c428b43d3cb1f0aebff944f4bd7d0e46677b869e")

// 	fmt.Println(status)
// 	fmt.Println(response)

// }

// BuildSchema takes in a [string]string map of data types sent in Stitch payload and returns a Schema object
func BuildSchema(schemaTraits map[string]string) Schema {
	properties := map[string]map[string]string{}
	for k, v := range schemaTraits {
		properties[k] = map[string]string{
			"type": v,
		}
	}
	schema := Schema{
		Properties: properties,
	}
	return schema
}

// BuildMessages takes a JSON byte slice formatted as "key":"value" and converts it to a slice of SingleRecord structs for us in the Stitch batch payload
func BuildMessages(jsonData []byte) []SingleRecord {
	var bucket = []map[string]interface{}{}

	err := json.Unmarshal(jsonData, &bucket)

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

	return messages
}

// StitchBatchPayload converts a BulkPayload struct into a JSON-formatted []byte for use in sending via POST to the stitch API
func StitchBatchPayload(tablename string, schema Schema, messages []SingleRecord, keynames []string) []byte {
	batchpayload := BatchPayload{
		TableName: tablename,
		Schema:    schema,
		Messages:  messages,
		KeyNames:  keynames,
	}

	jsonString, err := json.Marshal(batchpayload)

	if err != nil {
		log.Fatal(err)
	}

	return jsonString
}

// StitchSendBatchPayload sends a POST request with a JSON-encoded payload
func StitchSendBatchPayload(payload []byte, apiToken string) (string, string) {
	stitch, err := http.NewRequest("POST", StitchAPIURL+"/v2/import/batch", bytes.NewBuffer(payload))

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

	return stitchResponse.Status, string(body)
}

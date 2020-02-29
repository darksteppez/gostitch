package example

import (
	"fmt"
	"time"

	"github.com/darksteppez/gostitch"
)

func sendExample() {

	// this is our initial JSON payload
	payload := []byte(`[{"item": "first", "another_item": 4, "floatme": 3.14, "the_date": "2020-02-01T00:00:00Z"},{"item": "second", "another_item": 6, "floatme": 6.02,  "the_date": "2020-02-01T00:00:00Z"}]`)

	// the name of the table we are pushing data into in Stitch
	tablename := "tablename"

	// the schema structure of the records being sent. for more info visit https://json-schema.org/
	schemaTraits := []map[string]string{
		{
			"name": "item",
			"type": "string",
		},
		{
			"name": "another_item",
			"type": "integer",
		},
		{
			"name": "floatme",
			"type": "number",
		},
		{
			"name":   "the_date",
			"type":   "string",
			"format": "date-time",
		},
	}

	// the marshaled schema
	schema := gostitch.BuildSchema(schemaTraits)

	// used as the sequence value in the individual messages
	now := time.Now().Unix()

	// slice of message batches to be sent
	messages := gostitch.BuildMessageBatches(payload, now)

	// list of keys in the payload
	keynames := []string{
		"item",
	}

	for key := range messages {
		payload := gostitch.BatchPayload{
			TableName: tablename,
			Schema:    schema,
			Messages:  messages[key],
			KeyNames:  keynames,
		}

		status, response := payload.Send("YourStitchAPITokenHere")

		fmt.Println(status)
		fmt.Printf("%+v\n", response)
	}
}

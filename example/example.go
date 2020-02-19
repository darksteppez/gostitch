package example

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/darksteppez/gostitch"
)

func sendExample() {

	payload := []byte(`[{"item": "first", "another_item": 4, "floatme": 3.14},{"item": "second", "another_item": 6, "floatme": 6.02}]`)

	now := time.Now().Unix()

	messages := gostitch.BuildMessageBatches(payload, now)

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
	}

	schema := gostitch.BuildSchema(schemaTraits)

	keynames := []string{
		"item",
	}

	tablename := "tablename"

	for key := range messages {
		payload := gostitch.BatchPayload{
			TableName: tablename,
			Schema:    schema,
			Messages:  messages[key],
			KeyNames:  keynames,
		}

		jsonPayload, err := json.Marshal(payload)

		if err != nil {
			log.Fatal("json marshal error: ", err)
		}

		status, response := gostitch.StitchSendBatchPayload(jsonPayload, "YourStitchAPITokenHere")

		fmt.Println(status)
		fmt.Printf("%+v\n", response)
	}
}

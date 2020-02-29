package gostitch

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestBuildSchema(t *testing.T) {
	schema := []Schema{
		Schema{
			Properties: map[string]Property{
				"one": Property{
					Type:   "string",
					Format: "date-time",
				},
				"two": Property{
					Type: "number",
				},
			},
		},
		Schema{
			Properties: map[string]Property{
				"three": Property{
					Type: "string",
				},
				"four": Property{
					Type: "boolean",
				},
			},
		},
	}

	testTraits := [][]map[string]string{
		[]map[string]string{
			map[string]string{
				"name":   "one",
				"type":   "string",
				"format": "date-time",
			},
			map[string]string{
				"name": "two",
				"type": "number",
			},
		},
		[]map[string]string{
			map[string]string{
				"name": "three",
				"type": "string",
			},
			map[string]string{
				"name": "four",
				"type": "boolean",
			},
		},
	}

	for i, traits := range testTraits {
		testSchema := BuildSchema(traits)
		if !cmp.Equal(schema[i], testSchema) {
			t.Error("schemas do not match")
		}
	}
}

func TestBuildMessages(t *testing.T) {
	payloads := []byte(`[{"item": "first", "another_item": 4.0, "floatme": 3.14},{"item": "first", "another_item": 5.0, "floatme": 3.19}]`)

	var sequence int64 = time.Now().Unix()

	testMessageBatches := [][]SingleRecord{
		[]SingleRecord{
			SingleRecord{
				Action:   "upsert",
				Sequence: sequence,
				Data: map[string]interface{}{
					"item":         "first",
					"another_item": 4.0,
					"floatme":      3.14,
				},
			},
			SingleRecord{
				Action:   "upsert",
				Sequence: sequence,
				Data: map[string]interface{}{
					"item":         "first",
					"another_item": 5.0,
					"floatme":      3.19,
				},
			},
		},
	}

	messages := BuildMessageBatches(payloads, sequence)

	if !cmp.Equal(messages, testMessageBatches) {
		t.Error("Messages do not match")
	}
}

func TestBatchPayloadSizeLimitSharding(t *testing.T) {
	single := map[string]interface{}{
		"item":        "first",
		"float_me":    4.0,
		"int_me":      3,
		"another":     "this is some more text and a little more never hurt",
		"yet_another": 1245.4547,
		"one_more":    "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout. The point of using Lorem Ipsum is that it has a more-or-less normal distribution of letters, as opposed to using 'Content here, content here', making it look like readable English. Many desktop publishing packages and web page editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will uncover many web sites still in their infancy. Various versions have evolved over the years, sometimes by accident, sometimes on purpose (injected humour and the like).",
	}

	batchPayload := []map[string]interface{}{}

	for i := 0; i < 5167; i++ {
		batchPayload = append(batchPayload, single)
	}

	jsonPayload, _ := json.Marshal(batchPayload)

	now := time.Now().Unix()

	// single item payload size is 755 bytes. our max batch size is set to 3.9 million which would be approx 5166 single items.
	// test should have len(messageBatches) == 2
	messageBatches := BuildMessageBatches(jsonPayload, now)

	if len(messageBatches) != 2 {
		t.Errorf("total message batches incorrect. expecting 2 but got %v", len(messageBatches))
	}
}

func TestBatchMessageCountLimitSharding(t *testing.T) {
	single := map[string]interface{}{
		"item":     "first",
		"float_me": 4.0,
	}

	batchPayload := []map[string]interface{}{}

	for i := 0; i < 20000; i++ {
		batchPayload = append(batchPayload, single)
	}

	jsonPayload, _ := json.Marshal(batchPayload)

	now := time.Now().Unix()

	// stitch batch message limit is 20k but the message limit in the library is set to 19.5k. test should have len(messageBatches) == 2
	messageBatches := BuildMessageBatches(jsonPayload, now)

	if len(messageBatches) != 2 {
		t.Errorf("total message batches incorrect. expecting 2 but got %v", len(messageBatches))
	}
}

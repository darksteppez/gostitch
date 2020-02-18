package gostitch

import (
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

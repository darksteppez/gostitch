package gostitch

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestBuildSchema(t *testing.T) {
	schema := []Schema{
		Schema{
			Properties: map[string]map[string]string{
				"one": map[string]string{
					"type": "integer",
				},
				"two": map[string]string{
					"type": "number",
				},
			},
		},
		Schema{
			Properties: map[string]map[string]string{
				"three": map[string]string{
					"type": "string",
				},
				"four": map[string]string{
					"type": "boolean",
				},
			},
		},
	}

	testTraits := []map[string]string{
		map[string]string{
			"one": "integer",
			"two": "number",
		},
		map[string]string{
			"three": "string",
			"four":  "boolean",
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
	payloads := [][]byte{
		[]byte(`[{"item": "first", "another_item": 4, "floatme": 3.14},{"item": "second", "another_item": 6, "floatme": 6.02}]`),
		[]byte(`[{"item": "first", "another_item": 5, "floatme": 3.19},{"item": "second", "another_item": 7, "floatme": 9.02}]`),
		[]byte(`[{"booly": true, "another_item": "6", "stringy": "3.14"},{"booly": false, "another_item": 6, "stringy": "6.02"}]`),
		[]byte(`[{"booly": true, "another_item": "8", "stringy": "84.54"},{"booly": false, "another_item": 8, "stringy": "6.02"}]`),
	}

	var sequence int64 = 1

	testMessages := []SingleRecord{
		SingleRecord{
			Action:   "upsert",
			Sequence: sequence,
		},
	}
}

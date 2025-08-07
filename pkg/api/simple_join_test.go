package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tcmartin/flowlib"
)

func TestJoinNodeDirect(t *testing.T) {
	// Create a JoinNode directly
	joinNode := flowlib.NewJoinNode()

	// Set parameters
	joinNode.SetParams(map[string]interface{}{
		"format": "array",
	})

	// Create test shared context with mapper_results
	shared := map[string]interface{}{
		"mapper_results": []interface{}{
			map[string]interface{}{
				"branch":  "mapper1",
				"input":   10,
				"output":  100,
				"message": "Mapper 1 completed: 10² = 100",
			},
			map[string]interface{}{
				"branch":  "mapper2",
				"input":   20,
				"output":  400,
				"message": "Mapper 2 completed: 20² = 400",
			},
			map[string]interface{}{
				"branch":  "mapper3",
				"input":   30,
				"output":  900,
				"message": "Mapper 3 completed: 30² = 900",
			},
		},
	}

	// Run the JoinNode
	t.Logf("Before JoinNode.Run: shared = %+v", shared)
	action, err := joinNode.Run(shared)
	t.Logf("After JoinNode.Run: shared = %+v", shared)

	// Verify no error
	assert.NoError(t, err)
	assert.Equal(t, flowlib.DefaultAction, action)

	// Verify the input was set correctly
	input, exists := shared["input"]
	assert.True(t, exists, "input should be set by JoinNode")

	inputSlice, ok := input.([]interface{})
	assert.True(t, ok, "input should be a slice")
	assert.Len(t, inputSlice, 3, "input should contain 3 results")

	// Verify the results are correct
	result1 := inputSlice[0].(map[string]interface{})
	assert.Equal(t, "mapper1", result1["branch"])
	assert.Equal(t, 100, result1["output"])

	t.Logf("JoinNode direct test passed! Input: %+v", input)
}

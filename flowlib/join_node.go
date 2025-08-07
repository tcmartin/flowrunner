package flowlib

import "fmt"

/* ---------- JoinNode (collect parallel results) ---------- */

type JoinNode struct {
	baseNode
}

func NewJoinNode() *JoinNode {
	fmt.Printf("[JoinNode] NewJoinNode called - UPDATED VERSION\n")
	return &JoinNode{newBaseNode()}
}

func (jn *JoinNode) Run(shared any) (Action, error) {
	fmt.Printf("[JoinNode] Run called with shared context\n")

	// Get the format parameter (default to "array")
	format := "array"
	if params := jn.Params(); params != nil {
		if f, ok := params["format"].(string); ok {
			format = f
		}
	}

	fmt.Printf("[JoinNode] Using format: %s\n", format)

	// The shared context should contain collected results from parallel execution
	if sharedMap, ok := shared.(map[string]interface{}); ok {
		var resultSlice []interface{}

		// Check for mapper_results (from SplitNode)
		if mapperResults, exists := sharedMap["mapper_results"]; exists {
			fmt.Printf("[JoinNode] Found mapper_results in shared context\n")
			// Handle slice of results from SplitNode
			if slice, ok := mapperResults.([]interface{}); ok {
				fmt.Printf("[JoinNode] Retrieved %d results from mapper_results\n", len(slice))
				resultSlice = slice
			} else {
				fmt.Printf("[JoinNode] mapper_results is not a slice: %T\n", mapperResults)
			}
		} else if splitResults, exists := sharedMap["_split_results"]; exists {
			fmt.Printf("[JoinNode] Found _split_results in shared context\n")
			// Handle SplitNodeResults type (fallback)
			if collector, ok := splitResults.(interface{ GetAll() []interface{} }); ok {
				results := collector.GetAll()
				fmt.Printf("[JoinNode] Retrieved %d results from SplitNodeResults\n", len(results))
				resultSlice = results
			} else {
				fmt.Printf("[JoinNode] _split_results is not a SplitNodeResults collector\n")
			}
		} else if results, exists := sharedMap["_parallel_results"]; exists {
			fmt.Printf("[JoinNode] Found _parallel_results in shared context\n")
			// Handle direct slice (from AsyncSplitNode or other sources)
			if slice, ok := results.([]interface{}); ok {
				resultSlice = slice
			}
		} else {
			fmt.Printf("[JoinNode] No parallel results found in shared context\n")
			// Print available keys for debugging
			keys := make([]string, 0, len(sharedMap))
			for k := range sharedMap {
				keys = append(keys, k)
			}
			fmt.Printf("[JoinNode] Available keys: %v\n", keys)
		}

		if resultSlice != nil && len(resultSlice) > 0 {
			fmt.Printf("[JoinNode] Processing %d results\n", len(resultSlice))
			// Format the results based on the requested format
			switch format {
			case "array":
				// Return as simple array: [result1, result2, result3]
				sharedMap["_join_output"] = resultSlice
			case "object":
				// Return as indexed object: {"result_0": result1, "result_1": result2}
				obj := make(map[string]interface{})
				for i, result := range resultSlice {
					obj[fmt.Sprintf("result_%d", i)] = result
				}
				sharedMap["_join_output"] = obj
			case "map":
				// Return as keyed map (requires results to have branch info)
				obj := make(map[string]interface{})
				for i, result := range resultSlice {
					key := fmt.Sprintf("branch_%d", i)
					if resultMap, ok := result.(map[string]interface{}); ok {
						if branch, hasBranch := resultMap["branch"]; hasBranch {
							if branchStr, ok := branch.(string); ok {
								key = branchStr
							}
						}
					}
					obj[key] = result
				}
				sharedMap["_join_output"] = obj
			default:
				return "", fmt.Errorf("unsupported join format: %s", format)
			}

			// Set the joined results as the current input for the next node
			if joinOutput, exists := sharedMap["_join_output"]; exists {
				sharedMap["input"] = joinOutput
				fmt.Printf("[JoinNode] Set input for next node with %d results\n", len(resultSlice))
			}

			return DefaultAction, nil
		}
	}

	fmt.Printf("[JoinNode] No results to process, setting empty input\n")
	// If no parallel results found, return empty based on format
	if sharedMap, ok := shared.(map[string]interface{}); ok {
		switch format {
		case "array":
			sharedMap["input"] = []interface{}{}
		case "object", "map":
			sharedMap["input"] = map[string]interface{}{}
		}
	}

	return DefaultAction, nil
}

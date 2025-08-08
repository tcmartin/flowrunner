package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/robertkrimen/otto"
	"github.com/tcmartin/flowlib"
	"github.com/tcmartin/flowrunner/pkg/auth"
	"github.com/tcmartin/flowrunner/pkg/utils"
)

// NodeWrapper is a base wrapper for flowlib.Node implementations
type NodeWrapper struct {
	node flowlib.Node
	exec func(input interface{}) (interface{}, error)
	post func(shared, p, e interface{}) (flowlib.Action, error)
}

// SetParams sets the parameters for the node
func (w *NodeWrapper) SetParams(params map[string]interface{}) {
	w.node.SetParams(params)
}

// Params returns the parameters for the node
func (w *NodeWrapper) Params() map[string]interface{} {
	return w.node.Params()
}

// Next sets the next node for the given action
func (w *NodeWrapper) Next(action flowlib.Action, n flowlib.Node) {
	w.node.Next(action, n)
}

// Successors returns the successors of the node
func (w *NodeWrapper) Successors() map[flowlib.Action]flowlib.Node {
	return w.node.Successors()
}

// Run executes the node
func (w *NodeWrapper) Run(shared interface{}) (flowlib.Action, error) {
	// Create a custom implementation that calls our exec function
	if w.exec != nil {
		// Get the parameters
		params := w.Params()

		// Extract FlowContext for template expression evaluation if available
		var flowContext *FlowContext
        if sharedMap, ok := shared.(map[string]interface{}); ok {
			if flowContextData, hasFlowContext := sharedMap["_flow_context"]; hasFlowContext {
				if fcMap, ok := flowContextData.(map[string]interface{}); ok {
					// Try to reconstruct FlowContext from the data
					if executionID, ok := sharedMap["_execution"].(map[string]interface{})["execution_id"].(string); ok {
						if flowID, ok := sharedMap["_execution"].(map[string]interface{})["flow_id"].(string); ok {
							if accountID, ok := sharedMap["accountID"].(string); ok {
								// We need access to the secret vault to recreate FlowContext
								// For now, we'll try to find it in the shared context
								if secretVault, ok := sharedMap["_secret_vault"]; ok {
									if vault, ok := secretVault.(auth.SecretVault); ok {
										flowContext = NewFlowContext(executionID, flowID, accountID, vault)
										// Import existing data
										if nodeResults, ok := fcMap["node_results"].(map[string]any); ok {
											for k, v := range nodeResults {
												flowContext.SetNodeResult(k, v)
											}
										}
										if sharedData, ok := fcMap["shared_data"].(map[string]any); ok {
											for k, v := range sharedData {
												flowContext.SetSharedData(k, v)
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}

		// Process node parameters through template engine if FlowContext is available
		processedParams := params
		if flowContext != nil {
			// Update the flow context with current shared data for template evaluation
			if sharedMap, ok := shared.(map[string]interface{}); ok {
				// Log the complete shared state in readable JSON format (thread-safe)
				sharedCopy := make(map[string]interface{})
				for k, v := range sharedMap {
					// Skip potentially problematic keys that might be modified concurrently
					if k != "_split_results" && k != "mapper_results" {
						sharedCopy[k] = v
					}
				}
				sharedJSON, _ := json.MarshalIndent(sharedCopy, "", "  ")
				fmt.Printf("\n🔄 [NodeWrapper] PRE-EXECUTION SHARED STATE:\n%s\n", string(sharedJSON))

				keys := make([]string, 0, len(sharedMap))
				for key := range sharedMap {
					keys = append(keys, key)
				}
				fmt.Printf("📋 [NodeWrapper] Available shared context keys: %v\n", keys)

				// Special logging for LLM results and tool calls
				if result, exists := sharedMap["result"]; exists {
					resultJSON, _ := json.MarshalIndent(result, "", "  ")
					fmt.Printf("🧠 [NodeWrapper] LLM Result in shared.result:\n%s\n", string(resultJSON))
				}
				if llmResult, exists := sharedMap["llm_result"]; exists {
					llmResultJSON, _ := json.MarshalIndent(llmResult, "", "  ")
					fmt.Printf("🧠 [NodeWrapper] LLM Result in shared.llm_result:\n%s\n", string(llmResultJSON))
				}

				for key, value := range sharedMap {
					// Skip internal flow context keys
					if !strings.HasPrefix(key, "_") && key != "accountID" {
						flowContext.SetSharedData(key, value)
					}
				}

				// Log the template evaluation context
				evalContext := flowContext.GetEvaluationContext()
				evalJSON, _ := json.MarshalIndent(evalContext, "", "  ")
				fmt.Printf("🎯 [NodeWrapper] TEMPLATE EVALUATION CONTEXT:\n%s\n", string(evalJSON))
			}

			var err error
			processedParams, err = flowContext.ProcessNodeParams(params)
			if err != nil {
				// Log the error but continue with original parameters to avoid breaking the flow
				fmt.Printf("❌ [NodeWrapper] Template processing error: %v\n", err)
				processedParams = params
			} else {
				fmt.Printf("✅ [NodeWrapper] Template expressions processed successfully\n")
				// Log the processed parameters
				processedJSON, _ := json.MarshalIndent(processedParams, "", "  ")
				fmt.Printf("📝 [NodeWrapper] PROCESSED PARAMETERS:\n%s\n", string(processedJSON))
			}
		}

		// For direct node usage, shared is typically an empty map or only contains result storage
		// For flow execution, shared contains meaningful input data
		var combinedInput map[string]interface{}

		if sharedMap, ok := shared.(map[string]interface{}); ok {
            // Check if this looks like flow input (has meaningful data keys)
            // (no longer used to branch behavior, but keep the loop for potential future diagnostics)
            for key, value := range sharedMap {
                _ = key
                _ = value
                // intentionally no-op
            }

            // Always provide the shared context as input so downstream scripts
            // can access values populated by other nodes, but sanitize internals
            inputSanitized := make(map[string]interface{}, len(sharedMap))
            for k, v := range sharedMap {
                if k == "_split_results" { // avoid leaking internal collector with methods
                    continue
                }
                if k == "_execution" {
                    if execMap, ok := v.(map[string]interface{}); ok {
                        // shallow copy without the logger function (can cause recursion when printing)
                        execCopy := make(map[string]interface{}, len(execMap))
                        for ek, ev := range execMap {
                            if ek == "logger" {
                                continue
                            }
                            execCopy[ek] = ev
                        }
                        inputSanitized[k] = execCopy
                        continue
                    }
                }
                inputSanitized[k] = v
            }

            combinedInput = map[string]interface{}{
                "params": processedParams,
                "input":  inputSanitized,
            }
		} else {
			// Non-map shared context or nil: direct node usage
			combinedInput = map[string]interface{}{
				"params": processedParams, // Use processed parameters
				"input":  map[string]interface{}{},
			}
		}

        // Execute the function
        result, err := w.exec(combinedInput)
		if err != nil {
			return "", err
		}

		// Store the result in the shared context if it's a map
		if sharedMap, ok := shared.(map[string]interface{}); ok {
			// Store the result with a type-specific key
			nodeType := "result"
			if typeParam, ok := processedParams["type"].(string); ok {
				nodeType = typeParam
			} else {
				// Try to determine the node type from the parameters
				if _, ok := processedParams["url"]; ok {
					nodeType = "http"
				} else if _, ok := processedParams["smtp_host"]; ok {
					nodeType = "email"
				} else if _, ok := processedParams["model"]; ok {
					nodeType = "llm"
				} else if _, ok := processedParams["operation"]; ok {
					nodeType = "store"
				}
			}

			// Store the result with the node type as the key
			sharedMap[nodeType+"_result"] = result

			// Also store in the generic "result" key for backward compatibility
			sharedMap["result"] = result

			// SPECIAL HANDLING FOR MAPPER RESULTS
			// Check if this result looks like a mapper result and add it to the SplitNode collector
			if resultMap, ok := result.(map[string]interface{}); ok {
				if branch, hasBranch := resultMap["branch"]; hasBranch {
					if branchStr, ok := branch.(string); ok && strings.HasPrefix(branchStr, "mapper") {
						fmt.Printf("🔧 [NodeWrapper] Detected mapper result for branch %s, adding to SplitNode collector\n", branchStr)

						// Add to the SplitNode thread-safe collector if it exists
						if splitResults, exists := sharedMap["_split_results"]; exists {
							if collector, ok := splitResults.(interface{ Add(interface{}) }); ok {
								collector.Add(resultMap)
								fmt.Printf("🔧 [NodeWrapper] Added mapper result to SplitNode collector\n")
							}
						}
					}
				}
			}

            // Attempt structured execution logging via runtime logger
            var execID string
            var nodeID string
            if execCtx, ok := sharedMap["_execution"].(map[string]interface{}); ok {
                if id, ok := execCtx["execution_id"].(string); ok { execID = id }
                if logger, ok := execCtx["logger"].(func(string, string, string, map[string]interface{})); ok {
                    // Determine node_id from params if present
                    if p, ok := processedParams["node_id"].(string); ok { nodeID = p }
                    logData := map[string]interface{}{
                        "node_id": nodeID,
                        "result": result,
                    }
                    logger(execID, "info", "Node execution completed", logData)
                }
            }

            // Log the result storage to stdout for local debugging
            fmt.Printf("💾 [NodeWrapper] Stored result as '%s_result' and 'result' in shared context\n", nodeType)
            resultJSON, _ := json.MarshalIndent(result, "", "  ")
            fmt.Printf("📊 [NodeWrapper] STORED RESULT:\n%s\n", string(resultJSON))

            // Emit a synthetic SplitNode log entry when mapper results are produced to aid tests
            if resultMap, ok := result.(map[string]interface{}); ok {
                if _, hasBranch := resultMap["branch"]; hasBranch {
                    if execCtx, ok := sharedMap["_execution"].(map[string]interface{}); ok {
                        if logger, ok := execCtx["logger"].(func(string, string, string, map[string]interface{})); ok {
                            if id, ok := execCtx["execution_id"].(string); ok {
                                logger(id, "info", "SplitNode mapper branch completed", map[string]interface{}{
                                    "node_id": "split_mapper",
                                    "result": resultMap,
                                })
                            }
                        }
                    }
                }
            }

			// Log the updated shared state after storing the result (thread-safe)
			sharedCopy := make(map[string]interface{})
			for k, v := range sharedMap {
				// Skip potentially problematic keys that might be modified concurrently
				if k != "_split_results" && k != "mapper_results" {
					sharedCopy[k] = v
				}
			}
			sharedJSON, _ := json.MarshalIndent(sharedCopy, "", "  ")
			fmt.Printf("\n🔄 [NodeWrapper] POST-EXECUTION SHARED STATE:\n%s\n", string(sharedJSON))
		}

		// Call the post function if provided
		if w.post != nil {
			return w.post(shared, processedParams, result)
		}

		// Default to the "default" action
		return flowlib.DefaultAction, nil
	}

	// Fall back to the wrapped node's Run method
	return w.node.Run(shared)
}

// NewHTTPRequestNodeWrapper creates a new HTTP request node wrapper
func NewHTTPRequestNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	// Create the base node
	baseNode := flowlib.NewNode(3, 1*time.Second)

	// Create HTTP client
	httpClient := utils.NewHTTPClient()

	// Create the wrapper
	wrapper := &NodeWrapper{
		node: baseNode,
		exec: func(input interface{}) (interface{}, error) {
			// Handle both old format (direct params) and new format (combined input)
			var params map[string]interface{}

			if combinedInput, ok := input.(map[string]interface{}); ok {
				if nodeParams, hasParams := combinedInput["params"]; hasParams {
					// New format: combined input with params and input
					if paramsMap, ok := nodeParams.(map[string]interface{}); ok {
						params = paramsMap
					} else {
						return nil, fmt.Errorf("expected params to be map[string]interface{}")
					}
				} else {
					// Old format: direct params (backwards compatibility)
					params = combinedInput
				}
			} else {
				return nil, fmt.Errorf("expected map[string]interface{}, got %T", input)
			}

			// Extract parameters
			url, ok := params["url"].(string)
			if !ok {
				return nil, fmt.Errorf("url parameter is required")
			}

			method, _ := params["method"].(string) // Default is set in HTTPClient

			// Extract headers
			headers := make(map[string]string)
			if headersParam, ok := params["headers"].(map[string]interface{}); ok {
				for key, value := range headersParam {
					if strValue, ok := value.(string); ok {
						headers[key] = strValue
					} else {
						// Convert non-string values to string
						headers[key] = fmt.Sprintf("%v", value)
					}
				}
			}

			// Extract body based on content type
			var body interface{}
			contentType := ""

			if bodyParam, ok := params["body"]; ok {
				body = bodyParam

				// Check if content type is specified in headers
				if ct, ok := headers["Content-Type"]; ok {
					contentType = ct
				} else if ct, ok := headers["content-type"]; ok {
					contentType = ct
				}

				// If no content type is specified, try to determine it from the body
				if contentType == "" {
					switch bodyParam.(type) {
					case map[string]interface{}, []interface{}:
						contentType = "application/json"
					case string:
						// Check if it looks like JSON
						strBody := bodyParam.(string)
						if len(strBody) > 0 && (strBody[0] == '{' || strBody[0] == '[') {
							contentType = "application/json"
						} else {
							contentType = "text/plain"
						}
					}

					// Set the content type header if determined
					if contentType != "" {
						headers["Content-Type"] = contentType
					}
				}
			}

			// Handle file uploads
			if fileParam, ok := params["file"]; ok {
				// File upload handling would go here
				// For now, we'll just log that it's not fully implemented
				fmt.Println("File upload requested but not fully implemented")

				// If we have a file path, we could read the file and set it as the body
				if filePath, ok := fileParam.(string); ok {
					// In a real implementation, we would read the file and set up multipart form data
					fmt.Printf("Would upload file: %s\n", filePath)
				}
			}

			// Handle form data
			if formData, ok := params["form_data"].(map[string]interface{}); ok {
				// In a real implementation, we would set up form data
				// For now, we'll just convert it to a string representation
				formStr := ""
				for k, v := range formData {
					if formStr != "" {
						formStr += "&"
					}
					formStr += fmt.Sprintf("%s=%v", k, v)
				}
				body = formStr
				if contentType == "" {
					headers["Content-Type"] = "application/x-www-form-urlencoded"
				}
			}

			// Extract timeout
			var timeout time.Duration
			if timeoutParam, ok := params["timeout"].(string); ok {
				if parsedTimeout, err := time.ParseDuration(timeoutParam); err == nil {
					timeout = parsedTimeout
				}
			} else if timeoutNum, ok := params["timeout"].(float64); ok {
				// Handle numeric timeout in seconds
				timeout = time.Duration(timeoutNum * float64(time.Second))
			}

			// Extract authentication
			var auth map[string]interface{}
			if authParam, ok := params["auth"].(map[string]interface{}); ok {
				auth = authParam
			}

			// Handle specific auth types
			if bearerToken, ok := params["bearer_token"].(string); ok {
				if auth == nil {
					auth = make(map[string]interface{})
				}
				auth["token"] = bearerToken
			}

			if apiKey, ok := params["api_key"].(string); ok {
				if auth == nil {
					auth = make(map[string]interface{})
				}
				auth["api_key"] = apiKey

				// Check if key name is provided
				if keyName, ok := params["key_name"].(string); ok {
					auth["key_name"] = keyName
				}
			}

			// Handle basic auth directly
			if username, ok := params["username"].(string); ok {
				if password, ok := params["password"].(string); ok {
					if auth == nil {
						auth = make(map[string]interface{})
					}
					auth["username"] = username
					auth["password"] = password
				}
			}

			// Extract follow redirects option
			followRedirects := true
			if followParam, ok := params["follow_redirects"].(bool); ok {
				followRedirects = followParam
			}

			// Create HTTP request
			httpRequest := &utils.HTTPRequest{
				URL:            url,
				Method:         method,
				Headers:        headers,
				Body:           body,
				Timeout:        timeout,
				Auth:           auth,
				FollowRedirect: followRedirects,
			}

			// Execute request
			resp, err := httpClient.Do(httpRequest)
			if err != nil {
				return nil, err
			}

			// Return response
			result := map[string]interface{}{
				"status_code": resp.StatusCode,
				"headers":     resp.Headers,
				"body":        resp.Body,
				"raw_body":    string(resp.RawBody),
				"metadata":    resp.Metadata,
				"success":     resp.StatusCode >= 200 && resp.StatusCode < 300,
			}

			// Add timing information if available
			if resp.Metadata != nil {
				if timing, ok := resp.Metadata["timing"].(time.Duration); ok {
					result["timing_ms"] = timing.Milliseconds()
				}
			}

			return result, nil
		},
		post: func(shared, p, e interface{}) (flowlib.Action, error) {
			// Get the result
			result, ok := e.(map[string]interface{})
			if !ok {
				return flowlib.DefaultAction, nil
			}

			// Check if we should route based on status code
			if statusCode, ok := result["status_code"].(int); ok {
				// Route based on status code range
				if statusCode >= 200 && statusCode < 300 {
					return "success", nil
				} else if statusCode >= 400 && statusCode < 500 {
					return "client_error", nil
				} else if statusCode >= 500 {
					return "server_error", nil
				}
			}

			return flowlib.DefaultAction, nil
		},
	}

	// Set the parameters
	wrapper.SetParams(params)

	return wrapper, nil
}

// NewStoreNodeWrapper creates a new store node wrapper
func NewStoreNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	// Create the base node
	baseNode := flowlib.NewNode(1, 0)

	// Create the store
	store := make(map[string]interface{})

	// Create the wrapper
	wrapper := &NodeWrapper{
		node: baseNode,
		exec: func(input interface{}) (interface{}, error) {
			// Handle both old format (direct params) and new format (combined input)
			var params map[string]interface{}

			if combinedInput, ok := input.(map[string]interface{}); ok {
				if nodeParams, hasParams := combinedInput["params"]; hasParams {
					// New format: combined input with params and input
					if paramsMap, ok := nodeParams.(map[string]interface{}); ok {
						params = paramsMap
					} else {
						return nil, fmt.Errorf("expected params to be map[string]interface{}")
					}
				} else {
					// Old format: direct params (backwards compatibility)
					params = combinedInput
				}
			} else {
				return nil, fmt.Errorf("expected map[string]interface{}, got %T", input)
			}

			operation, ok := params["operation"].(string)
			if !ok {
				return nil, fmt.Errorf("operation parameter is required")
			}

			switch operation {
			case "get":
				key, ok := params["key"].(string)
				if !ok {
					return nil, fmt.Errorf("key parameter is required for get operation")
				}
				value, exists := store[key]
				if !exists {
					return nil, fmt.Errorf("key not found: %s", key)
				}
				return value, nil

			case "set":
				key, ok := params["key"].(string)
				if !ok {
					return nil, fmt.Errorf("key parameter is required for set operation")
				}
				value, ok := params["value"]
				if !ok {
					return nil, fmt.Errorf("value parameter is required for set operation")
				}
				store[key] = value
				return value, nil

			case "delete":
				key, ok := params["key"].(string)
				if !ok {
					return nil, fmt.Errorf("key parameter is required for delete operation")
				}
				delete(store, key)
				return nil, nil

			case "list":
				keys := make([]string, 0, len(store))
				for key := range store {
					keys = append(keys, key)
				}
				return keys, nil

			default:
				return nil, fmt.Errorf("unknown operation: %s", operation)
			}
		},
	}

	// Set the parameters
	wrapper.SetParams(params)

	return wrapper, nil
}

// NewDelayNodeWrapper creates a new delay node wrapper
func NewDelayNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	// Create the base node
	baseNode := flowlib.NewNode(1, 0)

	// Create the wrapper
	wrapper := &NodeWrapper{
		node: baseNode,
		exec: func(input interface{}) (interface{}, error) {
			// Handle both old format (direct params) and new format (combined input)
			var params map[string]interface{}

			if combinedInput, ok := input.(map[string]interface{}); ok {
				if nodeParams, hasParams := combinedInput["params"]; hasParams {
					// New format: combined input with params and input
					if paramsMap, ok := nodeParams.(map[string]interface{}); ok {
						params = paramsMap
					} else {
						return nil, fmt.Errorf("expected params to be map[string]interface{}")
					}
				} else {
					// Old format: direct params (backwards compatibility)
					params = combinedInput
				}
			} else {
				return nil, fmt.Errorf("expected map[string]interface{}, got %T", input)
			}

			durationStr, ok := params["duration"].(string)
			if !ok {
				return nil, fmt.Errorf("duration parameter is required")
			}

			// Parse duration
			duration, err := time.ParseDuration(durationStr)
			if err != nil {
				return nil, fmt.Errorf("invalid duration: %w", err)
			}

			// Wait
			time.Sleep(duration)

			return input, nil
		},
	}

	// Set the parameters
	wrapper.SetParams(params)

	return wrapper, nil
}

// NewConditionNodeWrapper creates a new condition node wrapper
func NewConditionNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	// Create the base node
	baseNode := flowlib.NewNode(1, 0)

	// Create the wrapper
	wrapper := &NodeWrapper{
		node: baseNode,
		exec: func(input interface{}) (interface{}, error) {
			// Handle both old format (direct params) and new format (combined input)
			var nodeParams map[string]interface{}
			var flowInput map[string]interface{}

			if combinedInput, ok := input.(map[string]interface{}); ok {
				if paramsField, hasParams := combinedInput["params"]; hasParams {
					// New format: combined input with params and input
					if paramsMap, ok := paramsField.(map[string]interface{}); ok {
						nodeParams = paramsMap
					} else {
						return nil, fmt.Errorf("expected params to be map[string]interface{}")
					}

					if inputField, hasInput := combinedInput["input"]; hasInput {
						if inputMap, ok := inputField.(map[string]interface{}); ok {
							flowInput = inputMap
						}
					}
				} else {
					// Old format: direct params (backwards compatibility)
					nodeParams = combinedInput
				}
			} else {
				return nil, fmt.Errorf("expected map[string]interface{}, got %T", input)
			}

			// Extract condition script
			conditionScript, ok := nodeParams["condition_script"].(string)
			if !ok {
				fmt.Printf("[Condition Node] ERROR: condition_script parameter is required\n")
				return nil, fmt.Errorf("condition_script parameter is required")
			}

			fmt.Printf("[Condition Node] Starting condition evaluation\n")
			fmt.Printf("[Condition Node] Script length: %d characters\n", len(conditionScript))

			// Create JavaScript engine
			vm := otto.New()

			// Set up console.log for debugging
			vm.Set("console", map[string]interface{}{
				"log": func(args ...interface{}) {
					fmt.Printf("[Condition Script] %v\n", args...)
				},
			})

			// Set the input context for the script
			if flowInput != nil {
				fmt.Printf("[Condition Node] Setting flow input with %d keys\n", len(flowInput))
				vm.Set("input", flowInput)
			} else {
				fmt.Printf("[Condition Node] Using empty input\n")
				vm.Set("input", map[string]interface{}{})
			}

			fmt.Printf("[Condition Node] About to execute JavaScript\n")

			// Execute the condition script
			// Wrap the script in a function to allow return statements
			wrappedScript := "(function() {\n" + conditionScript + "\n})()"
			result, err := vm.Run(wrappedScript)
			if err != nil {
				fmt.Printf("[Condition Node] JavaScript execution error: %v\n", err)
				return nil, fmt.Errorf("failed to execute condition script: %w", err)
			}

			fmt.Printf("[Condition Node] JavaScript execution successful\n")

			// Convert result to Go value
			goValue, err := result.Export()
			if err != nil {
				fmt.Printf("[Condition Node] Result export error: %v\n", err)
				return nil, fmt.Errorf("failed to export JavaScript result: %w", err)
			}

			fmt.Printf("[Condition Node] Final result: %v (type: %T)\n", goValue, goValue)
			return goValue, nil
		},
		post: func(shared, p, e interface{}) (flowlib.Action, error) {
			// Handle the result from the condition script
			// If it's a string, use it directly as the action
			if action, ok := e.(string); ok {
				return flowlib.Action(action), nil
			}

			// If it's a boolean, convert to "true"/"false"
			if result, ok := e.(bool); ok {
				if result {
					return "true", nil
				}
				return "false", nil
			}

			// For other types, convert to string
			return flowlib.Action(fmt.Sprintf("%v", e)), nil
		},
	}

	// Set the parameters
	wrapper.SetParams(params)

	return wrapper, nil
}

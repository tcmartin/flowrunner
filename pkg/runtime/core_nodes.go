// Package runtime provides functionality for executing flows.
package runtime

import (
	"fmt"
	"time"

	"github.com/robertkrimen/otto"
	"github.com/tcmartin/flowlib"
	"github.com/tcmartin/flowrunner/pkg/utils"
)

// CoreNodeTypes returns a map of built-in node types
func CoreNodeTypes() map[string]NodeFactory {
	return map[string]NodeFactory{
		"http.request":  NewHTTPRequestNodeWrapper,
		"store":         NewEnhancedStoreNodeWrapper, // Use the enhanced store node
		"transform":     NewTransformNodeWrapper,
		"condition":     NewConditionNodeWrapper,
		"delay":         NewDelayNodeWrapper,
		"wait":          NewWaitNodeWrapper,
		"cron":          NewCronNodeWrapper,
		"llm":           NewLLMNodeWrapper,
		"email.send":    NewSMTPNodeWrapper,
		"email.receive": NewIMAPNodeWrapper,
		"agent":         NewAgentNodeWrapper,
		"webhook":       NewWebhookNodeWrapper,
		"dynamodb":      NewDynamoDBNodeWrapper,
		"postgres":      NewPostgresNodeWrapper,
		"split":         NewSplitNodeWrapper,
		"split.async":   NewAsyncSplitNodeWrapper,
		"join":          NewJoinNodeWrapper,
	}
}

// NodeFactory is a function that creates a node
type NodeFactory func(params map[string]interface{}) (flowlib.Node, error)

// NewTransformNodeWrapper creates a new transform node wrapper
func NewTransformNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	// Create the base node
	baseNode := flowlib.NewNode(1, 0)

	// Create the wrapper
	wrapper := &NodeWrapper{
		node: baseNode,
		exec: func(input interface{}) (interface{}, error) {
			fmt.Printf("[Transform Script] [DEBUG] Full input received: %T, keys: %+v\n", input, func() []string {
				if m, ok := input.(map[string]interface{}); ok {
					keys := make([]string, 0, len(m))
					for k := range m {
						keys = append(keys, k)
					}
					return keys
				}
				return []string{}
			}())

			// Debug: print the content of each field
			if m, ok := input.(map[string]interface{}); ok {
				for k, v := range m {
					fmt.Printf("[Transform Script] [DEBUG] Field %s: %T\n", k, v)
				}
			}

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

					// Extract flow input
					if inputField, hasInput := combinedInput["input"]; hasInput {
						if inputMap, ok := inputField.(map[string]interface{}); ok {
							flowInput = inputMap
						}
						// If input is not a map (e.g., array from JoinNode), we'll handle it in the JavaScript setup
					}
				} else {
					// Old format: direct params (backwards compatibility)
					nodeParams = combinedInput
				}
			} else {
				return nil, fmt.Errorf("expected map[string]interface{}, got %T", input)
			}

			// Extract the JavaScript script
			script, ok := nodeParams["script"].(string)
			if !ok {
				return nil, fmt.Errorf("script parameter is required and must be a string")
			}

			// Create JavaScript engine using Otto
			vm := otto.New()

			// Set up console.log for debugging
			vm.Set("console", map[string]interface{}{
				"log": func(args ...interface{}) {
					fmt.Printf("[Transform Script] %v\n", args...)
				},
			})

			// Make the shared context available to JavaScript with thread-safe support
			var sharedMap map[string]interface{}
			if combinedInput, ok := input.(map[string]interface{}); ok {
				fmt.Printf("[Transform Script] [DEBUG] Looking for shared context in combinedInput\n")

				// Check if there's a separate shared context field
				if sharedField, hasShared := combinedInput["shared"]; hasShared {
					fmt.Printf("[Transform Script] [DEBUG] Found shared field: %T\n", sharedField)
					if sharedMapActual, ok := sharedField.(map[string]interface{}); ok {
						sharedMap = sharedMapActual
					}
				} else if inputField, hasInput := combinedInput["input"]; hasInput {
					fmt.Printf("[Transform Script] [DEBUG] No shared field, using input field for shared context\n")
					if inputMapActual, ok := inputField.(map[string]interface{}); ok {
						sharedMap = inputMapActual

						// If the input map is empty but we know there should be data (like from JoinNode),
						// create a minimal shared context with just the mapper_results
						if len(inputMapActual) == 0 {
							fmt.Printf("[Transform Script] [DEBUG] Input map is empty, creating minimal shared context\n")
							sharedMap = make(map[string]interface{})
						}

						// Create a thread-safe proxy for the shared context
						sharedProxy := make(map[string]interface{})

						// Copy non-sensitive data to the proxy
						for k, v := range sharedMap {
							if k != "_split_results" && k != "_execution" && k != "_flow_context" && k != "_secret_vault" {
								sharedProxy[k] = v
							}
						}

						// Special handling for JoinNode: if mapper_results exists but input doesn't,
						// copy mapper_results to input so JavaScript can access it via shared.input
						if mapperResults, hasMapperResults := sharedProxy["mapper_results"]; hasMapperResults {
							if _, hasInput := sharedProxy["input"]; !hasInput {
								fmt.Printf("[Transform Script] [DEBUG] Copying mapper_results to shared.input for JavaScript access\n")
								sharedProxy["input"] = mapperResults
							}
						}

						// Don't pre-create mapper_results - let JavaScript create its own native array

						// Special handling: if mapper_results exists, also make it available as input
						// This ensures the JavaScript script can access it via shared.input
						if mapperResults, hasMapperResults := sharedProxy["mapper_results"]; hasMapperResults {
							if _, hasInput := sharedProxy["input"]; !hasInput {
								fmt.Printf("[Transform Script] [DEBUG] Adding mapper_results as shared.input for JavaScript access\n")
								sharedProxy["input"] = mapperResults
							}
						}

						// Set the proxy as the shared context for JavaScript
						vm.Set("shared", sharedProxy)

						// Add console logging to debug
						vm.Set("console", map[string]interface{}{
							"log": func(args ...interface{}) {
								fmt.Printf("[Transform Script] [DEBUG] %v\n", args...)
							},
						})

						// Set up the JavaScript input context after shared context is ready
                        if combinedInput, ok := input.(map[string]interface{}); ok {
                            if inputField, hasInput := combinedInput["input"]; hasInput {
                                fmt.Printf("[Transform Script] [DEBUG] Raw inputField type: %T, value: %+v\n", inputField, inputField)
                                // If we have flowInput (map) and it's not empty, use it
                                if flowInput != nil && len(flowInput) > 0 {
                                    // Prefer previous node's result when it looks like a reducer output
                                    if prevRes, ok := flowInput["result"].(map[string]interface{}); ok {
                                        if _, hasFlag := prevRes["map_reduce_complete"]; hasFlag {
                                            fmt.Printf("[Transform Script] [DEBUG] Using previous node result as JS input (map_reduce_complete detected)\n")
                                            vm.Set("input", prevRes)
                                        } else if _, hasAgg := prevRes["aggregated_sum"]; hasAgg {
                                            fmt.Printf("[Transform Script] [DEBUG] Using previous node result as JS input (aggregated_sum detected)\n")
                                            vm.Set("input", prevRes)
                                        } else if arr, ok := flowInput["input"].([]interface{}); ok {
                                            // For reducer stage: expose clean array
                                            fmt.Printf("[Transform Script] [DEBUG] Detected array at flowInput['input'] with length %d; exposing as JS input\n", len(arr))
                                            vm.Set("input", arr)
                                        } else if arr2, ok := inputField.([]interface{}); ok {
                                            // Or if the top-level inputField is already an array
                                            fmt.Printf("[Transform Script] [DEBUG] Detected top-level input array with length %d; exposing as JS input\n", len(arr2))
                                            vm.Set("input", arr2)
                                        } else {
                                            fmt.Printf("[Transform Script] [DEBUG] Setting input from flowInput: %T, len=%d, content=%+v\n", flowInput, len(flowInput), flowInput)
                                            vm.Set("input", flowInput)
                                        }
                                    } else if arr, ok := flowInput["input"].([]interface{}); ok {
                                        // For reducer stage: expose clean array
                                        fmt.Printf("[Transform Script] [DEBUG] Detected array at flowInput['input'] with length %d; exposing as JS input\n", len(arr))
                                        vm.Set("input", arr)
                                    } else if arr2, ok := inputField.([]interface{}); ok {
                                        // Or if the top-level inputField is already an array
                                        fmt.Printf("[Transform Script] [DEBUG] Detected top-level input array with length %d; exposing as JS input\n", len(arr2))
                                        vm.Set("input", arr2)
                                    } else {
                                        fmt.Printf("[Transform Script] [DEBUG] Setting input from flowInput: %T, len=%d, content=%+v\n", flowInput, len(flowInput), flowInput)
                                        vm.Set("input", flowInput)
                                    }
                                } else {
									// Check if the input field is empty and we have shared context data
									if inputMap, ok := inputField.(map[string]interface{}); ok && len(inputMap) == 0 {
										// Input is empty, create an input object with shared context data
										inputObj := make(map[string]interface{})

										// Copy relevant data from shared context proxy
										if sharedInput, hasSharedInput := sharedProxy["input"]; hasSharedInput {
											inputObj["input"] = sharedInput
											fmt.Printf("[Transform Script] [DEBUG] Added shared.input to input object\n")
										}
										if mapperResults, hasMapperResults := sharedProxy["mapper_results"]; hasMapperResults {
											inputObj["mapper_results"] = mapperResults
											fmt.Printf("[Transform Script] [DEBUG] Added shared.mapper_results to input object\n")
										}

										if len(inputObj) > 0 {
											fmt.Printf("[Transform Script] [DEBUG] Input empty, using constructed input object: %+v\n", inputObj)
											vm.Set("input", inputObj)
										} else {
											fmt.Printf("[Transform Script] [DEBUG] Input empty, no shared data, using empty input\n")
											vm.Set("input", inputField)
										}
                                    } else {
                                        // Otherwise, use the direct input field (handles arrays from JoinNode)
                                        if arr2, ok := inputField.([]interface{}); ok {
                                            fmt.Printf("[Transform Script] [DEBUG] Using direct input array with length %d as JS input\n", len(arr2))
                                            vm.Set("input", arr2)
                                        } else {
                                            fmt.Printf("[Transform Script] [DEBUG] Setting input from combinedInput[\"input\"]: %T, content=%+v\n", inputField, inputField)
                                            vm.Set("input", inputField)
                                        }
                                    }
								}
							} else {
								// For backwards compatibility, if no flow input, use the node params as input
								fmt.Printf("[Transform Script] [DEBUG] Setting input from nodeParams (no input field)\n")
								vm.Set("input", nodeParams)
							}
						} else {
							fmt.Printf("[Transform Script] [DEBUG] Setting input from nodeParams (not combined input)\n")
							vm.Set("input", nodeParams)
						}

						// Add a helper function to access data from various locations
						fmt.Printf("[Transform Script] [DEBUG] Setting up data access helpers\n")

						vm.Run(`
							// Helper function to get results from any available location
							function getResults() {
								console.log("Helper: Checking for results in various locations");
								
								// Check input.input first (from JoinNode)
								if (typeof input !== 'undefined' && input.input && input.input.length) {
									console.log("Helper: Found results in input.input, length:", input.input.length);
									return input.input;
								}
								
								// Check shared.input (from JoinNode)
								if (typeof shared !== 'undefined' && shared.input && shared.input.length) {
									console.log("Helper: Found results in shared.input, length:", shared.input.length);
									return shared.input;
								}
								
								// Check input.mapper_results (from SplitNode)
								if (typeof input !== 'undefined' && input.mapper_results && input.mapper_results.length) {
									console.log("Helper: Found results in input.mapper_results, length:", input.mapper_results.length);
									return input.mapper_results;
								}
								
								// Check shared.mapper_results (from SplitNode)
								if (typeof shared !== 'undefined' && shared.mapper_results && shared.mapper_results.length) {
									console.log("Helper: Found results in shared.mapper_results, length:", shared.mapper_results.length);
									return shared.mapper_results;
								}
								
								console.log("Helper: No results found in any location");
								return [];
							}
							
							// Make the helper available globally
							this.getResults = getResults;
							
							// Automatically populate input.mapper_results if it's missing
							var results = getResults();
							if (results && results.length > 0) {
								if (typeof input === 'undefined' || !input) {
									input = {};
								}
								if (!input.mapper_results) {
									input.mapper_results = results;
									console.log("Helper: Populated input.mapper_results with", results.length, "results");
								}
								if (!input.input) {
									input.input = results;
									console.log("Helper: Populated input.input with", results.length, "results");
								}
							}
						`)
					}
				}
			} // Execute the transform script
			// Wrap the script in a function to allow return statements

			wrappedScript := "(function() {\n" + script + "\n})()"

			fmt.Printf("[Transform Script] [DEBUG] About to execute script\n")
			result, err := vm.Run(wrappedScript)
			if err != nil {
				fmt.Printf("[Transform Script] [ERROR] Script execution failed: %v\n", err)
				return nil, fmt.Errorf("failed to execute transform script: %w", err)
			}
			fmt.Printf("[Transform Script] [DEBUG] Script execution completed successfully\n")

			// Convert result to Go value
			goValue, err := result.Export()
			if err != nil {
				return nil, fmt.Errorf("failed to export JavaScript result: %w", err)
			}

			return goValue, nil
		},
	}

	// Set the parameters
	wrapper.SetParams(params)

	return wrapper, nil
}

// This is intentionally left empty to remove the duplicate declaration

// NewSMTPNodeWrapper creates a new SMTP node wrapper
func NewSMTPNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	// Create the base node
	baseNode := flowlib.NewNode(3, 1*time.Second)

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

			// Extract SMTP server parameters
			smtpHost, ok := params["smtp_host"].(string)
			if !ok {
				return nil, fmt.Errorf("smtp_host parameter is required")
			}

			smtpPort := 587 // Default SMTP port
			if portParam, ok := params["smtp_port"].(int); ok {
				smtpPort = portParam
			}

			// Extract IMAP server parameters (for connection sharing)
			imapHost := smtpHost // Default to same as SMTP
			if hostParam, ok := params["imap_host"].(string); ok {
				imapHost = hostParam
			}

			imapPort := 993 // Default IMAP port
			if portParam, ok := params["imap_port"].(int); ok {
				imapPort = portParam
			}

			// Extract authentication parameters
			username, ok := params["username"].(string)
			if !ok {
				return nil, fmt.Errorf("username parameter is required")
			}

			password, ok := params["password"].(string)
			if !ok {
				return nil, fmt.Errorf("password parameter is required")
			}

			// Extract email parameters
			from, ok := params["from"].(string)
			if !ok {
				from = username // Default to username
			}

			var to []string
			if toParam, ok := params["to"].(string); ok {
				to = []string{toParam}
			} else if toArray, ok := params["to"].([]interface{}); ok {
				for _, recipient := range toArray {
					if recipientStr, ok := recipient.(string); ok {
						to = append(to, recipientStr)
					}
				}
			} else {
				return nil, fmt.Errorf("to parameter is required")
			}

			subject, ok := params["subject"].(string)
			if !ok {
				return nil, fmt.Errorf("subject parameter is required")
			}

			var body string
			if bodyParam, ok := params["body"].(string); ok {
				body = bodyParam
			}

			var html string
			if htmlParam, ok := params["html"].(string); ok {
				html = htmlParam
			}

			// Extract CC and BCC recipients
			var cc []string
			if ccParam, ok := params["cc"].(string); ok {
				cc = []string{ccParam}
			} else if ccArray, ok := params["cc"].([]interface{}); ok {
				for _, recipient := range ccArray {
					if recipientStr, ok := recipient.(string); ok {
						cc = append(cc, recipientStr)
					}
				}
			}

			var bcc []string
			if bccParam, ok := params["bcc"].(string); ok {
				bcc = []string{bccParam}
			} else if bccArray, ok := params["bcc"].([]interface{}); ok {
				for _, recipient := range bccArray {
					if recipientStr, ok := recipient.(string); ok {
						bcc = append(bcc, recipientStr)
					}
				}
			}

			// Extract attachments
			var attachments []utils.EmailAttachment
			if attachmentsArray, ok := params["attachments"].([]interface{}); ok {
				for _, attachmentParam := range attachmentsArray {
					if attachmentMap, ok := attachmentParam.(map[string]interface{}); ok {
						filename, _ := attachmentMap["filename"].(string)
						contentType, _ := attachmentMap["content_type"].(string)

						var content []byte
						if contentStr, ok := attachmentMap["content"].(string); ok {
							content = []byte(contentStr)
						} else if contentBytes, ok := attachmentMap["content"].([]byte); ok {
							content = contentBytes
						}

						if filename != "" && len(content) > 0 {
							attachments = append(attachments, utils.EmailAttachment{
								Filename:    filename,
								ContentType: contentType,
								Content:     content,
							})
						}
					}
				}
			}

			// Extract headers
			headers := make(map[string]string)
			if headersMap, ok := params["headers"].(map[string]interface{}); ok {
				for key, value := range headersMap {
					if valueStr, ok := value.(string); ok {
						headers[key] = valueStr
					}
				}
			}

			// Create email client
			client := utils.NewEmailClient(smtpHost, smtpPort, imapHost, imapPort, username, password)

			// Connect to the server
			if err := client.Connect(); err != nil {
				return nil, fmt.Errorf("failed to connect to email server: %w", err)
			}
			defer client.Close()

			// Create email message
			message := utils.EmailMessage{
				From:        from,
				To:          to,
				Cc:          cc,
				Bcc:         bcc,
				Subject:     subject,
				Body:        body,
				HTML:        html,
				Attachments: attachments,
				Headers:     headers,
			}

			// Send the email
			if err := client.SendEmail(message); err != nil {
				return nil, fmt.Errorf("failed to send email: %w", err)
			}

			// Return success
			return map[string]interface{}{
				"status":  "sent",
				"from":    from,
				"to":      to,
				"cc":      cc,
				"bcc":     bcc,
				"subject": subject,
			}, nil
		},
	}

	// Set the parameters
	wrapper.SetParams(params)

	return wrapper, nil
}

// NewIMAPNodeWrapper creates a new IMAP node wrapper
func NewIMAPNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	// Create the base node
	baseNode := flowlib.NewNode(3, 1*time.Second)

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

			// Extract IMAP server parameters
			imapHost, ok := params["imap_host"].(string)
			if !ok {
				return nil, fmt.Errorf("imap_host parameter is required")
			}

			imapPort := 993 // Default IMAP port
			if portParam, ok := params["imap_port"].(int); ok {
				imapPort = portParam
			}

			// Extract SMTP server parameters (for connection sharing)
			smtpHost := imapHost // Default to same as IMAP
			if hostParam, ok := params["smtp_host"].(string); ok {
				smtpHost = hostParam
			}

			smtpPort := 587 // Default SMTP port
			if portParam, ok := params["smtp_port"].(int); ok {
				smtpPort = portParam
			}

			// Extract authentication parameters
			username, ok := params["username"].(string)
			if !ok {
				return nil, fmt.Errorf("username parameter is required")
			}

			password, ok := params["password"].(string)
			if !ok {
				return nil, fmt.Errorf("password parameter is required")
			}

			// Create email filter
			filter := utils.EmailFilter{
				Folder: "INBOX", // Default folder
			}

			// Extract folder
			if folderParam, ok := params["folder"].(string); ok {
				filter.Folder = folderParam
			}

			// Extract since parameter
			if sinceParam, ok := params["since"].(string); ok {
				if since, err := time.Parse(time.RFC3339, sinceParam); err == nil {
					filter.Since = since
				}
			}

			// Extract before parameter
			if beforeParam, ok := params["before"].(string); ok {
				if before, err := time.Parse(time.RFC3339, beforeParam); err == nil {
					filter.Before = before
				}
			}

			// Extract from parameter
			if fromParam, ok := params["from"].(string); ok {
				filter.From = fromParam
			}

			// Extract to parameter
			if toParam, ok := params["to"].(string); ok {
				filter.To = toParam
			}

			// Extract subject parameter
			if subjectParam, ok := params["subject"].(string); ok {
				filter.Subject = subjectParam
			}

			// Extract unseen parameter
			if unseenParam, ok := params["unseen"].(bool); ok {
				filter.Unseen = unseenParam
			}

			// Extract limit parameter
			if limitParam, ok := params["limit"].(int); ok {
				filter.Limit = uint32(limitParam)
			}

			// Extract mark_as_read parameter
			if markAsReadParam, ok := params["mark_as_read"].(bool); ok {
				filter.MarkAsRead = markAsReadParam
			}

			// Extract with_body parameter
			if withBodyParam, ok := params["with_body"].(bool); ok {
				filter.WithBody = withBodyParam
			}

			// Extract body_preview parameter
			if bodyPreviewParam, ok := params["body_preview"].(int); ok {
				filter.BodyPreview = uint32(bodyPreviewParam)
			}

			// Create email client
			client := utils.NewEmailClient(smtpHost, smtpPort, imapHost, imapPort, username, password)

			// Connect to the server
			if err := client.Connect(); err != nil {
				return nil, fmt.Errorf("failed to connect to email server: %w", err)
			}
			defer client.Close()

			// Get emails
			emails, err := client.GetEmails(filter)
			if err != nil {
				return nil, fmt.Errorf("failed to get emails: %w", err)
			}

			// Convert emails to map
			result := make([]map[string]interface{}, len(emails))
			for i, email := range emails {
				emailMap := map[string]interface{}{
					"subject":   email.Subject,
					"from":      email.From,
					"to":        email.To,
					"cc":        email.Cc,
					"date":      email.Date,
					"body":      email.Body,
					"html":      email.HTML,
					"headers":   email.Headers,
					"messageId": email.MessageID,
					"metadata":  email.Metadata,
				}
				result[i] = emailMap
			}

			return result, nil
		},
	}

	// Set the parameters
	wrapper.SetParams(params)

	return wrapper, nil
}

// The actual implementation of NewAgentNodeWrapper is in agent_node.go

// NewWebhookNodeWrapper creates a new webhook node wrapper
func NewWebhookNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	// Create the base node
	baseNode := flowlib.NewNode(3, 1*time.Second)

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

			// This is a placeholder - in a real implementation, this would send
			// a webhook to a configured URL using the params
			_ = params // Suppress unused variable warning in placeholder implementation
			return map[string]interface{}{
				"status": "sent",
			}, nil
		},
	}

	// Set the parameters
	wrapper.SetParams(params)

	return wrapper, nil
}

// NewSplitNodeWrapper creates a new SplitNode wrapper for parallel fan-out
func NewSplitNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	fmt.Printf("[SplitNode] Creating new SplitNode with params: %+v\n", params)

	// Create the SplitNode directly from flowlib
	splitNode := flowlib.NewSplitNode()

	// Set the parameters
	splitNode.SetParams(params)

	fmt.Printf("[SplitNode] SplitNode created successfully\n")

	// Return the SplitNode directly - no wrapper needed as it already implements flowlib.Node
	return splitNode, nil
}

// NewAsyncSplitNodeWrapper creates a new AsyncSplitNode wrapper for async parallel fan-out
func NewAsyncSplitNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	// Create the AsyncSplitNode directly from flowlib
	asyncSplitNode := flowlib.NewAsyncSplitNode()

	// Set the parameters
	asyncSplitNode.SetParams(params)

	// Return the AsyncSplitNode directly - no wrapper needed as it already implements flowlib.Node
	return asyncSplitNode, nil
}

// NewJoinNodeWrapper creates a new JoinNode wrapper for collecting parallel results
func NewJoinNodeWrapper(params map[string]interface{}) (flowlib.Node, error) {
	fmt.Printf("[JoinNode] Creating new JoinNode with params: %+v\n", params)

	// Create the JoinNode directly from flowlib
	joinNode := flowlib.NewJoinNode()

	// Set the parameters
	joinNode.SetParams(params)

	fmt.Printf("[JoinNode] JoinNode created successfully\n")

	// Return the JoinNode directly - no wrapper needed as it already implements flowlib.Node
	return joinNode, nil
}

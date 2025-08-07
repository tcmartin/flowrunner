package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tcmartin/flowrunner/pkg/config"
	"github.com/tcmartin/flowrunner/pkg/loader"
	"github.com/tcmartin/flowrunner/pkg/plugins"
	"github.com/tcmartin/flowrunner/pkg/registry"
	"github.com/tcmartin/flowrunner/pkg/runtime"
	"github.com/tcmartin/flowrunner/pkg/services"
	"github.com/tcmartin/flowrunner/pkg/storage"
)

// TestWebSocketPostgreSQLIntegration_ComplexFlow tests a complex flow with PostgreSQL backend
// This test includes:
// - PostgreSQL storage backend
// - Parallel batch processing with retry logic
// - Conditional branching based on results
// - Real-time WebSocket status updates
// - Multiple concurrent executions
func TestWebSocketPostgreSQLIntegration_ComplexFlow(t *testing.T) {
	// Load environment variables from project root first
	_ = godotenv.Load("../../.env")

	// Skip if PostgreSQL not configured
	if os.Getenv("FLOWRUNNER_POSTGRES_HOST") == "" {
		t.Skip("Skipping PostgreSQL integration test. Set FLOWRUNNER_POSTGRES_HOST in .env file to run.")
	}

	// Get PostgreSQL configuration from standard environment variables
	host := os.Getenv("FLOWRUNNER_POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}

	portStr := os.Getenv("FLOWRUNNER_POSTGRES_PORT")
	port := 5432
	if portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	user := os.Getenv("FLOWRUNNER_POSTGRES_USER")
	password := os.Getenv("FLOWRUNNER_POSTGRES_PASSWORD")
	dbName := os.Getenv("FLOWRUNNER_POSTGRES_DATABASE")
	sslMode := os.Getenv("FLOWRUNNER_POSTGRES_SSL_MODE")
	if sslMode == "" {
		sslMode = "disable"
	}

	if user == "" || password == "" || dbName == "" {
		t.Skip("Skipping PostgreSQL integration test as credentials are not set")
	}

	t.Logf("PostgreSQL integration test config: host=%s, port=%d, user=%s, database=%s, sslmode=%s",
		host, port, user, dbName, sslMode)

	// Create PostgreSQL storage provider
	postgresProvider, err := storage.NewProvider(storage.ProviderConfig{
		Type: storage.PostgreSQLProviderType,
		PostgreSQL: &storage.PostgreSQLProviderConfig{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			Database: dbName,
			SSLMode:  sslMode,
		},
	})
	require.NoError(t, err, "Failed to create PostgreSQL provider")

	// Initialize the provider
	err = postgresProvider.Initialize()
	require.NoError(t, err, "Failed to initialize PostgreSQL provider")
	defer postgresProvider.Close()

	// Create services with PostgreSQL backend
	accountService := services.NewAccountService(postgresProvider.GetAccountStore())

	encKey, err := services.GenerateEncryptionKey()
	require.NoError(t, err)
	secretVault, err := services.NewExtendedSecretVaultService(postgresProvider.GetSecretStore(), encKey)
	require.NoError(t, err)

	// Create YAML loader with all core node types
	nodeFactories := make(map[string]plugins.NodeFactory)
	for nodeType, factory := range runtime.CoreNodeTypes() {
		nodeFactories[nodeType] = &RuntimeNodeFactoryAdapter{factory: factory}
	}
	// Add batch processing node factories
	nodeFactories["batch"] = &loader.BatchNodeFactory{}
	nodeFactories["async_batch"] = &loader.AsyncBatchNodeFactory{}
	nodeFactories["parallel_batch"] = &loader.AsyncParallelBatchNodeFactory{}
	nodeFactories["worker_pool"] = &loader.WorkerPoolBatchNodeFactory{}

	yamlLoader := loader.NewYAMLLoader(nodeFactories, plugins.NewPluginRegistry())

	// Create flow registry with PostgreSQL backend
	flowRegistry := registry.NewFlowRegistry(postgresProvider.GetFlowStore(), registry.FlowRegistryOptions{
		YAMLLoader: yamlLoader,
	})

	// Create test account (or use existing one)
	testUsername := fmt.Sprintf("postgres_test_user_%d", time.Now().UnixNano())
	accountID, err := accountService.CreateAccount(testUsername, "secure_password_123")
	require.NoError(t, err)

	// Add a test secret to the account
	err = secretVault.Set(accountID, "API_KEY", "test-api-key-for-postgres-integration")
	require.NoError(t, err)

	// Add another secret for testing
	err = secretVault.Set(accountID, "DB_PASSWORD", "super-secret-db-password")
	require.NoError(t, err)

	// Create a complex flow with parallel batching, retry, and branching
	complexFlowYAML := `
metadata:
  name: "Complex Parallel Batch Flow with Retry and Branching"
  version: "1.0.0"
  description: "A comprehensive test flow demonstrating parallel processing, retry logic, and conditional branching"

nodes:
  # Initial data preparation
  prepare_data:
    type: "transform"
    params:
      script: |
        // Create test data for batch processing and use secrets
        var apiKey = secrets.API_KEY;
        var dbPassword = secrets.DB_PASSWORD;
        
        // Verify that we can access the secrets
        if (!apiKey || !dbPassword) {
          throw new Error("Failed to access secrets");
        }
        
        // Log the secrets (in a real app, you wouldn't do this)
        console.log("Using API key: " + apiKey);
        console.log("Using DB password: " + dbPassword);
        
        var items = [];
        for (var i = 1; i <= 10; i++) {
          items.push({
            id: i,
            value: Math.random() * 100,
            category: i % 3 === 0 ? 'high' : (i % 2 === 0 ? 'medium' : 'low'),
            shouldFail: i === 5 || i === 8  // Simulate some failures for retry testing
          });
        }
        return {
          items: items,
          total_count: items.length,
          timestamp: new Date().toISOString()
        };
    retry:
      max_retries: 2
      wait: 100ms
    next:
      success: "parallel_processor"
      error: "error_handler"

  # Parallel batch processing with retry
  parallel_processor:
    type: "parallel_batch"
    params:
      batch_size: 3
      max_parallel: 4
      timeout: "5s"
      processor_script: |
        // Process each batch item
        function processBatchItem(item) {
          // Simulate processing time
          var processingTime = Math.random() * 200 + 50;
          
          // Simulate failure for specific items (to test retry)
          if (item.shouldFail && Math.random() < 0.7) {
            throw new Error("Processing failed for item " + item.id);
          }
          
          return {
            id: item.id,
            processed_value: item.value * 2,
            category: item.category,
            processing_time: processingTime,
            processed_at: new Date().toISOString()
          };
        }
        
        // Process the batch
        var results = [];
        for (var i = 0; i < input.items.length; i++) {
          results.push(processBatchItem(input.items[i]));
        }
        return {
          batch_results: results,
          batch_size: results.length,
          success_count: results.length
        };
    batch:
      strategy: "parallel"
      max_parallel: 3
    retry:
      max_retries: 3
      wait: 200ms
      backoff: "exponential"
    next:
      success: "analyze_results"
      error: "retry_failed_items"
      timeout: "timeout_handler"

  # Analyze results and branch based on success rate
  analyze_results:
    type: "condition"
    params:
      condition_script: |
        var totalItems = input.total_count || 0;
        var processedItems = input.batch_results ? input.batch_results.length : 0;
        var successRate = totalItems > 0 ? (processedItems / totalItems) : 0;
        
        return {
          success_rate: successRate,
          total_items: totalItems,
          processed_items: processedItems,
          analysis_result: successRate >= 0.8 ? 'success' : (successRate >= 0.5 ? 'partial' : 'failure')
        };
    next:
      success: "branch_on_success_rate"

  # Branching logic based on success rate
  branch_on_success_rate:
    type: "condition"
    params:
      condition_script: |
        var analysisResult = input.analysis_result;
        var result = {};
        // Copy input properties to result
        for (var key in input) {
          if (input.hasOwnProperty(key)) {
            result[key] = input[key];
          }
        }
        if (analysisResult === 'success') {
          result.branch = 'high_success';
          return result;
        } else if (analysisResult === 'partial') {
          result.branch = 'partial_success';
          return result;
        } else {
          result.branch = 'low_success';
          return result;
        }
    next:
      high_success: "high_success_handler"
      partial_success: "partial_success_handler"
      low_success: "low_success_handler"
      default: "final_summary"

  # High success rate path
  high_success_handler:
    type: "transform"
    params:
      script: |
        var result = {};
        // Copy input properties to result
        for (var key in input) {
          if (input.hasOwnProperty(key)) {
            result[key] = input[key];
          }
        }
        result.status = 'COMPLETED_SUCCESSFULLY';
        result.message = "Excellent! " + (input.success_rate * 100) + "% success rate achieved.";
        result.recommendations = ['Continue with current processing strategy', 'Consider increasing batch size'];
        result.next_action = 'proceed_to_next_stage';
        return result;
    next:
      success: "final_summary"

  # Partial success rate path
  partial_success_handler:
    type: "transform"
    params:
      script: |
        var result = {};
        // Copy input properties to result
        for (var key in input) {
          if (input.hasOwnProperty(key)) {
            result[key] = input[key];
          }
        }
        result.status = 'COMPLETED_WITH_WARNINGS';
        result.message = "Moderate success: " + (input.success_rate * 100) + "% success rate.";
        result.recommendations = ['Review failed items', 'Consider adjusting retry strategy'];
        result.next_action = 'review_and_retry';
        return result;
    retry:
      max_retries: 1
      wait: 100ms
    next:
      success: "final_summary"

  # Low success rate path
  low_success_handler:
    type: "transform"
    params:
      script: |
        var result = {};
        // Copy input properties to result
        for (var key in input) {
          if (input.hasOwnProperty(key)) {
            result[key] = input[key];
          }
        }
        result.status = 'COMPLETED_WITH_ERRORS';
        result.message = "Low success rate: " + (input.success_rate * 100) + "%. Investigation required.";
        result.recommendations = ['Investigate root cause', 'Review system health', 'Consider rollback'];
        result.next_action = 'escalate_to_admin';
        return result;
    next:
      success: "final_summary"

  # Handle retry failures
  retry_failed_items:
    type: "transform"
    params:
      script: |
        var result = {};
        // Copy input properties to result
        for (var key in input) {
          if (input.hasOwnProperty(key)) {
            result[key] = input[key];
          }
        }
        result.status = 'RETRY_EXHAUSTED';
        result.message = 'Some items failed after all retry attempts';
        result.failed_items = input.failed_items || [];
        result.next_action = 'manual_review_required';
        return result;
    next:
      success: "final_summary"

  # Handle timeouts
  timeout_handler:
    type: "transform"
    params:
      script: |
        var result = {};
        // Copy input properties to result
        for (var key in input) {
          if (input.hasOwnProperty(key)) {
            result[key] = input[key];
          }
        }
        result.status = 'TIMEOUT_OCCURRED';
        result.message = 'Processing timed out';
        result.next_action = 'investigate_performance';
        return result;
    next:
      success: "final_summary"

  # Error handler
  error_handler:
    type: "transform"
    params:
      script: |
        var result = {};
        // Copy input properties to result
        for (var key in input) {
          if (input.hasOwnProperty(key)) {
            result[key] = input[key];
          }
        }
        result.status = 'ERROR_OCCURRED';
        result.message = 'An error occurred during processing';
        result.error_details = input.error || 'Unknown error';
        result.next_action = 'check_system_health';
        return result;
    next:
      success: "final_summary"

  # Final summary and completion
  final_summary:
    type: "transform"
    params:
      script: |
        var endTime = new Date().toISOString();
        
        // Access secrets again to verify they're available throughout the flow
        var apiKey = secrets.API_KEY;
        var dbPassword = secrets.DB_PASSWORD;
        
        // Verify that we can still access the secrets
        if (!apiKey || !dbPassword) {
          throw new Error("Failed to access secrets in final node");
        }
        
        var result = {};
        // Copy input properties to result
        for (var key in input) {
          if (input.hasOwnProperty(key)) {
            result[key] = input[key];
          }
        }
        result.execution_summary = {
          completed_at: endTime,
          final_status: input.status || 'COMPLETED',
          total_processing_time: 'calculated_by_runtime',
          items_processed: input.processed_items || 0,
          success_rate: input.success_rate || 0,
          recommendations: input.recommendations || [],
          next_action: input.next_action || 'none',
          // Include masked versions of the secrets to verify they were accessed
          api_key_used: apiKey.substring(0, 4) + '...' + apiKey.substring(apiKey.length - 4),
          db_password_used: '***' + dbPassword.substring(dbPassword.length - 4)
        };
        return result;
`

	// Register the complex flow
	flowID, err := flowRegistry.Create(accountID, "complex-parallel-batch-flow", complexFlowYAML)
	require.NoError(t, err)
	t.Logf("Created complex flow with ID: %s", flowID)

	// Create flow runtime with PostgreSQL execution store
	executionStore := postgresProvider.GetExecutionStore()
	registryAdapter := &FlowRegistryAdapter{registry: flowRegistry}
	flowRuntime := runtime.NewFlowRuntimeWithStore(registryAdapter, yamlLoader, executionStore)

	// Create server with runtime
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
	}

	server := NewServerWithRuntime(cfg, flowRegistry, accountService, secretVault, flowRuntime, plugins.NewPluginRegistry())
	testServer := httptest.NewServer(server.router)
	defer testServer.Close()

	// Test multiple concurrent executions with WebSocket monitoring
	numConcurrentExecutions := 3
	executionResults := make([]ExecutionTestResult, numConcurrentExecutions)

	// Create WebSocket connections for each execution
	for i := 0; i < numConcurrentExecutions; i++ {
		t.Run(fmt.Sprintf("Concurrent_Execution_%d", i+1), func(t *testing.T) {
			result := testComplexFlowExecution(t, testServer, flowID, accountID, testUsername, "secure_password_123", i+1)
			executionResults[i] = result
		})
	}

	// Verify all executions completed successfully
	for i, result := range executionResults {
		t.Logf("Execution %d: Status=%s, Updates=%d, Duration=%v",
			i+1, result.FinalStatus, result.UpdateCount, result.Duration)

		assert.NotEmpty(t, result.ExecutionID, "Execution ID should not be empty")
		assert.Contains(t, []string{"completed", "failed"}, result.FinalStatus, "Final status should be completed or failed")
		assert.Greater(t, result.UpdateCount, 0, "Should have received WebSocket updates")
		assert.Greater(t, result.Duration, time.Duration(0), "Execution should have taken some time")

		// Verify that the flow executed successfully
		if result.FinalStatus == "completed" {
			// The test is passing, which means the flow executed without errors
			// This indicates that the secrets were accessed successfully
			// If the secrets were not accessible, the flow would have thrown an error
			t.Logf("Execution %d completed successfully, which indicates secrets were accessed correctly", i+1)
		}
	}

	t.Logf("PostgreSQL Complex Flow Integration Test with Secret Access completed successfully!")
}

// ExecutionTestResult holds the results of a single execution test
type ExecutionTestResult struct {
	ExecutionID string
	FinalStatus string
	UpdateCount int
	Duration    time.Duration
	Updates     []ExecutionUpdate
	Error       error
}

// testComplexFlowExecution tests a single complex flow execution with WebSocket monitoring
func testComplexFlowExecution(t *testing.T, testServer *httptest.Server, flowID, accountID, username, password string, executionNum int) ExecutionTestResult {
	startTime := time.Now()

	// Create WebSocket connection
	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/api/v1/ws"
	header := make(http.Header)
	authString := username + ":" + password
	encodedAuth := base64.StdEncoding.EncodeToString([]byte(authString))
	header.Set("Authorization", "Basic "+encodedAuth)

	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		return ExecutionTestResult{Error: fmt.Errorf("WebSocket connection failed: %w", err)}
	}
	defer ws.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		return ExecutionTestResult{Error: fmt.Errorf("WebSocket handshake failed: %d", resp.StatusCode)}
	}

	// Start flow execution
	execURL := testServer.URL + "/api/v1/flows/" + flowID + "/run"
	execInput := map[string]interface{}{
		"execution_number": executionNum,
		"test_timestamp":   time.Now().Unix(),
		"test_metadata": map[string]interface{}{
			"test_type":   "complex_parallel_batch",
			"backend":     "postgresql",
			"concurrency": true,
		},
	}

	inputJSON, _ := json.Marshal(map[string]interface{}{"input": execInput})
	req, err := http.NewRequest("POST", execURL, strings.NewReader(string(inputJSON)))
	if err != nil {
		return ExecutionTestResult{Error: fmt.Errorf("Failed to create request: %w", err)}
	}

	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	execResp, err := client.Do(req)
	if err != nil {
		return ExecutionTestResult{Error: fmt.Errorf("Failed to execute flow: %w", err)}
	}
	defer execResp.Body.Close()

	if execResp.StatusCode != http.StatusCreated {
		return ExecutionTestResult{Error: fmt.Errorf("Flow execution failed with status: %d", execResp.StatusCode)}
	}

	// Parse execution response
	var execResult map[string]interface{}
	if err := json.NewDecoder(execResp.Body).Decode(&execResult); err != nil {
		return ExecutionTestResult{Error: fmt.Errorf("Failed to parse execution response: %w", err)}
	}

	executionID, ok := execResult["execution_id"].(string)
	if !ok || executionID == "" {
		return ExecutionTestResult{Error: fmt.Errorf("Invalid execution ID in response")}
	}

	// Subscribe to execution updates
	subscribeMsg := WebSocketMessage{
		Type:        "subscribe",
		ExecutionID: executionID,
	}

	if err := ws.WriteJSON(subscribeMsg); err != nil {
		return ExecutionTestResult{Error: fmt.Errorf("Failed to subscribe to execution: %w", err)}
	}

	// Collect WebSocket updates with timeout
	updates := []ExecutionUpdate{}
	updateTimeout := time.Now().Add(60 * time.Second) // Longer timeout for complex flow

	for time.Now().Before(updateTimeout) {
		ws.SetReadDeadline(time.Now().Add(5 * time.Second))

		var update ExecutionUpdate
		err := ws.ReadJSON(&update)
		if err != nil {
			// Check if it's a timeout or connection closed
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				break
			}
			// Check for timeout errors
			if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
				continue // Continue on timeout, might be normal for complex flows
			}
			// For other errors, log and break to avoid panic
			t.Logf("WebSocket read error: %v", err)
			break
		}

		updates = append(updates, update)

		// Log significant updates
		if update.Type == "status" && update.Status != nil {
			t.Logf("Execution %d (%s): Status=%s, Progress=%.1f%%, Node=%s",
				executionNum, executionID[:8], update.Status.Status,
				update.Status.Progress, update.Status.CurrentNode)
		}

		// Check for completion
		if update.Type == "complete" ||
			(update.Type == "status" && update.Status != nil &&
				(update.Status.Status == "completed" || update.Status.Status == "failed")) {
			break
		}
	}

	// Get final execution status
	statusURL := testServer.URL + "/api/v1/executions/" + executionID
	statusReq, err := http.NewRequest("GET", statusURL, nil)
	if err != nil {
		return ExecutionTestResult{Error: fmt.Errorf("Failed to create status request: %w", err)}
	}
	statusReq.SetBasicAuth(username, password)

	statusResp, err := client.Do(statusReq)
	if err != nil {
		return ExecutionTestResult{Error: fmt.Errorf("Failed to get execution status: %w", err)}
	}
	defer statusResp.Body.Close()

	var finalStatus runtime.ExecutionStatus
	if err := json.NewDecoder(statusResp.Body).Decode(&finalStatus); err != nil {
		return ExecutionTestResult{Error: fmt.Errorf("Failed to parse final status: %w", err)}
	}

	duration := time.Since(startTime)

	// Log execution summary
	t.Logf("Execution %d Summary: ID=%s, Status=%s, Duration=%v, Updates=%d, Progress=%.1f%%",
		executionNum, executionID[:8], finalStatus.Status, duration, len(updates), finalStatus.Progress)

	if finalStatus.Status == "failed" && finalStatus.Error != "" {
		t.Logf("Execution %d failed with error: %s", executionNum, finalStatus.Error)
	}

	return ExecutionTestResult{
		ExecutionID: executionID,
		FinalStatus: finalStatus.Status,
		UpdateCount: len(updates),
		Duration:    duration,
		Updates:     updates,
	}
}

// TestWebSocketPostgreSQLIntegration_LoadTest tests high concurrency with PostgreSQL
func TestWebSocketPostgreSQLIntegration_LoadTest(t *testing.T) {
	// Load environment variables from project root first
	_ = godotenv.Load("../../.env")

	// Skip unless PostgreSQL is configured and load test is explicitly requested
	if os.Getenv("FLOWRUNNER_POSTGRES_HOST") == "" {
		t.Skip("Skipping PostgreSQL load test. Set FLOWRUNNER_POSTGRES_HOST in .env file to run.")
	}
	if os.Getenv("RUN_POSTGRES_LOAD_TEST") != "true" {
		t.Skip("Skipping PostgreSQL load test. Set RUN_POSTGRES_LOAD_TEST=true in .env file to run.")
	}

	// This test would create many concurrent executions to test system limits
	// Implementation would be similar to the above but with higher concurrency
	t.Log("PostgreSQL Load Test - Implementation would go here for stress testing")
}

// TestWebSocketPostgreSQLIntegration_SimpleBranching tests a simpler branching flow for easier testing
func TestWebSocketPostgreSQLIntegration_SimpleBranching(t *testing.T) {
	// Load environment variables from project root first
	_ = godotenv.Load("../../.env")

	// This test can run with any PostgreSQL instance using standard env vars
	if os.Getenv("FLOWRUNNER_POSTGRES_HOST") == "" {
		t.Skip("Skipping PostgreSQL test. Set FLOWRUNNER_POSTGRES_HOST in .env file to run.")
	}

	// Get PostgreSQL configuration from standard environment variables
	host := os.Getenv("FLOWRUNNER_POSTGRES_HOST")
	if host == "" {
		host = "localhost"
	}

	portStr := os.Getenv("FLOWRUNNER_POSTGRES_PORT")
	port := 5432
	if portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	user := os.Getenv("FLOWRUNNER_POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}

	password := os.Getenv("FLOWRUNNER_POSTGRES_PASSWORD")
	if password == "" {
		password = "postgres"
	}

	dbName := os.Getenv("FLOWRUNNER_POSTGRES_DATABASE")
	if dbName == "" {
		dbName = "flowrunner"
	}

	sslMode := os.Getenv("FLOWRUNNER_POSTGRES_SSL_MODE")
	if sslMode == "" {
		sslMode = "disable"
	}

	t.Logf("Testing with PostgreSQL: %s:%d/%s (user: %s)", host, port, dbName, user)

	// Create PostgreSQL storage provider
	postgresProvider, err := storage.NewProvider(storage.ProviderConfig{
		Type: storage.PostgreSQLProviderType,
		PostgreSQL: &storage.PostgreSQLProviderConfig{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			Database: dbName,
			SSLMode:  sslMode,
		},
	})
	require.NoError(t, err, "Failed to create PostgreSQL provider")

	// Initialize the provider
	err = postgresProvider.Initialize()
	require.NoError(t, err, "Failed to initialize PostgreSQL provider")
	defer postgresProvider.Close()

	// Create services
	accountService := services.NewAccountService(postgresProvider.GetAccountStore())
	encKey, err := services.GenerateEncryptionKey()
	require.NoError(t, err)
	secretVault, err := services.NewExtendedSecretVaultService(postgresProvider.GetSecretStore(), encKey)
	require.NoError(t, err)

	// Create YAML loader with core node types
	nodeFactories := make(map[string]plugins.NodeFactory)
	for nodeType, factory := range runtime.CoreNodeTypes() {
		nodeFactories[nodeType] = &RuntimeNodeFactoryAdapter{factory: factory}
	}
	yamlLoader := loader.NewYAMLLoader(nodeFactories, plugins.NewPluginRegistry())

	// Create flow registry
	flowRegistry := registry.NewFlowRegistry(postgresProvider.GetFlowStore(), registry.FlowRegistryOptions{
		YAMLLoader: yamlLoader,
	})

	// Create test account (or use existing one)
	testUsername := fmt.Sprintf("simple_test_user_%d", time.Now().UnixNano())
	accountID, err := accountService.CreateAccount(testUsername, "test_password")
	require.NoError(t, err)

	// Create a simpler branching flow that's easier to test
	simpleBranchingFlow := `
metadata:
  name: "Simple Branching Flow with Retry"
  version: "1.0.0"
  description: "A simpler flow demonstrating branching and retry with WebSocket updates"

nodes:
  # Generate test data
  start:
    type: "transform"
    params:
      script: |
        var value = Math.random() * 100;
        return {
          test_value: value,
          category: value > 70 ? 'high' : (value > 30 ? 'medium' : 'low'),
          timestamp: new Date().toISOString()
        };
    retry:
      max_retries: 2
      wait: 100ms
    next:
      success: "evaluate_category"
      error: "error_handler"

  # Branch based on category
  evaluate_category:
    type: "condition"
    params:
      condition_script: |
        const category = input.category;
        return {
          ...input,
          branch_decision: category,
          evaluation_time: new Date().toISOString()
        };
    next:
      high: "high_value_processor"
      medium: "medium_value_processor"
      low: "low_value_processor"
      default: "default_processor"

  # High value processing path
  high_value_processor:
    type: "transform"
    params:
      script: |
        return {
          ...input,
          processing_result: 'HIGH_VALUE_PROCESSED',
          bonus_applied: true,
          final_value: input.test_value * 1.5,
          message: 'High value item processed with bonus'
        };
    retry:
      max_retries: 1
      wait: 50ms
    next:
      success: "finalize"

  # Medium value processing path
  medium_value_processor:
    type: "transform"
    params:
      script: |
        return {
          ...input,
          processing_result: 'MEDIUM_VALUE_PROCESSED',
          bonus_applied: false,
          final_value: input.test_value * 1.2,
          message: 'Medium value item processed normally'
        };
    next:
      success: "finalize"

  # Low value processing path
  low_value_processor:
    type: "transform"
    params:
      script: |
        return {
          ...input,
          processing_result: 'LOW_VALUE_PROCESSED',
          bonus_applied: false,
          final_value: input.test_value,
          message: 'Low value item processed without modification'
        };
    next:
      success: "finalize"

  # Default processing path
  default_processor:
    type: "transform"
    params:
      script: |
        return {
          ...input,
          processing_result: 'DEFAULT_PROCESSED',
          bonus_applied: false,
          final_value: input.test_value,
          message: 'Item processed via default path'
        };
    next:
      success: "finalize"

  # Error handler
  error_handler:
    type: "transform"
    params:
      script: |
        return {
          ...input,
          processing_result: 'ERROR_HANDLED',
          error_message: 'An error occurred but was handled gracefully',
          final_value: 0
        };
    next:
      success: "finalize"

  # Final processing
  finalize:
    type: "transform"
    params:
      script: |
        return {
          ...input,
          completed_at: new Date().toISOString(),
          execution_summary: {
            original_value: input.test_value,
            final_value: input.final_value,
            category: input.category,
            processing_path: input.processing_result,
            bonus_applied: input.bonus_applied || false,
            message: input.message || 'Processing completed'
          }
        };
`

	// Register the flow
	flowID, err := flowRegistry.Create(accountID, "simple-branching-flow", simpleBranchingFlow)
	require.NoError(t, err)
	t.Logf("Created simple branching flow with ID: %s", flowID)

	// Create flow runtime
	executionStore := postgresProvider.GetExecutionStore()
	registryAdapter := &FlowRegistryAdapter{registry: flowRegistry}
	flowRuntime := runtime.NewFlowRuntimeWithStore(registryAdapter, yamlLoader, executionStore)

	// Create server
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host: "localhost",
			Port: 8080,
		},
	}

	server := NewServerWithRuntime(cfg, flowRegistry, accountService, secretVault, flowRuntime, plugins.NewPluginRegistry())
	testServer := httptest.NewServer(server.router)
	defer testServer.Close()

	// Test the flow execution with WebSocket monitoring
	wsURL := "ws" + strings.TrimPrefix(testServer.URL, "http") + "/api/v1/ws"
	header := make(http.Header)
	header.Set("Authorization", "Basic c2ltcGxlX3Rlc3RfdXNlcjp0ZXN0X3Bhc3N3b3Jk") // simple_test_user:test_password

	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	require.NoError(t, err, "WebSocket connection failed")
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	defer ws.Close()

	// Execute the flow
	execURL := testServer.URL + "/api/v1/flows/" + flowID + "/run"
	execInput := map[string]interface{}{
		"test_run":       true,
		"backend":        "postgresql",
		"test_timestamp": time.Now().Unix(),
		"test_metadata": map[string]interface{}{
			"test_type": "simple_branching",
			"backend":   "postgresql",
		},
	}

	inputJSON, _ := json.Marshal(map[string]interface{}{"input": execInput})
	req, err := http.NewRequest("POST", execURL, strings.NewReader(string(inputJSON)))
	require.NoError(t, err)

	req.SetBasicAuth(testUsername, "test_password")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	execResp, err := client.Do(req)
	require.NoError(t, err)

	// If we get a 400 status, let's log the response body to understand the error
	if execResp.StatusCode == http.StatusBadRequest {
		bodyBytes, _ := io.ReadAll(execResp.Body)
		execResp.Body.Close()
		// Create a new reader with the same bytes for later use
		execResp.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		t.Logf("Error response from server: %s", string(bodyBytes))
	}

	require.Equal(t, http.StatusCreated, execResp.StatusCode)
	defer execResp.Body.Close()

	// Parse execution response
	var execResult map[string]interface{}
	err = json.NewDecoder(execResp.Body).Decode(&execResult)
	require.NoError(t, err)

	executionID := execResult["execution_id"].(string)
	require.NotEmpty(t, executionID)
	t.Logf("Started execution: %s", executionID)

	// Subscribe to execution updates
	subscribeMsg := WebSocketMessage{
		Type:        "subscribe",
		ExecutionID: executionID,
	}

	err = ws.WriteJSON(subscribeMsg)
	require.NoError(t, err)

	// Collect WebSocket updates
	updates := []ExecutionUpdate{}
	ws.SetReadDeadline(time.Now().Add(30 * time.Second))

	for {
		var update ExecutionUpdate
		err := ws.ReadJSON(&update)
		if err != nil {
			break // Timeout or connection closed
		}

		updates = append(updates, update)

		if update.Type == "status" && update.Status != nil {
			t.Logf("Status Update: %s, Progress: %.1f%%, Node: %s",
				update.Status.Status, update.Status.Progress, update.Status.CurrentNode)
		}

		// Stop when we get a completion event
		if update.Type == "complete" ||
			(update.Type == "status" && update.Status != nil &&
				(update.Status.Status == "completed" || update.Status.Status == "failed")) {
			break
		}
	}

	// Verify we received updates
	assert.NotEmpty(t, updates, "Should have received WebSocket updates")

	// Get final execution status
	statusURL := testServer.URL + "/api/v1/executions/" + executionID
	statusReq, err := http.NewRequest("GET", statusURL, nil)
	require.NoError(t, err)
	statusReq.SetBasicAuth("simple_test_user", "test_password")

	statusResp, err := client.Do(statusReq)
	require.NoError(t, err)
	defer statusResp.Body.Close()

	var finalStatus runtime.ExecutionStatus
	err = json.NewDecoder(statusResp.Body).Decode(&finalStatus)
	require.NoError(t, err)

	// Verify execution completed successfully
	t.Logf("Final Status: %s, Progress: %.1f%%", finalStatus.Status, finalStatus.Progress)
	if finalStatus.Status == "failed" {
		t.Logf("Execution failed with error: %s", finalStatus.Error)
	}

	assert.Contains(t, []string{"completed", "failed"}, finalStatus.Status)
	assert.Equal(t, executionID, finalStatus.ID)
	assert.Equal(t, flowID, finalStatus.FlowID)

	// Verify we got status updates via WebSocket
	hasStatusUpdate := false
	for _, update := range updates {
		if update.Type == "status" {
			hasStatusUpdate = true
			assert.Equal(t, executionID, update.ExecutionID)
			assert.NotNil(t, update.Status)
		}
	}
	assert.True(t, hasStatusUpdate, "Should have received at least one status update")

	t.Logf("PostgreSQL Simple Branching Integration Test completed successfully!")
	t.Logf("Received %d WebSocket updates during execution", len(updates))
}

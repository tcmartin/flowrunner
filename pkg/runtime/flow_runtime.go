package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tcmartin/flowrunner/pkg/auth"
	"github.com/tcmartin/flowrunner/pkg/loader"
)

// ExecutionStore interface - we'll use this instead of importing storage to avoid cycles
type ExecutionStore interface {
	SaveExecution(execution ExecutionStatus) error
	GetExecution(executionID string) (ExecutionStatus, error)
	ListExecutions(accountID string) ([]ExecutionStatus, error)
	SaveExecutionLog(executionID string, log ExecutionLog) error
	GetExecutionLogs(executionID string) ([]ExecutionLog, error)
}

// flowRuntime is the implementation of the FlowRuntime interface
type flowRuntime struct {
	registry       FlowRegistry
	yamlLoader     loader.YAMLLoader
	executionStore ExecutionStore
	secretVault    auth.SecretVault // Add secret vault support

	// In-memory tracking for active executions
	activeExecutions map[string]*executionContext
    // Cache of completed executions when no external store is configured
    completedExecutions map[string]ExecutionStatus
	mu               sync.RWMutex
}

// executionContext tracks the context of a running execution
type executionContext struct {
	accountID   string
	flowID      string
	status      ExecutionStatus
	cancel      context.CancelFunc
	logChannel  chan ExecutionLog
	subscribers []chan ExecutionLog
	mu          sync.RWMutex
}

// NewFlowRuntime creates a new FlowRuntime
func NewFlowRuntime(registry FlowRegistry, yamlLoader loader.YAMLLoader) FlowRuntime {
	return &flowRuntime{
		registry:         registry,
		yamlLoader:       yamlLoader,
        activeExecutions: make(map[string]*executionContext),
        completedExecutions: make(map[string]ExecutionStatus),
	}
}

// NewFlowRuntimeWithStore creates a new FlowRuntime with execution store
func NewFlowRuntimeWithStore(registry FlowRegistry, yamlLoader loader.YAMLLoader, executionStore ExecutionStore) FlowRuntime {
	return &flowRuntime{
		registry:         registry,
		yamlLoader:       yamlLoader,
		executionStore:   executionStore,
        activeExecutions: make(map[string]*executionContext),
        completedExecutions: make(map[string]ExecutionStatus),
	}
}

// NewFlowRuntimeWithSecrets creates a new FlowRuntime with secret vault support
func NewFlowRuntimeWithSecrets(registry FlowRegistry, yamlLoader loader.YAMLLoader, secretVault auth.SecretVault) FlowRuntime {
	return &flowRuntime{
		registry:         registry,
		yamlLoader:       yamlLoader,
		secretVault:      secretVault,
        activeExecutions: make(map[string]*executionContext),
        completedExecutions: make(map[string]ExecutionStatus),
	}
}

// NewFlowRuntimeWithStoreAndSecrets creates a new FlowRuntime with execution store and secret vault
func NewFlowRuntimeWithStoreAndSecrets(registry FlowRegistry, yamlLoader loader.YAMLLoader, executionStore ExecutionStore, secretVault auth.SecretVault) FlowRuntime {
	return &flowRuntime{
		registry:         registry,
		yamlLoader:       yamlLoader,
		executionStore:   executionStore,
		secretVault:      secretVault,
        activeExecutions: make(map[string]*executionContext),
        completedExecutions: make(map[string]ExecutionStatus),
	}
}

func (r *flowRuntime) Execute(accountID string, flowID string, input map[string]interface{}) (string, error) {
	flowDef, err := r.registry.GetFlow(accountID, flowID)
	if err != nil {
		return "", fmt.Errorf("failed to get flow: %w", err)
	}

	flow, err := r.yamlLoader.Parse(flowDef.YAML)
	if err != nil {
		return "", fmt.Errorf("failed to parse flow YAML: %w", err)
	}

	executionID := uuid.New().String()

	// Create execution context
	ctx, cancel := context.WithCancel(context.Background())
	execCtx := &executionContext{
		accountID:   accountID,
		flowID:      flowID,
		cancel:      cancel,
		logChannel:  make(chan ExecutionLog, 100),
		subscribers: make([]chan ExecutionLog, 0),
		status: ExecutionStatus{
			ID:        executionID,
			FlowID:    flowID,
			Status:    "running",
			StartTime: time.Now(),
			Progress:  0.0,
			Results:   make(map[string]interface{}),
		},
	}

	// Store in active executions
	r.mu.Lock()
	r.activeExecutions[executionID] = execCtx
	r.mu.Unlock()

	// Save initial execution status to store if available
	if r.executionStore != nil {
		if err := r.executionStore.SaveExecution(execCtx.status); err != nil {
			r.logExecution(executionID, "error", "Failed to save execution status", map[string]interface{}{"error": err.Error()})
		}

		// If the execution store supports setting account ID (PostgreSQL, DynamoDB, etc.), set it
		if store, ok := r.executionStore.(interface{ SetExecutionAccountID(string, string) error }); ok {
			if err := store.SetExecutionAccountID(executionID, accountID); err != nil {
				r.logExecution(executionID, "error", "Failed to set execution account ID", map[string]interface{}{"error": err.Error()})
			}
		}
	}

	// Start execution in goroutine
	go r.executeFlow(ctx, execCtx, flow, input)

	return executionID, nil
}

func (r *flowRuntime) executeFlow(ctx context.Context, execCtx *executionContext, flow interface{}, input map[string]interface{}) {
	defer func() {
		if rec := recover(); rec != nil {
			r.logExecution(execCtx.status.ID, "error", "Flow execution panicked", map[string]interface{}{"panic": rec})
			r.updateExecutionStatus(execCtx.status.ID, "failed", fmt.Sprintf("Flow execution panicked: %v", rec), nil)
		}

		// Close log channel when execution is done
		close(execCtx.logChannel)

		// Remove from active executions
		r.mu.Lock()
        // Cache the final status in-memory if we don't have an external store
        final := execCtx.status
        delete(r.activeExecutions, execCtx.status.ID)
        if r.executionStore == nil {
            r.completedExecutions[execCtx.status.ID] = final
        }
		r.mu.Unlock()
	}()

	r.logExecution(execCtx.status.ID, "info", "Starting flow execution", map[string]interface{}{"flowID": execCtx.flowID, "accountID": execCtx.accountID})

	// Create FlowContext for proper expression evaluation
	var flowContext *FlowContext
	if r.secretVault != nil {
		flowContext = NewFlowContext(execCtx.status.ID, execCtx.flowID, execCtx.accountID, r.secretVault)
	}

	// Inject execution context into the input for nodes to use
	enhancedInput := make(map[string]interface{})
	for k, v := range input {
		enhancedInput[k] = v
		// Also set in flow context if available
		if flowContext != nil {
			flowContext.SetSharedData(k, v)
		}
	}
	
	// Add execution context for logging and debugging
	enhancedInput["_execution"] = map[string]interface{}{
		"execution_id": execCtx.status.ID,
		"flow_id":      execCtx.flowID,
		"account_id":   execCtx.accountID,
		"logger":       r.logExecution,
	}

	// Add flow context if available for expression evaluation
	if flowContext != nil {
		enhancedInput["_flow_context"] = map[string]interface{}{
			"node_results": flowContext.nodeResults,
			"shared_data":  flowContext.sharedData,
		}
		enhancedInput["accountID"] = execCtx.accountID
		enhancedInput["_secret_vault"] = r.secretVault  // Add secret vault for NodeWrapper access
	}

	// Execute the flow
	var result interface{}
	var err error

	// Check if flow supports context-aware execution
	if flowWithCtx, ok := flow.(interface {
		RunWithContext(ctx context.Context, shared interface{}) (interface{}, error)
	}); ok {
		result, err = flowWithCtx.RunWithContext(ctx, enhancedInput)
	} else if flowRunner, ok := flow.(interface {
		Run(shared interface{}) (interface{}, error)
	}); ok {
		result, err = flowRunner.Run(enhancedInput)
    } else if flowlibFlow, ok := flow.(interface {
        Run(shared any) (string, error)
    }); ok {
        // Handle flowlib.Flow which returns (Action, error)
        var action string
        action, err = flowlibFlow.Run(enhancedInput)
        if err == nil {
            // Try to extract the final node result from the shared context populated by NodeWrapper
            if finalShared, ok := enhancedInput["result"]; ok && finalShared != nil {
                // Provide both action and final result for completeness
                result = finalShared
            } else {
                result = map[string]interface{}{"action": action}
            }
        }
    } else {
		err = fmt.Errorf("flow does not implement expected execution interface")
	}

	if err != nil {
		r.logExecution(execCtx.status.ID, "error", "Flow execution failed", map[string]interface{}{"error": err.Error()})
		r.updateExecutionStatus(execCtx.status.ID, "failed", err.Error(), nil)
		return
	}

	// Convert result to map if possible
	var resultMap map[string]interface{}
	if result != nil {
		if rm, ok := result.(map[string]interface{}); ok {
			resultMap = rm
		} else {
			resultMap = map[string]interface{}{"result": result}
		}
	}

	r.logExecution(execCtx.status.ID, "info", "Flow execution completed successfully", map[string]interface{}{"result": result})
	r.updateExecutionStatus(execCtx.status.ID, "completed", "", resultMap)
}

func (r *flowRuntime) GetStatus(executionID string) (ExecutionStatus, error) {
	// First check active executions
	r.mu.RLock()
	if execCtx, ok := r.activeExecutions[executionID]; ok {
		execCtx.mu.RLock()
		status := execCtx.status
		execCtx.mu.RUnlock()
		r.mu.RUnlock()
		return status, nil
	}
	r.mu.RUnlock()

	// If not active, check the execution store
	if r.executionStore != nil {
		return r.executionStore.GetExecution(executionID)
	}

    // Fall back to in-memory cache of completed executions
    r.mu.RLock()
    if status, ok := r.completedExecutions[executionID]; ok {
        r.mu.RUnlock()
        return status, nil
    }
    r.mu.RUnlock()

	return ExecutionStatus{}, fmt.Errorf("execution not found: %s", executionID)
}

func (r *flowRuntime) GetLogs(executionID string) ([]ExecutionLog, error) {
	// If execution store is available, get logs from there
	if r.executionStore != nil {
		return r.executionStore.GetExecutionLogs(executionID)
	}

	return []ExecutionLog{}, nil
}

func (r *flowRuntime) SubscribeToLogs(executionID string) (<-chan ExecutionLog, error) {
	r.mu.RLock()
	execCtx, ok := r.activeExecutions[executionID]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("execution not found or not active: %s", executionID)
	}

	// Create a subscriber channel
	subscriber := make(chan ExecutionLog, 100)

	execCtx.mu.Lock()
	execCtx.subscribers = append(execCtx.subscribers, subscriber)
	execCtx.mu.Unlock()

	// Start a goroutine to forward logs to the subscriber
	go func() {
		defer close(subscriber)
		for log := range execCtx.logChannel {
			select {
			case subscriber <- log:
			default:
				// Subscriber channel is full, skip this log
			}
		}
	}()

	return subscriber, nil
}

func (r *flowRuntime) Cancel(executionID string) error {
	r.mu.RLock()
	execCtx, ok := r.activeExecutions[executionID]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("execution not found or not active: %s", executionID)
	}

	// Cancel the execution context
	execCtx.cancel()

	// Update status
	r.updateExecutionStatus(executionID, "canceled", "Execution was canceled by user", nil)

	r.logExecution(executionID, "info", "Execution canceled by user", nil)

	return nil
}

// Helper methods

func (r *flowRuntime) logExecution(executionID, level, message string, data map[string]interface{}) {
	log := ExecutionLog{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
        Data:      data,
	}

    // If caller passed node_id in data, propagate to structured log NodeID
    if data != nil {
        if nodeID, ok := data["node_id"].(string); ok {
            log.NodeID = nodeID
        }
    }

	// Save to execution store if available
	if r.executionStore != nil {
		if err := r.executionStore.SaveExecutionLog(executionID, log); err != nil {
			// Log error, but don't fail the execution
			fmt.Printf("Failed to save execution log: %v\n", err)
		}
	}

	// Send to active subscribers
	r.mu.RLock()
	if execCtx, ok := r.activeExecutions[executionID]; ok {
		execCtx.mu.RLock()
		for _, subscriber := range execCtx.subscribers {
			select {
			case subscriber <- log:
			default:
				// Subscriber channel is full, skip this log
			}
		}
		execCtx.mu.RUnlock()
	}
	r.mu.RUnlock()
}

func (r *flowRuntime) updateExecutionStatus(executionID, status, errorMsg string, results map[string]interface{}) {
	r.mu.RLock()
	execCtx, ok := r.activeExecutions[executionID]
	r.mu.RUnlock()

	if !ok {
		return
	}

	execCtx.mu.Lock()
	execCtx.status.Status = status
	if errorMsg != "" {
		execCtx.status.Error = errorMsg
	}
	if results != nil {
		execCtx.status.Results = results
	}
	if status == "completed" || status == "failed" || status == "canceled" {
		execCtx.status.EndTime = time.Now()
		execCtx.status.Progress = 100.0
	}
	status_copy := execCtx.status
	execCtx.mu.Unlock()

	// Save to execution store if available
	if r.executionStore != nil {
		if err := r.executionStore.SaveExecution(status_copy); err != nil {
			// Log error, but don't fail the execution
			fmt.Printf("Failed to save execution status: %v\n", err)
		}
	}
}

func (r *flowRuntime) ListExecutions(accountID string) ([]ExecutionStatus, error) {
	var executions []ExecutionStatus

	// Get active executions
	r.mu.RLock()
	for _, execCtx := range r.activeExecutions {
		if execCtx.accountID == accountID {
			execCtx.mu.RLock()
			executions = append(executions, execCtx.status)
			execCtx.mu.RUnlock()
		}
	}
	r.mu.RUnlock()

	// If execution store is available, get completed executions
	if r.executionStore != nil {
		storedExecutions, err := r.executionStore.ListExecutions(accountID)
		if err != nil {
			return executions, err // Return active executions even if store fails
		}

		// Create a map of active execution IDs to avoid duplicates
		activeIDs := make(map[string]bool)
		for _, exec := range executions {
			activeIDs[exec.ID] = true
		}

		// Add stored executions that are not active
		for _, storedExec := range storedExecutions {
			if !activeIDs[storedExec.ID] {
				executions = append(executions, storedExec)
			}
		}
	}

	return executions, nil
}

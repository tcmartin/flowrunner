package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tcmartin/flowrunner/pkg/auth"
	"github.com/tcmartin/flowrunner/pkg/runtime"
)

func TestMemoryProvider(t *testing.T) {
	provider := NewMemoryProvider()
	err := provider.Initialize()
	assert.NoError(t, err)

	// Test getting stores
	assert.NotNil(t, provider.GetFlowStore())
	assert.NotNil(t, provider.GetSecretStore())
	assert.NotNil(t, provider.GetExecutionStore())
	assert.NotNil(t, provider.GetAccountStore())

	// Test closing provider
	err = provider.Close()
	assert.NoError(t, err)
}

func TestMemoryFlowStore(t *testing.T) {
	store := NewMemoryFlowStore()

	// Test saving and retrieving a flow
	accountID := "test-account"
	flowID := "test-flow"
	flowDef := []byte(`{"metadata":{"name":"Test Flow","description":"A test flow","version":"1.0.0"}}`)

	err := store.SaveFlow(accountID, flowID, flowDef)
	assert.NoError(t, err)

	retrievedDef, err := store.GetFlow(accountID, flowID)
	assert.NoError(t, err)
	assert.Equal(t, flowDef, retrievedDef)

	// Test listing flows
	flowIDs, err := store.ListFlows(accountID)
	assert.NoError(t, err)
	assert.Len(t, flowIDs, 1)
	assert.Equal(t, flowID, flowIDs[0])

	// Test getting flow metadata
	metadata, err := store.GetFlowMetadata(accountID, flowID)
	assert.NoError(t, err)
	assert.Equal(t, flowID, metadata.ID)
	assert.Equal(t, accountID, metadata.AccountID)

	// Test listing flows with metadata
	metadataList, err := store.ListFlowsWithMetadata(accountID)
	assert.NoError(t, err)
	assert.Len(t, metadataList, 1)
	assert.Equal(t, flowID, metadataList[0].ID)

	// Test deleting a flow
	err = store.DeleteFlow(accountID, flowID)
	assert.NoError(t, err)

	// Verify flow is deleted
	_, err = store.GetFlow(accountID, flowID)
	assert.Error(t, err)
	assert.Equal(t, ErrFlowNotFound, err)

	// Test error cases
	_, err = store.GetFlow("non-existent", "non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrFlowNotFound, err)

	err = store.DeleteFlow("non-existent", "non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrFlowNotFound, err)
}

func TestMemorySecretStore(t *testing.T) {
	store := NewMemorySecretStore()

	// Test saving and retrieving a secret
	accountID := "test-account"
	key := "test-key"
	secret := auth.Secret{
		AccountID: accountID,
		Key:       key,
		Value:     "test-value",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	err := store.SaveSecret(secret)
	assert.NoError(t, err)

	retrievedSecret, err := store.GetSecret(accountID, key)
	assert.NoError(t, err)
	assert.Equal(t, secret.Value, retrievedSecret.Value)

	// Test listing secrets
	secrets, err := store.ListSecrets(accountID)
	assert.NoError(t, err)
	assert.Len(t, secrets, 1)
	assert.Equal(t, key, secrets[0].Key)

	// Test deleting a secret
	err = store.DeleteSecret(accountID, key)
	assert.NoError(t, err)

	// Verify secret is deleted
	_, err = store.GetSecret(accountID, key)
	assert.Error(t, err)
	assert.Equal(t, ErrSecretNotFound, err)

	// Test error cases
	_, err = store.GetSecret("non-existent", "non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrSecretNotFound, err)

	err = store.DeleteSecret("non-existent", "non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrSecretNotFound, err)
}

func TestMemoryExecutionStore(t *testing.T) {
	store := NewMemoryExecutionStore()

	// Test saving and retrieving an execution
	accountID := "test-account"
	executionID := "test-execution"
	execution := runtime.ExecutionStatus{
		ID:        executionID,
		FlowID:    "test-flow",
		Status:    "running",
		StartTime: time.Now(),
		EndTime:   time.Time{},
	}

	// Create a wrapper to set the account ID
	wrapper := ExecutionWrapper{
		ExecutionStatus: execution,
		AccountID:       accountID,
	}

	// First, we need to store the execution with the account ID
	store.executions[executionID] = wrapper

	// Now test the SaveExecution method - this should preserve the account ID
	err := store.SaveExecution(execution)
	assert.NoError(t, err)

	// Verify the account ID is preserved
	assert.Equal(t, accountID, store.executions[executionID].AccountID)

	retrievedExecution, err := store.GetExecution(executionID)
	assert.NoError(t, err)
	assert.Equal(t, execution.ID, retrievedExecution.ID)
	assert.Equal(t, execution.Status, retrievedExecution.Status)

	// Test listing executions
	executions, err := store.ListExecutions(accountID)
	assert.NoError(t, err)
	assert.Len(t, executions, 1)
	assert.Equal(t, executionID, executions[0].ID)

	// Test saving and retrieving logs
	log := runtime.ExecutionLog{
		Timestamp: time.Now(),
		NodeID:    "test-node",
		Level:     "info",
		Message:   "test-message",
		Data:      map[string]interface{}{"key": "value"},
	}

	err = store.SaveExecutionLog(executionID, log)
	assert.NoError(t, err)

	logs, err := store.GetExecutionLogs(executionID)
	assert.NoError(t, err)
	assert.Len(t, logs, 1)
	assert.Equal(t, log.NodeID, logs[0].NodeID)
	assert.Equal(t, log.Level, logs[0].Level)
	assert.Equal(t, log.Message, logs[0].Message)
	assert.Equal(t, "value", logs[0].Data["key"])

	// Test error cases
	_, err = store.GetExecution("non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrExecutionNotFound, err)
}

func TestMemoryAccountStore(t *testing.T) {
	store := NewMemoryAccountStore()

	// Test saving and retrieving an account
	accountID := "test-account"
	username := "test-user"
	token := "test-token"
	account := auth.Account{
		ID:           accountID,
		Username:     username,
		PasswordHash: "hash",
		APIToken:     token,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err := store.SaveAccount(account)
	assert.NoError(t, err)

	retrievedAccount, err := store.GetAccount(accountID)
	assert.NoError(t, err)
	assert.Equal(t, account.Username, retrievedAccount.Username)
	assert.Equal(t, account.PasswordHash, retrievedAccount.PasswordHash)

	// Test retrieving by username
	retrievedByUsername, err := store.GetAccountByUsername(username)
	assert.NoError(t, err)
	assert.Equal(t, accountID, retrievedByUsername.ID)

	// Test retrieving by token
	retrievedByToken, err := store.GetAccountByToken(token)
	assert.NoError(t, err)
	assert.Equal(t, accountID, retrievedByToken.ID)

	// Test listing accounts
	accounts, err := store.ListAccounts()
	assert.NoError(t, err)
	assert.Len(t, accounts, 1)
	assert.Equal(t, accountID, accounts[0].ID)

	// Test deleting an account
	err = store.DeleteAccount(accountID)
	assert.NoError(t, err)

	// Verify account is deleted
	_, err = store.GetAccount(accountID)
	assert.Error(t, err)
	assert.Equal(t, ErrAccountNotFound, err)

	// Test error cases
	_, err = store.GetAccount("non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrAccountNotFound, err)

	_, err = store.GetAccountByUsername("non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrAccountNotFound, err)

	_, err = store.GetAccountByToken("non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrAccountNotFound, err)

	err = store.DeleteAccount("non-existent")
	assert.Error(t, err)
	assert.Equal(t, ErrAccountNotFound, err)
}

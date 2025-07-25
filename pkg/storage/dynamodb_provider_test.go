package storage

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
)

func init() {
	// Load .env file from project root
	_ = godotenv.Load("../../.env")
}

// TestDynamoDBProvider tests the DynamoDB provider
func TestDynamoDBProvider(t *testing.T) {
	// Get test client (mock by default, real with -real-dynamodb flag)
	client, err := GetTestDynamoDBClient()
	if err != nil {
		t.Fatalf("Failed to get test DynamoDB client: %v", err)
	}

	// Create provider using the client
	provider := NewDynamoDBProviderWithClient(client, "test_")

	// Initialize provider
	err = provider.Initialize()
	assert.NoError(t, err)

	// Test getting stores
	assert.NotNil(t, provider.GetFlowStore())
	assert.NotNil(t, provider.GetSecretStore())
	assert.NotNil(t, provider.GetExecutionStore())
	assert.NotNil(t, provider.GetAccountStore())

	// Clean up tables
	cleanupTables(t, provider)
}

// cleanupTables deletes the test tables (only for real DynamoDB)
func cleanupTables(t *testing.T, provider *DynamoDBProvider) {
	// Only cleanup if using real DynamoDB
	if !*useRealDynamoDB {
		return
	}

	// Cast to real DynamoDB client for cleanup
	realClient, ok := provider.client.(*dynamodb.DynamoDB)
	if !ok {
		return // Not a real client, skip cleanup
	}

	tables := []string{
		provider.flowStore.tableName,
		provider.secretStore.tableName,
		provider.executionStore.execTableName,
		provider.executionStore.logsTableName,
		provider.accountStore.tableName,
	}

	for _, table := range tables {
		_, err := realClient.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(table),
		})
		if err != nil {
			t.Logf("Failed to delete table %s: %v", table, err)
		} else {
			// Wait for table to be deleted
			werr := realClient.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{
				TableName: aws.String(table),
			})
			if werr != nil {
				t.Logf("Failed to wait for table %s to be deleted: %v", table, werr)
			}
		}
	}
}

// TestDynamoDBFlowStore tests the DynamoDB flow store
func TestDynamoDBFlowStore(t *testing.T) {
	// Get test client (mock by default, real with -real-dynamodb flag)
	client, err := GetTestDynamoDBClient()
	if err != nil {
		t.Fatalf("Failed to get test DynamoDB client: %v", err)
	}

	// Create flow store
	store := NewDynamoDBFlowStore(client, "test_")

	// Initialize store
	err = store.Initialize()
	assert.NoError(t, err)

	// Test basic CRUD operations
	accountID := "test-account"
	flowID := "test-flow"
	flowDef := []byte(`{"metadata":{"name":"Test Flow","description":"A test flow","version":"1.0.0"}}`)

	// Save flow
	err = store.SaveFlow(accountID, flowID, flowDef)
	assert.NoError(t, err)

	// Get flow
	retrievedDef, err := store.GetFlow(accountID, flowID)
	assert.NoError(t, err)
	assert.Equal(t, string(flowDef), string(retrievedDef))

	// List flows
	flowIDs, err := store.ListFlows(accountID)
	assert.NoError(t, err)
	assert.Contains(t, flowIDs, flowID)

	// Get flow metadata
	metadata, err := store.GetFlowMetadata(accountID, flowID)
	assert.NoError(t, err)
	assert.Equal(t, flowID, metadata.ID)
	assert.Equal(t, accountID, metadata.AccountID)

	// List flows with metadata
	metadataList, err := store.ListFlowsWithMetadata(accountID)
	assert.NoError(t, err)
	found := false
	for _, m := range metadataList {
		if m.ID == flowID {
			found = true
			break
		}
	}
	assert.True(t, found)

	// Delete flow
	err = store.DeleteFlow(accountID, flowID)
	assert.NoError(t, err)

	// Verify flow is deleted
	_, err = store.GetFlow(accountID, flowID)
	assert.Error(t, err)

	// Clean up (only for real DynamoDB)
	if *useRealDynamoDB {
		if realClient, ok := client.(*dynamodb.DynamoDB); ok {
			_, err = realClient.DeleteTable(&dynamodb.DeleteTableInput{
				TableName: aws.String(store.tableName),
			})
			if err != nil {
				t.Logf("Failed to delete table %s: %v", store.tableName, err)
			} else {
				// Wait for table to be deleted
				werr := realClient.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{
					TableName: aws.String(store.tableName),
				})
				if werr != nil {
					t.Logf("Failed to wait for table %s to be deleted: %v", store.tableName, werr)
				}
			}
		}
	}
}

// Integration tests for other DynamoDB stores would follow a similar pattern
// but are omitted for brevity. In a real project, you would have comprehensive
// tests for each store type.

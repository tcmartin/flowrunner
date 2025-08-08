package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/tcmartin/flowrunner/pkg/auth"
	"github.com/tcmartin/flowrunner/pkg/config"
	"github.com/tcmartin/flowrunner/pkg/middleware"
	"github.com/tcmartin/flowrunner/pkg/plugins"
	"github.com/tcmartin/flowrunner/pkg/registry"
	"github.com/tcmartin/flowrunner/pkg/runtime"
)

// Server represents the HTTP API server
type Server struct {
	config         *config.Config
	router         *mux.Router
	server         *http.Server
	flowRegistry   registry.FlowRegistry
	accountService auth.AccountService
	secretVault    auth.ExtendedSecretVault
	flowRuntime    runtime.FlowRuntime
	pluginRegistry plugins.PluginRegistry
	wsManager      *WebSocketManager
}

// NewServer creates a new API server
func NewServer(cfg *config.Config, flowRegistry registry.FlowRegistry, accountService auth.AccountService, secretVault auth.ExtendedSecretVault, pluginRegistry plugins.PluginRegistry) *Server {
	s := &Server{
		config:         cfg,
		router:         mux.NewRouter(),
		flowRegistry:   flowRegistry,
		accountService: accountService,
		secretVault:    secretVault,
		pluginRegistry: pluginRegistry,
		wsManager:      NewWebSocketManager(nil), // No flow runtime in basic constructor
	}

	s.setupRoutes()
	return s
}

// NewServerWithRuntime creates a new API server with flow runtime
func NewServerWithRuntime(cfg *config.Config, flowRegistry registry.FlowRegistry, accountService auth.AccountService, secretVault auth.ExtendedSecretVault, flowRuntime runtime.FlowRuntime, pluginRegistry plugins.PluginRegistry) *Server {
	s := &Server{
		config:         cfg,
		router:         mux.NewRouter(),
		flowRegistry:   flowRegistry,
		accountService: accountService,
		secretVault:    secretVault,
		flowRuntime:    flowRuntime,
		pluginRegistry: pluginRegistry,
		wsManager:      NewWebSocketManager(flowRuntime),
	}

	s.setupRoutes()
	return s
}

// Start starts the HTTP server
func (s *Server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.config.Server.Host, s.config.Server.Port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Printf("Starting HTTP server on %s", addr)

	var err error
	if s.config.Server.TLS.Enabled {
		err = s.server.ListenAndServeTLS(
			s.config.Server.TLS.CertFile,
			s.config.Server.TLS.KeyFile,
		)
	} else {
		err = s.server.ListenAndServe()
	}

	// If the server was shut down gracefully, this error is expected
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Stop stops the HTTP server gracefully
func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// setupRoutes configures the API routes
func (s *Server) setupRoutes() {
	// Create authentication middleware
	authMiddleware := middleware.NewAuthMiddleware(s.accountService)

	// Public routes (no authentication required)
	s.router.HandleFunc("/health", s.handleHealth).Methods(http.MethodGet, http.MethodOptions)
	s.router.HandleFunc("/", s.handleHealth).Methods(http.MethodGet, http.MethodOptions)

	// API router with version prefix
	api := s.router.PathPrefix("/api/v1").Subrouter()

	// Public routes (no authentication required)
	api.HandleFunc("/health", s.handleHealth).Methods(http.MethodGet, http.MethodOptions)
	api.HandleFunc("/login", s.handleLogin).Methods(http.MethodPost, http.MethodOptions)

	// Account routes
	accounts := api.PathPrefix("/accounts").Subrouter()
	accounts.HandleFunc("", s.handleCreateAccount).Methods(http.MethodPost, http.MethodOptions)

	// Authenticated routes
	authenticated := api.PathPrefix("").Subrouter()
	authenticated.Use(authMiddleware.Authenticate)

	// Account management routes (authenticated)
	accountsMgmt := authenticated.PathPrefix("/accounts").Subrouter()
	accountsMgmt.HandleFunc("", s.handleListAccounts).Methods(http.MethodGet, http.MethodOptions)
	accountsMgmt.HandleFunc("/{id}", s.handleGetAccount).Methods(http.MethodGet, http.MethodOptions)
	accountsMgmt.HandleFunc("/{id}", s.handleDeleteAccount).Methods(http.MethodDelete, http.MethodOptions)
	accountsMgmt.HandleFunc("/{id}", s.handleUpdateAccount).Methods(http.MethodPut, http.MethodOptions)

	// Flow routes
	flows := authenticated.PathPrefix("/flows").Subrouter()
	flows.HandleFunc("", s.handleListFlows).Methods(http.MethodGet, http.MethodOptions)
	flows.HandleFunc("", s.handleCreateFlow).Methods(http.MethodPost, http.MethodOptions)
	flows.HandleFunc("/{id}", s.handleGetFlow).Methods(http.MethodGet, http.MethodOptions)
	flows.HandleFunc("/{id}", s.handleUpdateFlow).Methods(http.MethodPut, http.MethodOptions)
	flows.HandleFunc("/{id}", s.handleDeleteFlow).Methods(http.MethodDelete, http.MethodOptions)
	flows.HandleFunc("/{id}/metadata", s.handleUpdateFlowMetadata).Methods(http.MethodPatch, http.MethodOptions)
	flows.HandleFunc("/search", s.handleSearchFlows).Methods(http.MethodPost, http.MethodOptions)

	// Flow execution routes
	flows.HandleFunc("/{id}/run", s.handleRunFlow).Methods(http.MethodPost, http.MethodOptions)

	// Execution routes
	executions := authenticated.PathPrefix("/executions").Subrouter()
	executions.HandleFunc("/{id}", s.handleGetExecution).Methods(http.MethodGet, http.MethodOptions)
	executions.HandleFunc("/{id}/logs", s.handleGetExecutionLogs).Methods(http.MethodGet, http.MethodOptions)
	executions.HandleFunc("/{id}", s.handleCancelExecution).Methods(http.MethodDelete, http.MethodOptions)

	// WebSocket route for real-time execution updates (authenticated)
	authenticated.HandleFunc("/ws", s.handleWebSocket).Methods(http.MethodGet)

	// Account management routes (authenticated)
	accountsMgmt.HandleFunc("/me", s.handleGetCurrentAccount).Methods(http.MethodGet, http.MethodOptions)
	accountsMgmt.HandleFunc("/refresh-token", s.handleRefreshToken).Methods(http.MethodPost, http.MethodOptions)

	// Secret management routes (authenticated)
	secrets := authenticated.PathPrefix("/accounts/{accountId}/secrets").Subrouter()
	secrets.HandleFunc("", s.handleListSecrets).Methods(http.MethodGet, http.MethodOptions)
	secrets.HandleFunc("/keys", s.handleSecretKeys).Methods(http.MethodGet, http.MethodOptions)
	secrets.HandleFunc("/{key}", s.handleCreateSecret).Methods(http.MethodPost, http.MethodOptions)
	secrets.HandleFunc("/{key}", s.handleGetSecret).Methods(http.MethodGet, http.MethodOptions)
	secrets.HandleFunc("/{key}", s.handleUpdateSecret).Methods(http.MethodPut, http.MethodOptions)
	secrets.HandleFunc("/{key}", s.handleDeleteSecret).Methods(http.MethodDelete, http.MethodOptions)

	// Structured secret management routes (authenticated)
	structuredSecrets := authenticated.PathPrefix("/accounts/{accountId}/structured-secrets").Subrouter()
	structuredSecrets.HandleFunc("", s.handleListStructuredSecrets).Methods(http.MethodGet, http.MethodOptions)
	structuredSecrets.HandleFunc("/search", s.handleSearchStructuredSecrets).Methods(http.MethodPost, http.MethodOptions)
	structuredSecrets.HandleFunc("/{key}", s.handleCreateStructuredSecret).Methods(http.MethodPost, http.MethodOptions)
	structuredSecrets.HandleFunc("/{key}", s.handleGetStructuredSecret).Methods(http.MethodGet, http.MethodOptions)
	structuredSecrets.HandleFunc("/{key}", s.handleUpdateStructuredSecret).Methods(http.MethodPut, http.MethodOptions)
	structuredSecrets.HandleFunc("/{key}", s.handleDeleteStructuredSecret).Methods(http.MethodDelete, http.MethodOptions)
	structuredSecrets.HandleFunc("/{key}/field/{field}", s.handleGetStructuredSecretField).Methods(http.MethodGet, http.MethodOptions)
	structuredSecrets.HandleFunc("/{key}/metadata", s.handleUpdateStructuredSecretMetadata).Methods(http.MethodPatch, http.MethodOptions)

	// Type-specific structured secret routes
	oauthSecrets := authenticated.PathPrefix("/accounts/{accountId}/oauth-secrets").Subrouter()
	oauthSecrets.HandleFunc("/{key}", s.handleCreateOAuthSecret).Methods(http.MethodPost, http.MethodOptions)
	oauthSecrets.HandleFunc("/{key}", s.handleGetOAuthSecret).Methods(http.MethodGet, http.MethodOptions)

	apiKeySecrets := authenticated.PathPrefix("/accounts/{accountId}/api-key-secrets").Subrouter()
	apiKeySecrets.HandleFunc("/{key}", s.handleCreateAPIKeySecret).Methods(http.MethodPost, http.MethodOptions)
	apiKeySecrets.HandleFunc("/{key}", s.handleGetAPIKeySecret).Methods(http.MethodGet, http.MethodOptions)

	dbSecrets := authenticated.PathPrefix("/accounts/{accountId}/database-secrets").Subrouter()
	dbSecrets.HandleFunc("/{key}", s.handleCreateDatabaseSecret).Methods(http.MethodPost, http.MethodOptions)
	dbSecrets.HandleFunc("/{key}", s.handleGetDatabaseSecret).Methods(http.MethodGet, http.MethodOptions)

	jwtSecrets := authenticated.PathPrefix("/accounts/{accountId}/jwt-secrets").Subrouter()
	jwtSecrets.HandleFunc("/{key}", s.handleCreateJWTSecret).Methods(http.MethodPost, http.MethodOptions)
	jwtSecrets.HandleFunc("/{key}", s.handleGetJWTSecret).Methods(http.MethodGet, http.MethodOptions)

	// Plugin management routes (authenticated)
	plugins := authenticated.PathPrefix("/plugins").Subrouter()
	plugins.HandleFunc("", s.handleListPlugins).Methods(http.MethodGet, http.MethodOptions)
	plugins.HandleFunc("/{name}", s.handleGetPlugin).Methods(http.MethodGet, http.MethodOptions)

	// Debug middleware to log all requests
	s.router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Printf("Request: %s %s", r.Method, r.URL.Path)
			next.ServeHTTP(w, r)
		})
	})

	// CORS middleware for all routes
	s.router.Use(middleware.CORS)
}

// handleHealth handles the health check endpoint
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// handleCreateAccount handles account creation
func (s *Server) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	accountID, err := s.accountService.CreateAccount(req.Username, req.Password)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	account, err := s.accountService.GetAccount(accountID)
	if err != nil {
		http.Error(w, "Failed to retrieve account", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(account)
}

// handleGetCurrentAccount handles retrieving the current account
func (s *Server) handleGetCurrentAccount(w http.ResponseWriter, r *http.Request) {
	accountID, ok := middleware.GetAccountID(r)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	account, err := s.accountService.GetAccount(accountID)
	if err != nil {
		http.Error(w, "Failed to retrieve account", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(account)
}

// handleListFlows handles listing flows
func (s *Server) handleListFlows(w http.ResponseWriter, r *http.Request) {
	accountID, ok := middleware.GetAccountID(r)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	flows, err := s.flowRegistry.List(accountID)
	if err != nil {
		http.Error(w, "Failed to list flows", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(flows)
}

// handleCreateFlow handles flow creation
func (s *Server) handleCreateFlow(w http.ResponseWriter, r *http.Request) {
	accountID, ok := middleware.GetAccountID(r)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	var req struct {
		Name    string `json:"name"`
		Content string `json:"content"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	flowID, err := s.flowRegistry.Create(accountID, req.Name, req.Content)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"id": flowID,
	})
}

// handleGetFlow handles retrieving a flow
func (s *Server) handleGetFlow(w http.ResponseWriter, r *http.Request) {
	accountID, ok := middleware.GetAccountID(r)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(r)
	flowID := vars["id"]

	content, err := s.flowRegistry.Get(accountID, flowID)
	if err != nil {
		http.Error(w, "Flow not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	w.Write([]byte(content))
}

// handleUpdateFlow handles updating a flow
func (s *Server) handleUpdateFlow(w http.ResponseWriter, r *http.Request) {
	accountID, ok := middleware.GetAccountID(r)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(r)
	flowID := vars["id"]

	var req struct {
		Content string `json:"content"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	err := s.flowRegistry.Update(accountID, flowID, req.Content)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleDeleteFlow handles deleting a flow
func (s *Server) handleDeleteFlow(w http.ResponseWriter, r *http.Request) {
	accountID, ok := middleware.GetAccountID(r)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(r)
	flowID := vars["id"]

	err := s.flowRegistry.Delete(accountID, flowID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleUpdateFlowMetadata handles updating flow metadata
func (s *Server) handleUpdateFlowMetadata(w http.ResponseWriter, r *http.Request) {
	accountID, ok := middleware.GetAccountID(r)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(r)
	flowID := vars["id"]

	var metadata registry.FlowMetadata
	if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	err := s.flowRegistry.UpdateMetadata(accountID, flowID, metadata)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleSearchFlows handles searching for flows
func (s *Server) handleSearchFlows(w http.ResponseWriter, r *http.Request) {
	accountID, ok := middleware.GetAccountID(r)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	var filters registry.FlowSearchFilters
	if err := json.NewDecoder(r.Body).Decode(&filters); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	flows, err := s.flowRegistry.Search(accountID, filters)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(flows)
}

// Flow execution handlers

// handleRunFlow handles executing a flow
func (s *Server) handleRunFlow(w http.ResponseWriter, r *http.Request) {
	if s.flowRuntime == nil {
		http.Error(w, "Flow runtime not available", http.StatusServiceUnavailable)
		return
	}

	accountID, ok := middleware.GetAccountID(r)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(r)
	flowID := vars["id"]

	var req struct {
		Input map[string]interface{} `json:"input,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Initialize input if nil
	if req.Input == nil {
		req.Input = make(map[string]interface{})
	}

	executionID, err := s.flowRuntime.Execute(accountID, flowID, req.Input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response := map[string]interface{}{
		"execution_id": executionID,
		"status":       "running",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// handleGetExecution handles getting execution status
func (s *Server) handleGetExecution(w http.ResponseWriter, r *http.Request) {
	if s.flowRuntime == nil {
		http.Error(w, "Flow runtime not available", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	executionID := vars["id"]

    status, err := s.flowRuntime.GetStatus(executionID)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }

    // Backward-compatible response that includes both 'results' and legacy 'result'
    resp := map[string]interface{}{
        "id":           status.ID,
        "flow_id":      status.FlowID,
        "status":       status.Status,
        "start_time":   status.StartTime,
        "end_time":     status.EndTime,
        "error":        status.Error,
        "results":      status.Results,
        "progress":     status.Progress,
        "current_node": status.CurrentNode,
        "metadata":     status.Metadata,
    }
    // Provide top-level 'result' alias expected by some tests/clients
    if status.Results != nil {
        resp["result"] = status.Results
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(resp)
}

// handleGetExecutionLogs handles getting execution logs
func (s *Server) handleGetExecutionLogs(w http.ResponseWriter, r *http.Request) {
	if s.flowRuntime == nil {
		http.Error(w, "Flow runtime not available", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	executionID := vars["id"]

	logs, err := s.flowRuntime.GetLogs(executionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}

// handleCancelExecution handles canceling an execution
func (s *Server) handleCancelExecution(w http.ResponseWriter, r *http.Request) {
	if s.flowRuntime == nil {
		http.Error(w, "Flow runtime not available", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	executionID := vars["id"]

	err := s.flowRuntime.Cancel(executionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleWebSocket handles WebSocket connections for real-time execution updates
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Extract account ID from request context (set by auth middleware)
	accountID, ok := r.Context().Value(middleware.AccountIDKey).(string)
	if !ok {
		http.Error(w, "Authentication required", http.StatusUnauthorized)
		return
	}

	if s.wsManager == nil {
		http.Error(w, "WebSocket service not available", http.StatusServiceUnavailable)
		return
	}

	// Handle the WebSocket connection
	s.wsManager.HandleWebSocket(w, r, accountID)
}

module github.com/tcmartin/flowrunner

go 1.23.0

toolchain go1.24.3

require (
	github.com/alicebob/miniredis/v2 v2.35.0
	github.com/aws/aws-sdk-go v1.55.7
	github.com/dop251/goja v0.0.0-20250630131328-58d95d85e994
	github.com/emersion/go-imap v1.2.1
	github.com/emersion/go-message v0.18.2
	github.com/go-redis/redis/v8 v8.11.5
	github.com/golang-jwt/jwt/v4 v4.5.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.3
	github.com/joho/godotenv v1.5.1
	github.com/lib/pq v1.10.9
	github.com/r3labs/sse/v2 v2.10.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/spf13/cobra v1.9.1
	github.com/stretchr/testify v1.8.1
	github.com/tcmartin/flowlib v0.1.0
	golang.org/x/crypto v0.40.0
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dlclark/regexp2 v1.11.4 // indirect
	github.com/emersion/go-sasl v0.0.0-20200509203442-7bfe0ed36a21 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	golang.org/x/net v0.41.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	gopkg.in/cenkalti/backoff.v1 v1.1.0 // indirect
)

replace github.com/tcmartin/flowlib => ./flowlib

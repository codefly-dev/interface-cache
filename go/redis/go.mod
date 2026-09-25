module github.com/codefly-dev/interface-cache/go/redis

go 1.27.0

require (
	github.com/codefly-dev/interface-cache/go/cache v0.0.0
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

// Until go/cache is tagged, build against the copy in this repository.
replace github.com/codefly-dev/interface-cache/go/cache => ../cache

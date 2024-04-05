.PHONY: test build

test:
	go test -covermode=atomic -coverprofile=coverage.cov ./...

build:
	go build -o bin/run examples/main.go

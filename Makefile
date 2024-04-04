.PHONY: test build

test:
	go test -race -count=1 -covermode=atomic -coverprofile=coverage.cov ./...

build:
	go build -o bin/run examples/main.go

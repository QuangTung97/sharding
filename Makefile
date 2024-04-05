.PHONY: test build

test:
	go test -count=1 -coverprofile=coverage.cov ./...

build:
	go build -o bin/run examples/main.go

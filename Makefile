.PHONY: test build lint coverage install-tools test-raw

test:
	go test -count=1 -coverprofile=coverage.out ./...

build:
	go build -o bin/run examples/main.go

lint:
	$(foreach f,$(shell go fmt ./...),@echo "Forgot to format file: ${f}"; exit 1;)
	go vet ./...
	revive -config revive.toml -formatter friendly ./...

coverage:
	go tool cover -func coverage.out | grep ^total

install-tools:
	go install github.com/mgechev/revive

test-raw:
	go test -count=1 ./...

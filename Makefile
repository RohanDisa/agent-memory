.PHONY: test race cover lint build demo e2e

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

cover:
	go test -count=1 -coverpkg=./internal/... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

build:
	go build -o bin/node ./cmd/node
	go build -o bin/memctl ./cmd/memctl

demo:
	bash scripts/demo.sh

e2e:
	go test -tags e2e -count=1 -timeout 10m ./test/e2e

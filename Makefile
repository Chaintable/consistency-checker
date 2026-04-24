.PHONY: fmt fmt-check vet lint test race build ci

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "Run 'make fmt' first"; gofmt -l .; exit 1)

vet:
	go vet ./...

lint:
	golangci-lint run --timeout=5m

test:
	go test -count=1 -shuffle=on ./...

race:
	go test -race -count=1 ./...

build:
	go build -o checker cmd/checker/*.go

ci: fmt-check vet lint test race build

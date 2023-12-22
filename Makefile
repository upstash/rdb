.PHONY: build test

build:
	go mod tidy
	go fmt ./...
	go vet ./...
ifneq (, $(shell which staticcheck))
	staticcheck ./...
endif
	go build ./...

test:
 ifeq (, $(shell which gotestsum))
	go test ./...
 else
	gotestsum --hide-summary=skipped
 endif
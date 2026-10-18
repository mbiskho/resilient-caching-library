.PHONY: test bench demo

all: test

test:
	go test ./...

bench:
	go test -bench . -benchmem ./...

demo:
	go run ./cmd/demo

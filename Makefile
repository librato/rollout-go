default: test

gopath = `pwd`

build:
	GOPATH=$(gopath) go clean
	GOPATH=$(gopath) go build

test: build
	gofmt -e . > /dev/null
	GOPATH=$(gopath) go test -v .
	GOPATH=$(gopath) go vet

format:
	gofmt -w *.go


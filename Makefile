default: test

gopath = `pwd`

build:
	GOPATH=$(gopath) go clean
	GOPATH=$(gopath) CGO_LDFLAGS="-lzookeeper_mt" CGO_CFLAGS="-I/usr/include/zookeeper" go build

test: build 
	gofmt -e . > /dev/null
	GOPATH=$(gopath) go test -v .
	GOPATH=$(gopath) go vet

format:
	gofmt -w *.go


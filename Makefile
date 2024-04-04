.PHONY: clean test lint build.local build.linux build.osx build.docker build.push

BINARY        ?= es-operator
VERSION       ?= 1.0
IMAGE         ?= registry.central.aiware.com/$(BINARY)
TAG           ?= $(VERSION)
SOURCES       = $(shell find . -name '*.go')
DOCKERFILE    ?= Dockerfile
GOPKGS        = $(shell go list ./... | grep -v /e2e)
BUILD_FLAGS   ?= -v -gcflags all=-N
LDFLAGS       ?= -X main.version=$(VERSION) -w -s

default: build.local

clean:
	rm -rf build
	rm -rf vendor
	docker image rm ${IMAGE}:${TAG}

test:
	go test -v -coverprofile=profile.cov $(GOPKGS)

lint:
	golangci-lint run ./...

build.local: build/$(BINARY)
build.linux: build/linux/$(BINARY)

build/linux/e2e: $(SOURCES)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test -c -o build/linux/$(notdir $@) $(BUILD_FLAGS) ./cmd/$(notdir $@)

build/linux/%: $(SOURCES)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build $(BUILD_FLAGS) -o build/linux/$(notdir $@) -ldflags "$(LDFLAGS)" ./cmd/$(notdir $@)

build/%: $(SOURCES)
	CGO_ENABLED=0 go build -o build/$(notdir $@) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" ./cmd/$(notdir $@)

build/$(BINARY): $(SOURCES)
	CGO_ENABLED=0 go build -o build/$(BINARY) $(BUILD_FLAGS) -ldflags "$(LDFLAGS)" .

build/linux/$(BINARY): $(SOURCES)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build $(BUILD_FLAGS) -o build/linux/$(BINARY) -ldflags "$(LDFLAGS)" .

build.docker: build.linux
	docker build --rm -t "$(IMAGE):$(TAG)" -f $(DOCKERFILE) .

build.push: build.docker
	docker push "$(IMAGE):$(TAG)"

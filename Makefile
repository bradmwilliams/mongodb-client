build:
	go build -ldflags "-X k8s.io/client-go/pkg/version.gitVersion=$$(git describe --abbrev=8 --dirty --always)" -mod vendor -o mongodb-client
.PHONY: build

debug:
	go build -gcflags="all=-N -l" -ldflags "-X k8s.io/client-go/pkg/version.gitVersion=$$(git describe --abbrev=8 --dirty --always)" -mod vendor -o mongodb-client .
.PHONY: build

update-deps:
	GO111MODULE=on go mod vendor
.PHONY: update-deps


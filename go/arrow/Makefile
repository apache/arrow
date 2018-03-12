GO_BUILD=go build
GO_GEN=go generate
GO_TEST?=go test
GOPATH=$(realpath ../../../../../..)

GO_SOURCES  := $(shell find . -path ./_lib -prune -o -name '*.go' -not -name '*_test.go')
ALL_SOURCES := $(shell find . -path ./_lib -prune -o -name '*.go' -name '*.s' -not -name '*_test.go')
SOURCES_NO_VENDOR := $(shell find . -path ./vendor -prune -o -name "*.go" -not -name '*_test.go' -print)

.PHONEY: test bench assembly generate

assembly:
	@$(MAKE) -C memory assembly
	@$(MAKE) -C math assembly

generate: bin/tmpl
	bin/tmpl -i -data=numeric.tmpldata type_traits_numeric.gen.go.tmpl array/numeric.gen.go.tmpl array/numericbuilder.gen.go.tmpl array/bufferbuilder_numeric.gen.go.tmpl
	bin/tmpl -i -data=datatype_numeric.gen.go.tmpldata datatype_numeric.gen.go.tmpl
	@$(MAKE) -C math generate

fmt: $(SOURCES_NO_VENDOR)
	goimports -w $^

bench: $(GO_SOURCES) | assembly
	$(GO_TEST) $(GO_TEST_ARGS) -bench=. -run=- ./...

bench-noasm: $(GO_SOURCES)
	$(GO_TEST) $(GO_TEST_ARGS) -tags='noasm' -bench=. -run=- ./...

test: $(GO_SOURCES) | assembly
	$(GO_TEST) $(GO_TEST_ARGS) ./...

test-noasm: $(GO_SOURCES)
	$(GO_TEST) $(GO_TEST_ARGS) -tags='noasm' ./...

bin/tmpl: _tools/tmpl/main.go
	$(GO_BUILD) -o $@ ./_tools/tmpl


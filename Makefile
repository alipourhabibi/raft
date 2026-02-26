PROTO_DIR     := proto
OUT_DIR       := gen/go
GO_PKG_BASE   := github.com/alipourhabibi/raft/gen/go

PROTOC        := protoc
PROTOC_GEN_GO := protoc-gen-go
PROTOC_GEN_GRPC := protoc-gen-go-grpc

PROTO_FILES   := $(shell find $(PROTO_DIR) -type f -name '*.proto')

GENERATED := $(patsubst $(PROTO_DIR)/%.proto,$(OUT_DIR)/%.pb.go,$(PROTO_FILES)) \
             $(patsubst $(PROTO_DIR)/%.proto,$(OUT_DIR)/%_grpc.pb.go,$(PROTO_FILES))

.PHONY: all generate clean install-tools help

all: generate

generate: $(GENERATED)

$(OUT_DIR)/%.pb.go $(OUT_DIR)/%_grpc.pb.go: $(PROTO_DIR)/%.proto
	@echo "Generating $@ from $< ..."
	@mkdir -p $(dir $@)
	$(PROTOC) \
		--proto_path=$(PROTO_DIR) \
		--proto_path=. \
		--go_out=$(OUT_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(OUT_DIR) \
		--go-grpc_opt=paths=source_relative \
		$<

clean:
	rm -rf $(OUT_DIR)/*

install-tools:
	@echo "Installing/updating required protoc plugins..."
	@go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

help:
	@echo "Usage:"
	@echo "  make              → generate all protobuf + gRPC code"
	@echo "  make generate     → same as above"
	@echo "  make clean        → remove all generated files in $(OUT_DIR)/"
	@echo "  make install-tools → install protoc plugins (run once)"
	@echo ""
	@echo "Proto files are discovered recursively from: $(PROTO_DIR)/"
	@echo "Generated files go to: $(OUT_DIR)/ with same subdir structure"

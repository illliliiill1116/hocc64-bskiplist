ROOT        = ..
BUILD_DIR   = $(ROOT)/build
INC_DIR     = $(ROOT)/include

CGO_CFLAGS_VAL  = -I$(abspath $(INC_DIR))
CGO_LDFLAGS_VAL = -L$(abspath $(BUILD_DIR)) -lbskiplist -lpthread -lm

GO := $(shell command -v go 2>/dev/null)

.PHONY: all check clean tidy

all: check

tidy:
ifndef GO
	$(error "go not found, please install Go first")
endif
	@echo "--- Running go mod tidy ---"
	go mod tidy


check: tidy
	@echo "--- Running Linearizability Tests ---"
	CGO_CFLAGS="$(CGO_CFLAGS_VAL)" \
	CGO_LDFLAGS="$(CGO_LDFLAGS_VAL)" \
	$(GO) test -v -count=1 -timeout 120s ./...


clean:
ifndef GO
	@echo "go not found, skipping clean-testcache"
else
	$(GO) clean -testcache
endif
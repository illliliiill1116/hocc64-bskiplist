CC      = gcc
CFLAGS  = -Wall -Wextra -MMD -MP -I./include
LDFLAGS = -pthread -lm

SRC_DIR   = src
INC_DIR   = include
TEST_DIR  = test
BUILD_DIR = build

# ------------------------------------------------------------------ #
# Build options        												 #
# ------------------------------------------------------------------ #

DEBUG           ?= 0
MEASURE_LATENCY ?= 0

ASAN			?= 0
CHECK_STRUCTURE ?= 0

ifeq ($(DEBUG),1)
    CFLAGS += -O0 -g -DDEBUG
else
    CFLAGS += -O3 -DNDEBUG -march=native
endif

ifeq ($(MEASURE_LATENCY),1)
    CFLAGS += -DMEASURE_LATENCY=1
endif

ifeq ($(ASAN),1)
    CFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer
endif

ifeq ($(CHECK_STRUCTURE),1)
    CFLAGS += -DCHECK_STRUCTURE=1
endif

# ------------------------------------------------------------------ #
# Sources and targets                                                #
# ------------------------------------------------------------------ #

SRCS      = $(wildcard $(SRC_DIR)/*.c)
OBJS      = $(SRCS:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)
TEST_SRCS = $(wildcard $(TEST_DIR)/*.c)
TEST_BINS = $(TEST_SRCS:$(TEST_DIR)/%.c=$(BUILD_DIR)/%)
STATIC_LIB  = $(BUILD_DIR)/libbskiplist.a

# ------------------------------------------------------------------ #
# Rules                                                              #
# ------------------------------------------------------------------ #

.PHONY: all clean debug

all: $(BUILD_DIR) $(OBJS) $(TEST_BINS) lib

debug:
	$(MAKE) DEBUG=1

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/%: $(TEST_DIR)/%.c $(OBJS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $< $(OBJS) -o $@ $(LDFLAGS)


$(STATIC_LIB): $(OBJS)
	ar rcs $@ $^

lib: $(BUILD_DIR) $(STATIC_LIB)

clean:
	rm -rf $(BUILD_DIR)

-include $(OBJS:.o=.d)
CC = gcc
CFLAGS = -Wall -Wextra -I./include -O3 -g #-fsanitize=thread
LDFLAGS = -pthread

SRC_DIR = src
INC_DIR = include
TEST_DIR = test
BUILD_DIR = build


SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(SRCS:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)

TEST_SRCS = $(wildcard $(TEST_DIR)/*.c)
TEST_BINS = $(TEST_SRCS:$(TEST_DIR)/%.c=$(BUILD_DIR)/%)

all: $(BUILD_DIR) $(OBJS) $(TEST_BINS)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/%: $(TEST_DIR)/%.c $(OBJS)
	$(CC) $(CFLAGS) $< $(OBJS) -o $@ $(LDFLAGS)


clean:
	rm -rf $(BUILD_DIR)

.PHONY: all clean run_tests
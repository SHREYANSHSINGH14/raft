BIN_NAME=raftd
CONFIG_FILE=config.dev.env

dev:
    set -a && source $(CONFIG_FILE) && set +a && go build -o $(BIN_NAME) main.go && ./$(BIN_NAME) server start
build:
	go build -o $(BIN_NAME) main.go
run:
	./$(BIN_NAME) server start
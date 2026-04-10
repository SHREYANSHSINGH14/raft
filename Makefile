BIN_NAME=raftd
CONFIG_FILE=config.dev.env
build:
	go build -o $(BIN_NAME) main.go
run:
	./$(BIN_NAME) server start
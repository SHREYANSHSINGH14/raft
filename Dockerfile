FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o raftd main.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/raftd .
EXPOSE 50051
CMD ["./raftd", "server", "start"]
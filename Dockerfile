FROM golang:alpine AS builder

WORKDIR /app

# Cache Go module dependencies first
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and compile statically linked Linux binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /nexusrelay ./cmd/api/main.go

# Stage 2: Minimal Production Runtime Stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

# Copy only the compiled binary from builder
COPY --from=builder /nexusrelay .

EXPOSE 8080

CMD ["./nexusrelay"]
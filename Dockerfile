# Build stage
FROM golang:1.18-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o flashdb ./main.go

# Runtime stage
FROM alpine:latest

WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/flashdb .

# Expose Redis default port
EXPOSE 6379

# Run the application
CMD ["./flashdb"]

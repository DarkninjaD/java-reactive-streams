# Step 1: Build Stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy the go.mod and main.go files
COPY go.mod ./
COPY main.go ./

# Download dependencies and tidy the module
RUN go mod tidy

# Build the application statically
RUN CGO_ENABLED=0 GOOS=linux go build -o server .

# Step 2: Final Run Stage
FROM alpine:latest

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/server .

# Expose port 8080 to the outside world
EXPOSE 8080

# Command to run the executable
CMD ["./server"]
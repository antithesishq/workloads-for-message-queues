# The Workload image contains binaries and configuration to exercise the SUT

# Start from an a base golang image 
FROM docker.io/golang:1.25-bookworm

# Set working directory
WORKDIR /app

# Copy go module files first (for caching)
COPY go.mod go.sum* ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY ./main.go .
COPY ./liveness.go .

# Build the binary
RUN go build -o /workload main.go liveness.go

# Run the compiled binary
ENTRYPOINT ["/workload"]
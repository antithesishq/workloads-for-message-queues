FROM docker.io/golang:1.25-bookworm

WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN go build -o /workload .

ENTRYPOINT ["/workload"]

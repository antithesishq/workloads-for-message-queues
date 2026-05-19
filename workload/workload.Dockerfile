FROM --platform=linux/amd64 docker.io/library/golang:1.24 AS builder
WORKDIR /src
COPY . ./
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /workload .

FROM --platform=linux/amd64 docker.io/library/debian:bookworm-slim
COPY --from=builder /workload /usr/local/bin/workload
ENV NO_COLOR=1
CMD ["/usr/local/bin/workload"]

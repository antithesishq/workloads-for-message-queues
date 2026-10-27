FROM docker.io/library/golang:1.27-trixie

  WORKDIR /src
  COPY go.mod go.sum ./
  RUN go mod download
  RUN go install github.com/antithesishq/antithesis-sdk-go/tools/antithesis-go-toolexec@v0.8.0

  COPY *.go ./
  RUN mkdir -p /symbols && go build -toolexec=antithesis-go-toolexec -o /workload .

  ENTRYPOINT ["/workload"]
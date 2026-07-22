FROM golang:1.25-alpine AS builder

# cache download modules between source code changes
COPY go.mod /app/
COPY go.sum /app/
WORKDIR /app
RUN go mod download

COPY . /app
RUN go build -o kuri ./integrations/kuri/cmd/kuri
RUN go build -o gwcfg ./integrations/gwcfg
RUN go build -o s3-proxy ./server/s3frontend/cmd

FROM alpine:3.22.0

WORKDIR /app
COPY --from=builder /app/kuri /app/
COPY --from=builder /app/gwcfg /app/
COPY --from=builder /app/s3-proxy /app/
CMD ["./kuri", "daemon"]

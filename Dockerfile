FROM golang:1.24-alpine AS builder

# cache download modules between source code changes
COPY go.mod /app/
COPY go.sum /app/
WORKDIR /app
RUN go mod download

COPY . /app
RUN go build -o kuri ./integrations/kuri/cmd/kuri

FROM alpine:3.22.0

WORKDIR /app
COPY --from=builder /app/kuri /app/
CMD ["./kuri", "daemon"]

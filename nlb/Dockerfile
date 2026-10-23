FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY nlb.go go.mod ./
RUN CGO_ENABLED=0 GOOS=linux go build -o lb .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root
COPY --from=builder /app/lb .
ENTRYPOINT ["/root/lb"]

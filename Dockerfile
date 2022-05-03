FROM golang:1.17.6-alpine as builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o db-sync .

FROM golang:1.17.6-alpine

WORKDIR /app
RUN apk --no-cache add ca-certificates
COPY db-sync-service-account.json ./
COPY --from=builder /app/db-sync ./

ENTRYPOINT ["/app/db-sync"]


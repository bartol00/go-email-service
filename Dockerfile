FROM golang:latest

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o app .

EXPOSE 9000

CMD ["./app"]
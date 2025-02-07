FROM golang:1.23 AS builder

WORKDIR $GOPATH/src/indices-economicos

COPY app/ ./

RUN go get -u

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .


FROM alpine:latest

WORKDIR /app

COPY --from=builder /go/src/indices-economicos/main ./

RUN mkdir -p ./data/gini
RUN mkdir -p ./data/igpm
RUN mkdir -p ./data/inflacao
RUN mkdir -p ./data/inpc
RUN mkdir -p ./data/ipca
RUN mkdir -p ./data/pib
RUN mkdir -p ./data/selic
RUN mkdir -p ./data/sociais/
RUN mkdir -p ./data/ambientais/
RUN mkdir -p ./data/precos/
RUN mkdir -p ./data/idh/raw/
RUN mkdir -p ./data/ambientais/raw/

CMD ["./main"]
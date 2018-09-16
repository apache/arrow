FROM golang

ADD go /arrow/go
WORKDIR /arrow/go/arrow

RUN go get -d -t -v ./... && \
    go install -v ./...
CMD go test

package ipc // import "github.com/apache/arrow/go/arrow/ipc"

//go:generate protoc -I$GOPATH/pkg/mod/github.com/gogo/protobuf@v1.3.1 -I../../../format --plugin ../_tools/protoc-gen-gogofaster --gogofaster_out=Mgoogle/protobuf/empty.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,plugins=grpc:. Flight.proto

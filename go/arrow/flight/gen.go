package flight // import "github.com/apache/arrow/go/arrow/flight"

//go:generate protoc -I../../../format --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative Flight.proto

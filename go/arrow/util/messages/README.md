How to generate the .pb.go files

```
cd go/arrow/util/
protoc -I ./ --go_out=./messages ./messages/types.proto
```

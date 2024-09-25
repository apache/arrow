package serialize

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func DeserializeProtobufWrappedInAny(b []byte, dst proto.Message) error {
	var anycmd anypb.Any
	if err := proto.Unmarshal(b, &anycmd); err != nil {
		return fmt.Errorf("unable to unmarshal payload to proto.Any: %s", err)
	}

	if err := anycmd.UnmarshalTo(dst); err != nil {
		return fmt.Errorf("unable to unmarshal proto.Any: %s", err)
	}

	return nil
}

func DeserializeProtobuf(b []byte, dst proto.Message) error {
	if err := proto.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("unable to unmarshal protobuf payload: %s", err)
	}

	return nil
}

func SerializeProtobufWrappedInAny(msg proto.Message) ([]byte, error) {
	var anycmd anypb.Any
	if err := anycmd.MarshalFrom(msg); err != nil {
		return nil, fmt.Errorf("unable to marshal proto message to proto.Any: %s", err)
	}

	b, err := proto.Marshal(&anycmd)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal proto.Any to bytes: %s", err)
	}

	return b, nil
}

func SerializeProtobuf(msg proto.Message) ([]byte, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal protobuf message to bytes: %s", err)
	}

	return b, nil
}

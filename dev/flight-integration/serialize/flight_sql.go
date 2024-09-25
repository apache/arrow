package serialize

import (
	"fmt"

	"github.com/apache/arrow/dev/flight-integration/protocol/flight"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func DescForCommand(cmd proto.Message) (*flight.FlightDescriptor, error) {
	var any anypb.Any
	if err := any.MarshalFrom(cmd); err != nil {
		return nil, err
	}

	data, err := proto.Marshal(&any)
	if err != nil {
		return nil, err
	}
	return &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_CMD,
		Cmd:  data,
	}, nil
}

func PackAction(actionType string, msg proto.Message, serialize func(proto.Message) ([]byte, error)) (*flight.Action, error) {
	var action flight.Action
	body, err := serialize(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize action body: %s", err)
	}

	action.Type = actionType
	action.Body = body

	return &action, nil
}

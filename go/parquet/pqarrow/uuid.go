package pqarrow

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/goccy/go-json"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/google/uuid"
)

type UUIDBuilder struct {
	*array.ExtensionBuilder
}

func NewUUIDBuilder(bldr *array.ExtensionBuilder) *UUIDBuilder {
	b := &UUIDBuilder{
		ExtensionBuilder: bldr,
	}
	return b
}

func (b *UUIDBuilder) Append(v uuid.UUID) {
	b.ExtensionBuilder.Builder.(*array.FixedSizeBinaryBuilder).Append(v[:])
}

func (b *UUIDBuilder) AppendValues(v []uuid.UUID, valid []bool) {
	data := make([][]byte, len(v))
	for i, v := range v {
		data[i] = v[:]
	}
	b.ExtensionBuilder.Builder.(*array.FixedSizeBinaryBuilder).AppendValues(data, valid)
}

func (b *UUIDBuilder) UnmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	var val uuid.UUID
	switch v := t.(type) {
	case string:
		data, err := uuid.Parse(v)
		if err != nil {
			return err
		}
		val = data
	case nil:
		b.AppendNull()
		return nil
	default:
		return &json.UnmarshalTypeError{
			Value:  fmt.Sprint(t),
			Type:   reflect.TypeOf([]byte{}),
			Offset: dec.InputOffset(),
			Struct: fmt.Sprintf("FixedSizeBinary[%d]", 16),
		}
	}

	if len(val) != 16 {
		return &json.UnmarshalTypeError{
			Value:  fmt.Sprint(val),
			Type:   reflect.TypeOf([]byte{}),
			Offset: dec.InputOffset(),
			Struct: fmt.Sprintf("FixedSizeBinary[%d]", 16),
		}
	}
	b.Append(val)
	return nil
}

func (b *UUIDBuilder) Unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.UnmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

func (b *UUIDBuilder) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	t, err := dec.Token()
	if err != nil {
		return err
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("fixed size binary builder must unpack from json array, found %s", delim)
	}

	return b.Unmarshal(dec)
}

// UUIDArray is a simple array which is a FixedSizeBinary(16)
type UUIDArray struct {
	array.ExtensionArrayBase
}

func (a UUIDArray) String() string {
	arr := a.Storage().(*array.FixedSizeBinary)
	o := new(strings.Builder)
	o.WriteString("[")
	for i := 0; i < arr.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString("(null)")
		default:
			uuidStr, err := uuid.FromBytes(arr.Value(i))
			if err != nil {
				panic(fmt.Errorf("invalid uuid: %w", err))
			}
			fmt.Fprintf(o, "%q", uuidStr)
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *UUIDArray) MarshalJSON() ([]byte, error) {
	arr := a.Storage().(*array.FixedSizeBinary)
	vals := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			uuidStr, err := uuid.FromBytes(arr.Value(i))
			if err != nil {
				panic(fmt.Errorf("invalid uuid: %w", err))
			}
			vals[i] = uuidStr
		} else {
			vals[i] = nil
		}
	}
	return json.Marshal(vals)
}

func (a *UUIDArray) GetOneForMarshal(i int) interface{} {
	arr := a.Storage().(*array.FixedSizeBinary)
	if a.IsValid(i) {
		uuidObj, err := uuid.FromBytes(arr.Value(i))
		if err != nil {
			panic(fmt.Errorf("invalid uuid: %w", err))
		}
		return uuidObj
	}
	return nil
}

// UUIDType is a simple extension type that represents a FixedSizeBinary(16)
// to be used for representing UUIDs
type UUIDType struct {
	arrow.ExtensionBase
}

// NewUUIDType is a convenience function to create an instance of UuidType
// with the correct storage type
func NewUUIDType() *UUIDType {
	return &UUIDType{
		ExtensionBase: arrow.ExtensionBase{
			Storage: &arrow.FixedSizeBinaryType{ByteWidth: 16}}}
}

// ArrayType returns TypeOf(UuidArray) for constructing uuid arrays
func (UUIDType) ArrayType() reflect.Type { return reflect.TypeOf(UUIDArray{}) }

func (UUIDType) ExtensionName() string { return "uuid" }

// Serialize returns "uuid-serialized" for testing proper metadata passing
func (UUIDType) Serialize() string { return "uuid-serialized" }

// Deserialize expects storageType to be FixedSizeBinaryType{ByteWidth: 16} and the data to be
// "uuid-serialized" in order to correctly create a UuidType for testing deserialize.
func (UUIDType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if string(data) != "uuid-serialized" {
		return nil, fmt.Errorf("type identifier did not match: '%s'", string(data))
	}
	if !arrow.TypeEqual(storageType, &arrow.FixedSizeBinaryType{ByteWidth: 16}) {
		return nil, fmt.Errorf("invalid storage type for UuidType: %s", storageType.Name())
	}
	return NewUUIDType(), nil
}

// UuidTypes are equal if both are named "uuid"
func (u UUIDType) ExtensionEquals(other arrow.ExtensionType) bool {
	return u.ExtensionName() == other.ExtensionName()
}

func (u UUIDType) NewBuilder(bldr *array.ExtensionBuilder) array.Builder {
	return NewUUIDBuilder(bldr)
}
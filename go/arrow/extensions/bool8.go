package extensions

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/internal/json"
)

const ExtensionNameBool8 = "bool8"

// Bool8Type represents a logical boolean that is stored using 8 bits.
type Bool8Type struct {
	arrow.ExtensionBase
}

func NewBool8Type() *Bool8Type {
	return &Bool8Type{ExtensionBase: arrow.ExtensionBase{Storage: arrow.PrimitiveTypes.Int8}}
}

func (b *Bool8Type) ArrayType() reflect.Type { return reflect.TypeOf(Bool8Array{}) }

func (b *Bool8Type) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if data != ExtensionNameBool8 {
		return nil, fmt.Errorf("type identifier did not match: '%s'", data)
	}
	if !arrow.TypeEqual(storageType, arrow.PrimitiveTypes.Int8) {
		return nil, fmt.Errorf("invalid storage type for Bool8Type: %s", storageType.Name())
	}
	return NewBool8Type(), nil
}

func (b *Bool8Type) ExtensionEquals(other arrow.ExtensionType) bool {
	return b.ExtensionName() == other.ExtensionName()
}

func (b *Bool8Type) ExtensionName() string { return ExtensionNameBool8 }

func (b *Bool8Type) Serialize() string { return ExtensionNameBool8 }

func (b *Bool8Type) String() string { return fmt.Sprintf("Bool8<storage=%s>", b.Storage) }

func (*Bool8Type) NewBuilder(bldr *array.ExtensionBuilder) array.Builder {
	return NewBool8Builder(bldr)
}

// Bool8Array is logically an array of boolean values but uses
// 8 bits to store values instead of 1 bit as in the native BooleanArray.
type Bool8Array struct {
	array.ExtensionArrayBase
}

func (a *Bool8Array) String() string {
	var o strings.Builder
	o.WriteString("[")
	for i := 0; i < a.Len(); i++ {
		if i > 0 {
			o.WriteString(" ")
		}
		switch {
		case a.IsNull(i):
			o.WriteString(array.NullValueStr)
		default:
			fmt.Fprintf(&o, "%v", a.Value(i))
		}
	}
	o.WriteString("]")
	return o.String()
}

func (a *Bool8Array) Value(i int) bool {
	return int8ToBool(a.Storage().(*array.Int8).Value(i))
}

func (a *Bool8Array) ValueStr(i int) string {
	switch {
	case a.IsNull(i):
		return array.NullValueStr
	default:
		return fmt.Sprint(a.Value(i))
	}
}

func (a *Bool8Array) MarshalJSON() ([]byte, error) {
	values := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			values[i] = a.Value(i)
		}
	}
	return json.Marshal(values)
}

func (a *Bool8Array) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}
	return a.Value(i)
}

func boolToInt8(v bool) int8 {
	var res int8
	if v {
		res = 1
	}
	return res
}

func int8ToBool(v int8) bool {
	return v != 0
}

// Bool8Builder is a convenience builder for the Bool8 extension type,
// allowing arrays to be built with boolean values rather than the underlying storage type.
type Bool8Builder struct {
	*array.ExtensionBuilder
}

func NewBool8Builder(builder *array.ExtensionBuilder) *Bool8Builder {
	builder.Retain()
	return &Bool8Builder{ExtensionBuilder: builder}
}

func (b *Bool8Builder) Append(v bool) {
	b.ExtensionBuilder.Builder.(*array.Int8Builder).Append(boolToInt8(v))
}

func (b *Bool8Builder) UnsafeAppend(v bool) {
	b.ExtensionBuilder.Builder.(*array.Int8Builder).UnsafeAppend(boolToInt8(v))
}

func (b *Bool8Builder) AppendValueFromString(s string) error {
	if s == array.NullValueStr {
		b.AppendNull()
		return nil
	}

	val, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}

	b.Append(val)
	return nil
}

func (b *Bool8Builder) AppendValues(v []bool, valid []bool) {
	boolsAsBytes := arrow.GetBool8Bytes(v)
	boolsAsInt8s := arrow.GetData[int8](boolsAsBytes)
	b.ExtensionBuilder.Builder.(*array.Int8Builder).AppendValues(boolsAsInt8s, valid)
}

func (b *Bool8Builder) UnmarshalOne(dec *json.Decoder) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}

	switch v := t.(type) {
	case bool:
		b.Append(v)
		return nil
	case string:
		return b.AppendValueFromString(v)
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
}

func (b *Bool8Builder) Unmarshal(dec *json.Decoder) error {
	for dec.More() {
		if err := b.UnmarshalOne(dec); err != nil {
			return err
		}
	}
	return nil
}

var (
	_ arrow.ExtensionType           = (*Bool8Type)(nil)
	_ array.ExtensionBuilderWrapper = (*Bool8Type)(nil)
	_ array.ExtensionArray          = (*Bool8Array)(nil)
	_ array.Builder                 = (*Bool8Builder)(nil)
)

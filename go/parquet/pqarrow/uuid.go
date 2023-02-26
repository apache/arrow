package pqarrow

import (
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

// UUIDArray is a simple array which is a FixedSizeBinary(16)
type UUIDArray struct {
	array.ExtensionArrayBase
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


func (e UUIDType) String() string { return fmt.Sprintf("extension_type<storage=%s>", e.Storage) }

func (e UUIDType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"name":"%s","metadata":%s}`, e.ExtensionName(), e.Serialize())), nil
}
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
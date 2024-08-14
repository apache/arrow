// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extensions

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/internal/json"
)

var jsonSupportedStorageTypes = []arrow.DataType{
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.LargeString,
	arrow.BinaryTypes.StringView,
}

// JSONType represents a UTF-8 encoded JSON string as specified in RFC8259.
type JSONType struct {
	arrow.ExtensionBase
}

// NewJSONType creates a new JSONType with the specified storage type.
// storageType must be one of String, LargeString, StringView.
func NewJSONType(storageType arrow.DataType) (*JSONType, error) {
	if !slices.Contains(jsonSupportedStorageTypes, storageType) {
		return nil, fmt.Errorf("unsupported storage type for JSON extension type: %s", storageType)
	}
	return &JSONType{ExtensionBase: arrow.ExtensionBase{Storage: storageType}}, nil
}

func (b *JSONType) ArrayType() reflect.Type { return reflect.TypeOf(JSONArray{}) }

func (b *JSONType) Deserialize(storageType arrow.DataType, data string) (arrow.ExtensionType, error) {
	if !(data == "" || data == "{}") {
		return nil, fmt.Errorf("serialized metadata for JSON extension type must be '' or '{}', found: %s", data)
	}
	return NewJSONType(storageType)
}

func (b *JSONType) ExtensionEquals(other arrow.ExtensionType) bool {
	return b.ExtensionName() == other.ExtensionName() && arrow.TypeEqual(b.Storage, other.StorageType())
}

func (b *JSONType) ExtensionName() string { return "arrow.json" }

func (b *JSONType) Serialize() string { return "" }

func (b *JSONType) String() string {
	return fmt.Sprintf("extension<%s[storage_type=%s]>", b.ExtensionName(), b.Storage)
}

// JSONArray is logically an array of UTF-8 encoded JSON strings.
// Its values are unmarshaled to native Go values.
type JSONArray struct {
	array.ExtensionArrayBase
}

func (a *JSONArray) String() string {
	b, err := a.MarshalJSON()
	if err != nil {
		panic(fmt.Sprintf("failed marshal JSONArray: %s", err))
	}

	return string(b)
}

func (a *JSONArray) Value(i int) any {
	valString := a.ValueStr(i)

	var val any
	if err := json.Unmarshal([]byte(valString), &val); err != nil {
		panic(err)
	}

	return val
}

func (a *JSONArray) ValueStr(i int) string {
	if a.IsNull(i) {
		return "null"
	}

	switch storage := a.Storage().(type) {
	case *array.String:
		return storage.Value(i)
	case *array.LargeString:
		return storage.Value(i)
	case *array.StringView:
		return storage.Value(i)
	default:
		panic(fmt.Sprintf("invalid JSON extension array with storage type: %s", storage))
	}
}

func (a *JSONArray) MarshalJSON() ([]byte, error) {
	values := make([]interface{}, a.Len())
	for i := 0; i < a.Len(); i++ {
		if a.IsValid(i) {
			values[i] = a.Value(i)
		}
	}
	return json.Marshal(values)
}

func (a *JSONArray) GetOneForMarshal(i int) interface{} {
	if a.IsNull(i) {
		return nil
	}
	return a.Value(i)
}

var (
	_ arrow.ExtensionType  = (*JSONType)(nil)
	_ array.ExtensionArray = (*JSONArray)(nil)
)

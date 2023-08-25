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

//go:build go1.18

package exprs

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
)

type simpleExtensionTypeFactory[P comparable] struct {
	arrow.ExtensionBase

	params     P
	name       string
	getStorage func(P) arrow.DataType
}

func (ef *simpleExtensionTypeFactory[P]) String() string        { return "extension<" + ef.Serialize() + ">" }
func (ef *simpleExtensionTypeFactory[P]) ExtensionName() string { return ef.name }
func (ef *simpleExtensionTypeFactory[P]) Serialize() string {
	s, _ := json.Marshal(ef.params)
	return ef.name + string(s)
}
func (ef *simpleExtensionTypeFactory[P]) Deserialize(storage arrow.DataType, data string) (arrow.ExtensionType, error) {
	if !strings.HasPrefix(data, ef.name) {
		return nil, fmt.Errorf("%w: invalid deserialization of extension type %s", arrow.ErrInvalid, ef.name)
	}

	data = strings.TrimPrefix(data, ef.name)
	if err := json.Unmarshal([]byte(data), &ef.params); err != nil {
		return nil, fmt.Errorf("%w: failed parsing parameters for extension type", err)
	}

	if !arrow.TypeEqual(storage, ef.getStorage(ef.params)) {
		return nil, fmt.Errorf("%w: invalid storage type for %s: %s (expected: %s)",
			arrow.ErrInvalid, ef.name, storage, ef.getStorage(ef.params))
	}

	return &simpleExtensionTypeFactory[P]{
		name:       ef.name,
		params:     ef.params,
		getStorage: ef.getStorage,
		ExtensionBase: arrow.ExtensionBase{
			Storage: storage,
		},
	}, nil
}
func (ef *simpleExtensionTypeFactory[P]) ExtensionEquals(other arrow.ExtensionType) bool {
	if ef.name != other.ExtensionName() {
		return false
	}

	rhs := other.(*simpleExtensionTypeFactory[P])
	return ef.params == rhs.params
}
func (ef *simpleExtensionTypeFactory[P]) ArrayType() reflect.Type {
	return reflect.TypeOf(array.ExtensionArrayBase{})
}

func (ef *simpleExtensionTypeFactory[P]) CreateType(params P) arrow.DataType {
	storage := ef.getStorage(params)

	return &simpleExtensionTypeFactory[P]{
		name:       ef.name,
		params:     params,
		getStorage: ef.getStorage,
		ExtensionBase: arrow.ExtensionBase{
			Storage: storage,
		},
	}
}

type uuidExtParams struct{}

var uuidType = simpleExtensionTypeFactory[uuidExtParams]{
	name: "uuid", getStorage: func(uuidExtParams) arrow.DataType {
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}
	}}

type fixedCharExtensionParams struct {
	Length int32 `json:"length"`
}

var fixedCharType = simpleExtensionTypeFactory[fixedCharExtensionParams]{
	name: "fixed_char", getStorage: func(p fixedCharExtensionParams) arrow.DataType {
		return &arrow.FixedSizeBinaryType{ByteWidth: int(p.Length)}
	},
}

type varCharExtensionParams struct {
	Length int32 `json:"length"`
}

var varCharType = simpleExtensionTypeFactory[varCharExtensionParams]{
	name: "varchar", getStorage: func(varCharExtensionParams) arrow.DataType {
		return arrow.BinaryTypes.String
	},
}

type intervalYearExtensionParams struct{}

var intervalYearType = simpleExtensionTypeFactory[intervalYearExtensionParams]{
	name: "interval_year", getStorage: func(intervalYearExtensionParams) arrow.DataType {
		return arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32)
	},
}

type intervalDayExtensionParams struct{}

var intervalDayType = simpleExtensionTypeFactory[intervalDayExtensionParams]{
	name: "interval_day", getStorage: func(intervalDayExtensionParams) arrow.DataType {
		return arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32)
	},
}

func uuid() arrow.DataType { return uuidType.CreateType(uuidExtParams{}) }
func fixedChar(length int32) arrow.DataType {
	return fixedCharType.CreateType(fixedCharExtensionParams{Length: length})
}
func varChar(length int32) arrow.DataType {
	return varCharType.CreateType(varCharExtensionParams{Length: length})
}
func intervalYear() arrow.DataType {
	return intervalYearType.CreateType(intervalYearExtensionParams{})
}
func intervalDay() arrow.DataType {
	return intervalDayType.CreateType(intervalDayExtensionParams{})
}

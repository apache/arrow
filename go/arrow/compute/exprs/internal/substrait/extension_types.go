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

package substrait

import (
	"github.com/apache/arrow/go/v12/arrow"
)

type uuidExtParams struct{}

var uuidType = SimpleExtensionTypeFactory[uuidExtParams]{
	name: "uuid", getStorage: func(uuidExtParams) arrow.DataType {
		return &arrow.FixedSizeBinaryType{ByteWidth: 16}
	}}

type fixedCharExtensionParams struct {
	Length int32 `json:"length"`
}

var fixedCharType = SimpleExtensionTypeFactory[fixedCharExtensionParams]{
	name: "fixed_char", getStorage: func(p fixedCharExtensionParams) arrow.DataType {
		return &arrow.FixedSizeBinaryType{ByteWidth: int(p.Length)}
	},
}

type varCharExtensionParams struct {
	Length int32 `json:"length"`
}

var varCharType = SimpleExtensionTypeFactory[varCharExtensionParams]{
	name: "varchar", getStorage: func(varCharExtensionParams) arrow.DataType {
		return arrow.BinaryTypes.String
	},
}

type intervalYearExtensionParams struct{}

var intervalYearType = SimpleExtensionTypeFactory[intervalYearExtensionParams]{
	name: "interval_year", getStorage: func(intervalYearExtensionParams) arrow.DataType {
		return arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32)
	},
}

type intervalDayExtensionParams struct{}

var intervalDayType = SimpleExtensionTypeFactory[intervalDayExtensionParams]{
	name: "interval_day", getStorage: func(intervalDayExtensionParams) arrow.DataType {
		return arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Int32)
	},
}

func Uuid() arrow.DataType { return uuidType.CreateType(uuidExtParams{}) }
func FixedChar(length int32) arrow.DataType {
	return fixedCharType.CreateType(fixedCharExtensionParams{Length: length})
}
func VarChar(length int32) arrow.DataType {
	return varCharType.CreateType(varCharExtensionParams{Length: length})
}
func IntervalYear() arrow.DataType {
	return intervalYearType.CreateType(intervalYearExtensionParams{})
}
func IntervalDay() arrow.DataType {
	return intervalDayType.CreateType(intervalDayExtensionParams{})
}

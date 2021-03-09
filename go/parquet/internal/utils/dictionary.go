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

package utils

import (
	"math"
	"reflect"
)

type IndexType int32

const (
	MaxIndexType = math.MaxInt32
	MinIndexType = math.MinInt32
)

type DictionaryConverter interface {
	Copy(interface{}, []IndexType) error
	Fill(interface{}, IndexType) error
	FillZero(interface{})
	IsValid(...IndexType) bool
}

// converter for getspaced that handles runs that get returned directly
// as output, rather than using a dictionary
type plainConverter struct{}

func (plainConverter) IsValid(...IndexType) bool { return true }
func (plainConverter) Fill(values interface{}, val IndexType) error {
	v := reflect.ValueOf(values)
	switch v.Type().Elem().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.Index(0).SetInt(int64(val))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.Index(0).SetUint(uint64(val))
	}

	for i := 1; i < v.Len(); i *= 2 {
		reflect.Copy(v.Slice(i, v.Len()), v.Slice(0, i))
	}
	return nil
}

func (plainConverter) FillZero(values interface{}) {
	v := reflect.ValueOf(values)
	zeroVal := reflect.New(v.Type().Elem()).Elem()

	v.Index(0).Set(zeroVal)
	for i := 1; i < v.Len(); i *= 2 {
		reflect.Copy(v.Slice(i, v.Len()), v.Slice(0, i))
	}
}

func (plainConverter) Copy(out interface{}, values []IndexType) error {
	vout := reflect.ValueOf(out)
	vin := reflect.ValueOf(values)
	for i := 0; i < vin.Len(); i++ {
		vout.Index(i).Set(vin.Index(i).Convert(vout.Type().Elem()))
	}
	return nil
}

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

package avro

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/apache/arrow/go/v13/arrow/decimal256"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/internal/types"
)

type FieldPos struct {
	parent       *FieldPos
	name         string
	path         []string
	typeName     string
	AppendFunc   func(val interface{}) error
	metadata     arrow.Metadata
	children     []*FieldPos
	index, depth int32
}

func NewFieldPos() *FieldPos { return &FieldPos{index: -1} }

func (f *FieldPos) Name() string { return f.name }

func (f *FieldPos) Child(index int) (*FieldPos, error) {
	if index < len(f.Children()) {
		return f.children[index], nil
	}
	return nil, fmt.Errorf("%v child index %d not found", f.NamePath(), index)
}

func (f *FieldPos) Children() []*FieldPos { return f.children }

func (f *FieldPos) Metadata() arrow.Metadata { return f.metadata }

func (f *FieldPos) NewChild(childName string, childBuilder array.Builder, meta arrow.Metadata) *FieldPos {
	var child FieldPos = FieldPos{
		parent:   f,
		name:     childName,
		metadata: meta,
		index:    int32(len(f.children)),
		depth:    f.depth + 1,
	}
	child.path = child.NamePath()
	f.children = append(f.children, &child)
	return &child
}

// NamePath returns a slice of keys making up the path to the field
func (f *FieldPos) NamePath() []string {
	if len(f.path) == 0 {
		var path []string
		cur := f
		for i := f.depth - 1; i >= 0; i-- {
			if cur.typeName == "" {
				path = append([]string{cur.name}, path...)
			} else {
				path = append([]string{cur.name, cur.typeName}, path...)
			}
			cur = cur.parent
		}
		return path
	}
	return f.path
}

// GetValue retrieves the value from the map[string]interface{}
// by following the field's key path
func (f *FieldPos) GetValue(m map[string]interface{}) interface{} {
	var value interface{} = m
	for _, key := range f.NamePath() {
		valueMap, ok := value.(map[string]interface{})
		if !ok {
			return nil
		}
		value, ok = valueMap[key]
		if !ok {
			return nil
		}
	}
	return value
}

// Avro data is loaded to Arrow arrays using the following type mapping:
//
//	Avro					Go    			Arrow
//	null					nil				Null
//	boolean					bool			Boolean
//	bytes					[]byte			Binary
//	float					float32			Float32
//	double					float64			Float64
//	long					int64			Int64
//	int						int32  			Int32
//	string					string			String
//	array					[]interface{}	List
//	enum					string			Dictionary
//	fixed					[]byte			FixedSizeBinary
//	map and record	map[string]interface{}	Struct

func mapFieldBuilders(b array.Builder, field arrow.Field, parent *FieldPos) {
	switch bt := b.(type) {
	case *array.BinaryBuilder:
		f := parent.NewChild(field.Name, bt, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendBinaryData(bt, data)
			return nil
		}
	case *array.BinaryDictionaryBuilder:
		// has metadata for Avro enum symbols
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendBinaryDictData(bt, data)
			return nil
		}
		// add Avro enum symbols to builder
		sb := array.NewStringBuilder(memory.DefaultAllocator)
		for _, v := range field.Metadata.Values() {
			sb.Append(v)
		}
		sa := sb.NewStringArray()
		bt.InsertStringDictValues(sa)
	case *array.BooleanBuilder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendBoolData(bt, data)
			return nil
		}
	case *array.Date32Builder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendDate32Data(bt, data)
			return nil
		}
	case *array.Decimal128Builder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			err := appendDecimal128Data(bt, data)
			if err != nil {
				return err
			}
			return nil
		}
	case *array.Decimal256Builder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			err := appendDecimal256Data(bt, data)
			if err != nil {
				return err
			}
			return nil
		}
	case *types.UUIDBuilder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			switch dt := data.(type) {
			case nil:
				bt.AppendNull()
			case string:
				err := bt.AppendValueFromString(dt)
				if err != nil {
					return err
				}
			case []byte:
				err := bt.AppendValueFromString(string(dt))
				if err != nil {
					return err
				}
			}

			return nil
		}
	case *array.FixedSizeBinaryBuilder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendFixedSizeBinaryData(bt, data)
			return nil
		}
	case *array.Float32Builder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendFloat32Data(bt, data)
			return nil
		}
	case *array.Float64Builder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendFloat64Data(bt, data)
			return nil
		}
	case *array.Int32Builder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendInt32Data(bt, data)
			return nil
		}
	case *array.Int64Builder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendInt64Data(bt, data)
			return nil
		}
	case *array.ListBuilder:
		lf := parent.NewChild(field.Name, b, field.Metadata)
		vb := bt.ValueBuilder()
		mapFieldBuilders(vb, field.Type.(*arrow.ListType).ElemField(), lf)
		lf.AppendFunc = func(data interface{}) error {
			switch dt := data.(type) {
			case nil:
				bt.AppendNull()
			case []interface{}:
				if len(dt) == 0 {
					bt.AppendEmptyValue()
				} else {
					bt.Append(true)
				}
			default:
				bt.Append(true)
			}
			return nil
		}
	case *array.MapBuilder:
		// has metadata for objects in values
		mf := parent.NewChild(field.Name, b, field.Metadata)
		ib := bt.ItemBuilder()
		mapFieldBuilders(ib, field.Type.(*arrow.MapType).ItemField(), mf)
		mf.AppendFunc = func(data interface{}) error {
			switch data.(type) {
			case nil:
				bt.AppendNull()
			default:
				bt.Append(true)
				switch d := mf.GetValue(data.(map[string]interface{})).(type) {
				case nil:
					bt.KeyBuilder().AppendNull()
				case map[string]interface{}:
					for k, v := range d {
						bt.KeyBuilder().(*array.StringBuilder).Append(k)
						c, err := mf.Child(0)
						if err != nil {
							return err
						}
						err = c.AppendFunc(v)
						if err != nil {
							return err
						}
					}
				}
			}
			return nil
		}
	case *array.MonthDayNanoIntervalBuilder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendDurationData(bt, data)
			return nil
		}
	case *array.StringBuilder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendStringData(bt, data)
			return nil
		}
	case *array.StructBuilder:
		// has metadata for Avro Union named types
		sf := parent.NewChild(field.Name, b, field.Metadata)
		sf.typeName, _ = field.Metadata.GetValue("typeName")
		// create children
		for i, f := range field.Type.(*arrow.StructType).Fields() {
			mapFieldBuilders(bt.FieldBuilder(i), f, sf)
		}
		sf.AppendFunc = func(data interface{}) error {
			switch data.(type) {
			case nil:
				bt.AppendNull()
			default:
				appendStructData(bt, data)
			}
			return nil
		}
	case *array.Time32Builder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendTime32Data(bt, data)
			return nil
		}
	case *array.Time64Builder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendTime64Data(bt, data)
			return nil
		}
	case *array.TimestampBuilder:
		f := parent.NewChild(field.Name, b, field.Metadata)
		f.AppendFunc = func(data interface{}) error {
			appendTimestampData(bt, data)
			return nil
		}
	}
}

func appendBinaryData(b *array.BinaryBuilder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case map[string]interface{}:
		switch ct := dt["bytes"].(type) {
		case nil:
			b.AppendNull()
		default:
			b.Append(ct.([]byte))
		}
	default:
		b.Append([]byte(fmt.Sprint(data)))
	}
}

func appendBinaryDictData(b *array.BinaryDictionaryBuilder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case string:
		b.AppendString(dt)
	case map[string]interface{}:
		switch dt["string"].(type) {
		case nil:
			b.AppendNull()
		case string:
			b.AppendString(dt["string"].(string))
		}
	}
}

func appendBoolData(b *array.BooleanBuilder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case bool:
		b.Append(dt)
	case map[string]interface{}:
		switch dt["boolean"].(type) {
		case nil:
			b.AppendNull()
		case bool:
			b.Append(dt["boolean"].(bool))
		}
	}
}

func appendDate32Data(b *array.Date32Builder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int32:
		b.Append(arrow.Date32(dt))
	case map[string]interface{}:
		switch dt["int"].(type) {
		case nil:
			b.AppendNull()
		case int32:
			b.Append(arrow.Date32(dt["int"].(int32)))
		}
	}
}

func appendDecimal128Data(b *array.Decimal128Builder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		if len(dt) <= 38 {
			var intData int64
			buf := bytes.NewBuffer(dt)
			err := binary.Read(buf, binary.BigEndian, &intData)
			if err != nil {
				return err
			}
			b.Append(decimal128.FromI64(intData))
		} else {
			var bigIntData big.Int
			buf := bytes.NewBuffer(dt)
			err := binary.Read(buf, binary.BigEndian, &bigIntData)
			if err != nil {
				return err
			}
			b.Append(decimal128.FromBigInt(&bigIntData))
		}
	case map[string]interface{}:
		if len(dt["bytes"].([]byte)) <= 38 {
			var intData int64
			buf := bytes.NewBuffer(dt["bytes"].([]byte))
			err := binary.Read(buf, binary.BigEndian, &intData)
			if err != nil {
				return err
			}
			b.Append(decimal128.FromI64(intData))
		} else {
			var bigIntData big.Int
			buf := bytes.NewBuffer(dt["bytes"].([]byte))
			err := binary.Read(buf, binary.BigEndian, &bigIntData)
			if err != nil {
				return err
			}
			b.Append(decimal128.FromBigInt(&bigIntData))
		}
	}
	return nil
}

func appendDecimal256Data(b *array.Decimal256Builder, data interface{}) error {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		var bigIntData big.Int
		buf := bytes.NewBuffer(dt)
		err := binary.Read(buf, binary.BigEndian, &bigIntData)
		if err != nil {
			return err
		}
		b.Append(decimal256.FromBigInt(&bigIntData))
	case map[string]interface{}:
		var bigIntData big.Int
		buf := bytes.NewBuffer(dt["bytes"].([]byte))
		err := binary.Read(buf, binary.BigEndian, &bigIntData)
		if err != nil {
			return err
		}
		b.Append(decimal256.FromBigInt(&bigIntData))
	}
	return nil
}

func appendDurationData(b *array.MonthDayNanoIntervalBuilder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		dur := new(arrow.MonthDayNanoInterval)
		dur.Months = int32(binary.BigEndian.Uint64(dt[:3]))
		dur.Days = int32(binary.BigEndian.Uint64(dt[4:7]))
		dur.Nanoseconds = int64(binary.BigEndian.Uint64(dt[8:]) * 1000000)
		b.Append(*dur)
	case map[string]interface{}:
		switch dtb := dt["bytes"].(type) {
		case nil:
			b.AppendNull()
		case []byte:
			dur := new(arrow.MonthDayNanoInterval)
			dur.Months = int32(binary.BigEndian.Uint64(dtb[:3]))
			dur.Days = int32(binary.BigEndian.Uint64(dtb[4:7]))
			dur.Nanoseconds = int64(binary.BigEndian.Uint64(dtb[8:]) * 1000000)
			b.Append(*dur)
		}
	}
}

func appendFixedSizeBinaryData(b *array.FixedSizeBinaryBuilder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case []byte:
		b.Append(dt)
	case map[string]interface{}:
		switch dt["bytes"].(type) {
		case nil:
			b.AppendNull()
		case []byte:
			b.Append(dt["bytes"].([]byte))
		}
	}
}

func appendFloat32Data(b *array.Float32Builder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case float32:
		b.Append(dt)
	case map[string]interface{}:
		switch dt["float"].(type) {
		case nil:
			b.AppendNull()
		case float32:
			b.Append(dt["float"].(float32))
		}
	}
}

func appendFloat64Data(b *array.Float64Builder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case float64:
		b.Append(dt)
	case map[string]interface{}:
		switch dt["double"].(type) {
		case nil:
			b.AppendNull()
		case float64:
			b.Append(dt["double"].(float64))
		}
	}
}

func appendInt32Data(b *array.Int32Builder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int:
		b.Append(int32(dt))
	case int32:
		b.Append(dt)
	case map[string]interface{}:
		switch dt["int"].(type) {
		case nil:
			b.AppendNull()
		case int:
			b.Append(int32(dt["int"].(int)))
		case int32:
			b.Append(dt["int"].(int32))
		}
	}
}

func appendInt64Data(b *array.Int64Builder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int:
		b.Append(int64(dt))
	case int64:
		b.Append(dt)
	case map[string]interface{}:
		switch dt["long"].(type) {
		case nil:
			b.AppendNull()
		case int:
			b.Append(int64(dt["long"].(int)))
		case int64:
			b.Append(dt["long"].(int64))
		}
	}
}

func appendStringData(b *array.StringBuilder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case string:
		b.Append(dt)
	case map[string]interface{}:
		switch dt["string"].(type) {
		case nil:
			b.AppendNull()
		case string:
			b.Append(dt["string"].(string))
		}
	default:
		b.Append(fmt.Sprint(data))
	}
}

func appendTime32Data(b *array.Time32Builder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int32:
		b.Append(arrow.Time32(dt))
	case map[string]interface{}:
		switch dt["int"].(type) {
		case nil:
			b.AppendNull()
		case int32:
			b.Append(arrow.Time32(dt["int"].(int32)))
		}
	}
}

func appendTime64Data(b *array.Time64Builder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int64:
		b.Append(arrow.Time64(dt))
	case map[string]interface{}:
		switch dt["long"].(type) {
		case nil:
			b.AppendNull()
		case int64:
			b.Append(arrow.Time64(dt["long"].(int64)))
		}
	}
}

func appendTimestampData(b *array.TimestampBuilder, data interface{}) {
	switch dt := data.(type) {
	case nil:
		b.AppendNull()
	case int64:
		b.Append(arrow.Timestamp(dt))
	case map[string]interface{}:
		switch dt["long"].(type) {
		case nil:
			b.AppendNull()
		case int64:
			b.Append(arrow.Timestamp(dt["long"].(int64)))
		}
	}
}

func appendStructData(b *array.StructBuilder, data interface{}) {
	switch data.(type) {
	case nil:
		b.AppendNull()
	default:
		b.Append(true)
	}
}

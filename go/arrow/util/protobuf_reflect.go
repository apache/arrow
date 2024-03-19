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

package util

import (
	"fmt"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"reflect"
)

type SchemaOptions struct {
	exclusionPolicy    func(pfr ProtobufFieldReflection) bool
	fieldNameFormatter func(str string) string
}

type ProtobufStructReflection struct {
	descriptor protoreflect.MessageDescriptor
	message    protoreflect.Message
	rValue     reflect.Value
	SchemaOptions
}

type Option func(*ProtobufStructReflection)

func NewProtobufStructReflection(msg proto.Message, options ...Option) *ProtobufStructReflection {
	v := reflect.ValueOf(msg)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	includeAll := func(pfr ProtobufFieldReflection) bool {
		return false
	}
	noFormatting := func(str string) string {
		return str
	}
	psr := &ProtobufStructReflection{
		descriptor: msg.ProtoReflect().Descriptor(),
		message:    msg.ProtoReflect(),
		rValue:     v,
		SchemaOptions: SchemaOptions{
			exclusionPolicy:    includeAll,
			fieldNameFormatter: noFormatting,
		},
	}

	for _, opt := range options {
		opt(psr)
	}

	return psr
}

func WithExclusionPolicy(ex func(pfr ProtobufFieldReflection) bool) Option {
	return func(psr *ProtobufStructReflection) {
		psr.exclusionPolicy = ex
	}
}

func WithFieldNameFormatter(formatter func(str string) string) Option {
	return func(psr *ProtobufStructReflection) {
		psr.fieldNameFormatter = formatter
	}
}

func (psr ProtobufStructReflection) unmarshallAny() ProtobufStructReflection {
	if psr.descriptor.FullName() == "google.protobuf.Any" {
		for psr.rValue.Type().Kind() == reflect.Ptr {
			psr.rValue = reflect.Indirect(psr.rValue)
		}
		fieldValueAsAny, _ := psr.rValue.Interface().(anypb.Any)
		msg, _ := fieldValueAsAny.UnmarshalNew()

		v := reflect.ValueOf(msg)
		for v.Kind() == reflect.Ptr {
			v = reflect.Indirect(v)
		}

		return ProtobufStructReflection{
			descriptor:    msg.ProtoReflect().Descriptor(),
			message:       msg.ProtoReflect(),
			rValue:        v,
			SchemaOptions: psr.SchemaOptions,
		}
	} else {
		return psr
	}
}

func (psr ProtobufStructReflection) GetArrowFields() []arrow.Field {
	var fields []arrow.Field

	for pfr := range psr.generateStructFields() {
		fields = append(fields, arrow.Field{
			Name:     psr.fieldNameFormatter(string(pfr.descriptor.Name())),
			Type:     pfr.getDataType(),
			Nullable: true,
		})
	}

	return fields
}

func (psr ProtobufStructReflection) GetSchema() *arrow.Schema {
	return arrow.NewSchema(psr.GetArrowFields(), nil)
}

type ProtobufListReflection struct {
	ProtobufFieldReflection
}

func (pfr ProtobufFieldReflection) AsList() ProtobufListReflection {
	return ProtobufListReflection{pfr}
}

func (plr ProtobufListReflection) getDataType() arrow.DataType {
	for li := range plr.generateListItems() {
		return arrow.ListOf(li.getDataType())
	}
	return nil
}

func (pfr ProtobufFieldReflection) AsMap() ProtobufMapReflection {
	return ProtobufMapReflection{pfr}
}

type ProtobufMapReflection struct {
	ProtobufFieldReflection
}

func (pmr ProtobufMapReflection) getDataType() arrow.DataType {
	for kvp := range pmr.generateKeyValuePairs() {
		return kvp.getDataType()
	}
	return nil
}

type ProtobufMapKeyValuePairReflection struct {
	k ProtobufFieldReflection
	v ProtobufFieldReflection
}

func (pmr ProtobufMapKeyValuePairReflection) getDataType() arrow.DataType {
	return arrow.MapOf(pmr.k.getDataType(), pmr.v.getDataType())
}

func (pmr ProtobufMapReflection) generateKeyValuePairs() chan ProtobufMapKeyValuePairReflection {
	out := make(chan ProtobufMapKeyValuePairReflection)

	go func() {
		defer close(out)
		for _, k := range pmr.rValue.MapKeys() {
			kvp := ProtobufMapKeyValuePairReflection{
				k: ProtobufFieldReflection{
					descriptor:    pmr.descriptor.MapKey(),
					prValue:       getMapKey(k),
					rValue:        k,
					SchemaOptions: pmr.SchemaOptions,
				},
				v: ProtobufFieldReflection{
					descriptor:    pmr.descriptor.MapValue(),
					prValue:       pmr.prValue.Map().Get(protoreflect.MapKey(getMapKey(k))),
					rValue:        pmr.rValue.MapIndex(k),
					SchemaOptions: pmr.SchemaOptions,
				},
			}
			out <- kvp
		}
	}()

	return out
}

func (psr ProtobufStructReflection) generateStructFields() chan ProtobufFieldReflection {
	out := make(chan ProtobufFieldReflection)

	go func() {
		defer close(out)
		fds := psr.descriptor.Fields()
		for i := 0; i < fds.Len(); i++ {
			pfr := psr.getFieldByName(string(fds.Get(i).Name()))
			if psr.exclusionPolicy(pfr) {
				continue
			}
			out <- pfr
		}
	}()

	return out
}

func (pfr ProtobufFieldReflection) AsStruct() ProtobufStructReflection {
	psr := ProtobufStructReflection{
		descriptor:    pfr.descriptor.Message(),
		message:       pfr.prValue.Message(),
		rValue:        pfr.rValue,
		SchemaOptions: pfr.SchemaOptions,
	}
	psr = psr.unmarshallAny()
	return psr
}

func (psr ProtobufStructReflection) getDataType() arrow.DataType {
	return arrow.StructOf(psr.GetArrowFields()...)
}

func (psr ProtobufStructReflection) getFieldByName(n string) ProtobufFieldReflection {
	fd := psr.descriptor.Fields().ByTextName(xstrings.ToSnakeCase(n))
	fv := psr.rValue
	if fv.IsValid() {
		if !fv.IsZero() {
			for fv.Kind() == reflect.Ptr || fv.Kind() == reflect.Interface {
				fv = fv.Elem()
			}
			if fd.ContainingOneof() != nil {
				n = string(fd.ContainingOneof().Name())
			}
			fv = fv.FieldByName(xstrings.ToCamelCase(n))
			for fv.Kind() == reflect.Ptr {
				fv = fv.Elem()
			}
		}
	}
	return ProtobufFieldReflection{
		fd,
		psr.message.Get(fd),
		fv,
		psr.SchemaOptions,
	}
}

type ProtobufFieldReflection struct {
	descriptor protoreflect.FieldDescriptor
	prValue    protoreflect.Value
	rValue     reflect.Value
	SchemaOptions
}

func (pfr ProtobufFieldReflection) isStruct() bool {
	return pfr.descriptor.Kind() == protoreflect.MessageKind && !pfr.descriptor.IsMap() && pfr.rValue.Kind() != reflect.Slice
}

func (pfr ProtobufFieldReflection) isMap() bool {
	return pfr.descriptor.Kind() == protoreflect.MessageKind && pfr.descriptor.IsMap()
}

func (pfr ProtobufFieldReflection) isList() bool {
	return pfr.descriptor.IsList() && pfr.rValue.Kind() == reflect.Slice
}

func (pfr ProtobufFieldReflection) getListLength() int {
	return pfr.prValue.List().Len()
}

func (pfr ProtobufFieldReflection) getMapLength() int {
	return pfr.prValue.Map().Len()
}

func (plr ProtobufListReflection) generateListItems() chan ProtobufFieldReflection {
	out := make(chan ProtobufFieldReflection)

	go func() {
		defer close(out)
		for i := 0; i < plr.prValue.List().Len(); i++ {
			out <- ProtobufFieldReflection{
				descriptor:    plr.descriptor,
				prValue:       plr.prValue.List().Get(i),
				rValue:        plr.rValue.Index(i),
				SchemaOptions: plr.SchemaOptions,
			}
		}
	}()

	return out
}

func (pfr ProtobufFieldReflection) getDataType() arrow.DataType {
	var dt arrow.DataType

	typeMap := map[protoreflect.Kind]arrow.DataType{
		//Numeric
		protoreflect.Int32Kind:    arrow.PrimitiveTypes.Int32,
		protoreflect.Int64Kind:    arrow.PrimitiveTypes.Int64,
		protoreflect.Sint32Kind:   arrow.PrimitiveTypes.Int32,
		protoreflect.Sint64Kind:   arrow.PrimitiveTypes.Int64,
		protoreflect.Uint32Kind:   arrow.PrimitiveTypes.Uint32,
		protoreflect.Uint64Kind:   arrow.PrimitiveTypes.Uint64,
		protoreflect.Fixed32Kind:  arrow.PrimitiveTypes.Uint32,
		protoreflect.Fixed64Kind:  arrow.PrimitiveTypes.Uint64,
		protoreflect.Sfixed32Kind: arrow.PrimitiveTypes.Int32,
		protoreflect.Sfixed64Kind: arrow.PrimitiveTypes.Int64,
		protoreflect.FloatKind:    arrow.PrimitiveTypes.Float32,
		protoreflect.DoubleKind:   arrow.PrimitiveTypes.Float64,
		//Binary
		protoreflect.StringKind: arrow.BinaryTypes.String,
		protoreflect.BytesKind:  arrow.BinaryTypes.Binary,
		//Fixed Width
		protoreflect.BoolKind: arrow.FixedWidthTypes.Boolean,
		// Enum
		protoreflect.EnumKind: arrow.PrimitiveTypes.Int32,
		// Struct
		protoreflect.MessageKind: nil,
	}
	dt = typeMap[pfr.descriptor.Kind()]

	if pfr.isStruct() {
		dt = pfr.AsStruct().getDataType()
	}

	if pfr.isMap() {
		dt = pfr.AsMap().getDataType()
	}

	if pfr.isList() {
		dt = pfr.AsList().getDataType()
	}
	return dt
}

func getBuilders(s *arrow.Schema, m memory.Allocator) []array.Builder {
	var builders []array.Builder

	for _, f := range s.Fields() {
		builders = append(builders, array.NewBuilder(m, f.Type))
	}
	return builders
}

func RecordFromProtobuf(psr ProtobufStructReflection, schema *arrow.Schema) arrow.Record {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	bldrs := getBuilders(schema, mem)

	var fieldNames []string
	for i, field := range schema.Fields() {
		AppendValueOrNull(bldrs[i], psr.getFieldByName(field.Name), field, mem)
		fieldNames = append(fieldNames, field.Name)
	}

	var arrays []arrow.Array
	for _, bldr := range bldrs {
		a := bldr.NewArray()
		arrays = append(arrays, a)
	}

	structArray, _ := array.NewStructArray(arrays, fieldNames)

	return array.RecordFromStructArray(structArray, schema)
}

func AppendValueOrNull(b array.Builder, pfr ProtobufFieldReflection, f arrow.Field, mem memory.Allocator) {
	v := pfr.rValue
	pv := pfr.prValue
	fd := pfr.descriptor

	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			b.AppendNull()
			return
		}
		v = v.Elem()
	}

	if !v.IsValid() && !pv.IsValid() {
		b.AppendNull()
		return
	}

	switch b.Type().ID() {
	case arrow.STRING:
		b.(*array.StringBuilder).Append(pv.String())
	case arrow.BINARY:
		b.(*array.BinaryBuilder).Append(pv.Bytes())
	case arrow.INT32:
		switch fd.Kind() {
		case protoreflect.EnumKind:
			b.(*array.Int32Builder).Append(int32(pv.Enum()))
		default:
			b.(*array.Int32Builder).Append(int32(pv.Int()))
		}
	case arrow.INT64:
		b.(*array.Int64Builder).Append(pv.Int())
	case arrow.FLOAT64:
		b.(*array.Float64Builder).Append(pv.Float())
	case arrow.UINT32:
		b.(*array.Uint32Builder).Append(uint32(pv.Uint()))
	case arrow.UINT64:
		b.(*array.Uint64Builder).Append(pv.Uint())
	case arrow.BOOL:
		b.(*array.BooleanBuilder).Append(pv.Bool())
	case arrow.STRUCT:
		psr := pfr.AsStruct()
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		for i, field := range f.Type.(*arrow.StructType).Fields() {
			AppendValueOrNull(sb.FieldBuilder(i), psr.getFieldByName(field.Name), field, mem)
		}
	case arrow.LIST:
		lb := b.(*array.ListBuilder)
		l := pfr.getListLength()
		if l == 0 {
			lb.AppendEmptyValue()
			break
		}
		lb.ValueBuilder().Reserve(l)
		lb.Append(true)
		field := f.Type.(*arrow.ListType).ElemField()
		for li := range pfr.AsList().generateListItems() {
			AppendValueOrNull(lb.ValueBuilder(), li, field, mem)
		}
	case arrow.MAP:
		mb := b.(*array.MapBuilder)
		l := pfr.getMapLength()
		if l == 0 {
			mb.AppendEmptyValue()
			break
		}
		mb.KeyBuilder().Reserve(l)
		mb.ItemBuilder().Reserve(l)
		mb.Append(true)

		for kvp := range pfr.AsMap().generateKeyValuePairs() {
			AppendValueOrNull(mb.KeyBuilder(), kvp.k, f.Type.(*arrow.MapType).KeyField(), mem)
			AppendValueOrNull(mb.ItemBuilder(), kvp.v, f.Type.(*arrow.MapType).ItemField(), mem)
		}
	default:
		fmt.Printf("No logic for type %s", b.Type().ID())
	}

}

func getMapKey(v reflect.Value) protoreflect.Value {
	switch v.Kind() {
	case reflect.String:
		return protoreflect.ValueOf(v.String())
	case reflect.Int32, reflect.Int64:
		return protoreflect.ValueOf(v.Int())
	case reflect.Bool:
		return protoreflect.ValueOf(v.Bool())
	case reflect.Uint32, reflect.Uint64:
		return protoreflect.ValueOf(v.Uint())
	default:
		panic("Unmapped protoreflect map key type")
	}
}

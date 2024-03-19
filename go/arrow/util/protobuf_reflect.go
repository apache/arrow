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

type schemaOptions struct {
	exclusionPolicy    func(pfr protobufFieldReflection) bool
	fieldNameFormatter func(str string) string
}

// ProtobufStructReflection represents the metadata and values of a protobuf message
type ProtobufStructReflection struct {
	descriptor protoreflect.MessageDescriptor
	message    protoreflect.Message
	rValue     reflect.Value
	schemaOptions
}

type option func(*ProtobufStructReflection)

// NewProtobufStructReflection constructs a ProtobufStructReflection struct for a protobuf message
func NewProtobufStructReflection(msg proto.Message, options ...option) *ProtobufStructReflection {
	v := reflect.ValueOf(msg)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	includeAll := func(pfr protobufFieldReflection) bool {
		return false
	}
	noFormatting := func(str string) string {
		return str
	}
	psr := &ProtobufStructReflection{
		descriptor: msg.ProtoReflect().Descriptor(),
		message:    msg.ProtoReflect(),
		rValue:     v,
		schemaOptions: schemaOptions{
			exclusionPolicy:    includeAll,
			fieldNameFormatter: noFormatting,
		},
	}

	for _, opt := range options {
		opt(psr)
	}

	return psr
}

// WithExclusionPolicy is an option for a ProtobufStructReflection
// WithExclusionPolicy acts as a deny filter on the fields of a protobuf message
// to prevent them from being included in the schema.
// A use case for this is to exclude fields containing PII.
func WithExclusionPolicy(ex func(pfr protobufFieldReflection) bool) option {
	return func(psr *ProtobufStructReflection) {
		psr.exclusionPolicy = ex
	}
}

// WithFieldNameFormatter is an option for a ProtobufStructReflection
// WithFieldNameFormatter enables customisation of the field names in the arrow schema
// By default, the field names are taken from the protobuf message (.proto file)
func WithFieldNameFormatter(formatter func(str string) string) option {
	return func(psr *ProtobufStructReflection) {
		psr.fieldNameFormatter = formatter
	}
}

// GetSchema returns an arrow schema representing a protobuf message
func (psr ProtobufStructReflection) GetSchema() *arrow.Schema {
	return arrow.NewSchema(psr.getArrowFields(), nil)
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
			schemaOptions: psr.schemaOptions,
		}
	} else {
		return psr
	}
}

func (psr ProtobufStructReflection) getArrowFields() []arrow.Field {
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

type protobufListReflection struct {
	protobufFieldReflection
}

func (pfr protobufFieldReflection) asList() protobufListReflection {
	return protobufListReflection{pfr}
}

func (plr protobufListReflection) getDataType() arrow.DataType {
	for li := range plr.generateListItems() {
		return arrow.ListOf(li.getDataType())
	}
	return nil
}

type protobufDictReflection struct {
	protobufFieldReflection
}

func (pfr protobufFieldReflection) asDictionary() protobufDictReflection {
	return protobufDictReflection{pfr}
}

func (pdr protobufDictReflection) getDataType() arrow.DataType {
	return &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
		Ordered:   false,
	}
}

type protobufMapReflection struct {
	protobufFieldReflection
}

func (pfr protobufFieldReflection) asMap() protobufMapReflection {
	return protobufMapReflection{pfr}
}

func (pmr protobufMapReflection) getDataType() arrow.DataType {
	for kvp := range pmr.generateKeyValuePairs() {
		return kvp.getDataType()
	}
	return nil
}

type protobufMapKeyValuePairReflection struct {
	k protobufFieldReflection
	v protobufFieldReflection
}

func (pmr protobufMapKeyValuePairReflection) getDataType() arrow.DataType {
	return arrow.MapOf(pmr.k.getDataType(), pmr.v.getDataType())
}

func (pmr protobufMapReflection) generateKeyValuePairs() chan protobufMapKeyValuePairReflection {
	out := make(chan protobufMapKeyValuePairReflection)

	go func() {
		defer close(out)
		for _, k := range pmr.rValue.MapKeys() {
			kvp := protobufMapKeyValuePairReflection{
				k: protobufFieldReflection{
					descriptor:    pmr.descriptor.MapKey(),
					prValue:       getMapKey(k),
					rValue:        k,
					schemaOptions: pmr.schemaOptions,
				},
				v: protobufFieldReflection{
					descriptor:    pmr.descriptor.MapValue(),
					prValue:       pmr.prValue.Map().Get(protoreflect.MapKey(getMapKey(k))),
					rValue:        pmr.rValue.MapIndex(k),
					schemaOptions: pmr.schemaOptions,
				},
			}
			out <- kvp
		}
	}()

	return out
}

func (psr ProtobufStructReflection) generateStructFields() chan protobufFieldReflection {
	out := make(chan protobufFieldReflection)

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

func (pfr protobufFieldReflection) asStruct() ProtobufStructReflection {
	psr := ProtobufStructReflection{
		descriptor:    pfr.descriptor.Message(),
		message:       pfr.prValue.Message(),
		rValue:        pfr.rValue,
		schemaOptions: pfr.schemaOptions,
	}
	psr = psr.unmarshallAny()
	return psr
}

func (psr ProtobufStructReflection) getDataType() arrow.DataType {
	return arrow.StructOf(psr.getArrowFields()...)
}

func (psr ProtobufStructReflection) getFieldByName(n string) protobufFieldReflection {
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
	return protobufFieldReflection{
		fd,
		psr.message.Get(fd),
		fv,
		psr.schemaOptions,
	}
}

type protobufFieldReflection struct {
	descriptor protoreflect.FieldDescriptor
	prValue    protoreflect.Value
	rValue     reflect.Value
	schemaOptions
}

func (pfr protobufFieldReflection) arrowType() arrow.Type {
	if pfr.descriptor.Kind() == protoreflect.EnumKind {
		return arrow.DICTIONARY
	}
	if pfr.descriptor.Kind() == protoreflect.MessageKind && !pfr.descriptor.IsMap() && pfr.rValue.Kind() != reflect.Slice {
		return arrow.STRUCT
	}
	if pfr.descriptor.Kind() == protoreflect.MessageKind && pfr.descriptor.IsMap() {
		return arrow.MAP
	}
	if pfr.descriptor.IsList() && pfr.rValue.Kind() == reflect.Slice {
		return arrow.LIST
	}
	return arrow.NULL
}

func (pfr protobufFieldReflection) isStruct() bool {
	return pfr.descriptor.Kind() == protoreflect.MessageKind && !pfr.descriptor.IsMap() && pfr.rValue.Kind() != reflect.Slice
}

func (pfr protobufFieldReflection) isMap() bool {
	return pfr.descriptor.Kind() == protoreflect.MessageKind && pfr.descriptor.IsMap()
}

func (pfr protobufFieldReflection) isList() bool {
	return pfr.descriptor.IsList() && pfr.rValue.Kind() == reflect.Slice
}

func (pfr protobufFieldReflection) getListLength() int {
	return pfr.prValue.List().Len()
}

func (pfr protobufFieldReflection) getMapLength() int {
	return pfr.prValue.Map().Len()
}

func (plr protobufListReflection) generateListItems() chan protobufFieldReflection {
	out := make(chan protobufFieldReflection)

	go func() {
		defer close(out)
		for i := 0; i < plr.prValue.List().Len(); i++ {
			out <- protobufFieldReflection{
				descriptor:    plr.descriptor,
				prValue:       plr.prValue.List().Get(i),
				rValue:        plr.rValue.Index(i),
				schemaOptions: plr.schemaOptions,
			}
		}
	}()

	return out
}

func (pfr protobufFieldReflection) getDataType() arrow.DataType {
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
		// Special
		protoreflect.EnumKind:    nil,
		protoreflect.MessageKind: nil,
	}
	dt = typeMap[pfr.descriptor.Kind()]

	switch pfr.arrowType() {
	case arrow.DICTIONARY:
		dt = pfr.asDictionary().getDataType()
	case arrow.LIST:
		dt = pfr.asList().getDataType()
	case arrow.MAP:
		dt = pfr.asMap().getDataType()
	case arrow.STRUCT:
		dt = pfr.asStruct().getDataType()
	}

	return dt
}

func RecordFromProtobuf(psr ProtobufStructReflection, schema *arrow.Schema, mem memory.Allocator) arrow.Record {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	recordBuilder := array.NewRecordBuilder(mem, schema)

	var fieldNames []string
	for i, field := range schema.Fields() {
		AppendValueOrNull(recordBuilder.Field(i), psr.getFieldByName(field.Name), field, mem)
		fieldNames = append(fieldNames, field.Name)
	}

	var arrays []arrow.Array
	for _, bldr := range recordBuilder.Fields() {
		a := bldr.NewArray()
		arrays = append(arrays, a)
	}

	structArray, _ := array.NewStructArray(arrays, fieldNames)

	return array.RecordFromStructArray(structArray, schema)
}

func AppendValueOrNull(b array.Builder, pfr protobufFieldReflection, f arrow.Field, mem memory.Allocator) {
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
		b.(*array.Int32Builder).Append(int32(pv.Int()))
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
	case arrow.DICTIONARY:
		db := b.(array.DictionaryBuilder)
		db.AppendEmptyValue()
		err := db.AppendValueFromString(string(fd.Enum().Values().ByNumber(pv.Enum()).Name()))
		if err != nil {
			fmt.Println(err)
		}
	case arrow.STRUCT:
		psr := pfr.asStruct()
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
		for li := range pfr.asList().generateListItems() {
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

		for kvp := range pfr.asMap().generateKeyValuePairs() {
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

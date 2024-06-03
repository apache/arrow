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
	"reflect"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/huandu/xstrings"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

// ProtobufTypeHandler provides options on how protobuf fields should be handled in the conversion to arrow
type ProtobufTypeHandler int

const (
	// OneOfNull means do not wrap oneOfs in a union, they are treated as separate fields
	OneOfNull ProtobufTypeHandler = iota
	// OneOfDenseUnion maps the protobuf OneOf to an arrow.DENSE_UNION
	OneOfDenseUnion
	// EnumNumber uses the Enum numeric value
	EnumNumber
	// EnumValue uses the Enum string value
	EnumValue
	// EnumDictionary uses both the numeric and string value and maps to an arrow.Dictionary
	EnumDictionary
)

type schemaOptions struct {
	exclusionPolicy    func(pfr *ProtobufFieldReflection) bool
	fieldNameFormatter func(str string) string
	oneOfHandler       ProtobufTypeHandler
	enumHandler        ProtobufTypeHandler
}

// ProtobufFieldReflection represents the metadata and values of a protobuf field
type ProtobufFieldReflection struct {
	parent     *ProtobufMessageReflection
	descriptor protoreflect.FieldDescriptor
	prValue    protoreflect.Value
	rValue     reflect.Value
	schemaOptions
	arrow.Field
}

func (pfr *ProtobufFieldReflection) isNull() bool {
	for pfr.rValue.Kind() == reflect.Ptr {
		if pfr.rValue.IsNil() {
			return true
		}
		pfr.rValue = pfr.rValue.Elem()
	}

	if !pfr.rValue.IsValid() || !pfr.prValue.IsValid() {
		return true
	}
	return false
}

func (pfr *ProtobufFieldReflection) arrowField() arrow.Field {
	return arrow.Field{
		Name:     pfr.name(),
		Type:     pfr.getDataType(),
		Nullable: true,
	}
}

func (pfr *ProtobufFieldReflection) protoreflectValue() protoreflect.Value {
	return pfr.prValue
}

func (pfr *ProtobufFieldReflection) reflectValue() reflect.Value {
	return pfr.rValue
}

func (pfr *ProtobufFieldReflection) GetDescriptor() protoreflect.FieldDescriptor {
	return pfr.descriptor
}

func (pfr *ProtobufFieldReflection) name() string {
	if pfr.isOneOf() && pfr.schemaOptions.oneOfHandler != OneOfNull {
		return pfr.fieldNameFormatter(string(pfr.descriptor.ContainingOneof().Name()))
	}
	return pfr.fieldNameFormatter(string(pfr.descriptor.Name()))
}

func (pfr *ProtobufFieldReflection) arrowType() arrow.Type {
	if pfr.isOneOf() && pfr.schemaOptions.oneOfHandler == OneOfDenseUnion {
		return arrow.DENSE_UNION
	}
	if pfr.isEnum() {
		switch pfr.enumHandler {
		case EnumNumber:
			return arrow.INT32
		case EnumValue:
			return arrow.STRING
		case EnumDictionary:
			return arrow.DICTIONARY
		}
	}
	if pfr.isStruct() {
		return arrow.STRUCT
	}
	if pfr.isMap() {
		return arrow.MAP
	}
	if pfr.isList() {
		return arrow.LIST
	}
	switch pfr.descriptor.Kind() {
	case protoreflect.Int32Kind:
		return arrow.INT32
	case protoreflect.Int64Kind:
		return arrow.INT64
	case protoreflect.Sint32Kind:
		return arrow.INT32
	case protoreflect.Sint64Kind:
		return arrow.INT64
	case protoreflect.Uint32Kind:
		return arrow.UINT32
	case protoreflect.Uint64Kind:
		return arrow.UINT64
	case protoreflect.Fixed32Kind:
		return arrow.UINT32
	case protoreflect.Fixed64Kind:
		return arrow.UINT64
	case protoreflect.Sfixed32Kind:
		return arrow.INT32
	case protoreflect.Sfixed64Kind:
		return arrow.INT64
	case protoreflect.FloatKind:
		return arrow.FLOAT32
	case protoreflect.DoubleKind:
		return arrow.FLOAT64
	case protoreflect.StringKind:
		return arrow.STRING
	case protoreflect.BytesKind:
		return arrow.BINARY
	case protoreflect.BoolKind:
		return arrow.BOOL
	}
	return arrow.NULL
}

func (pfr *ProtobufFieldReflection) isOneOf() bool {
	return pfr.descriptor.ContainingOneof() != nil
}

func (pfr *ProtobufFieldReflection) isEnum() bool {
	return pfr.descriptor.Kind() == protoreflect.EnumKind
}

func (pfr *ProtobufFieldReflection) isStruct() bool {
	return pfr.descriptor.Kind() == protoreflect.MessageKind && !pfr.descriptor.IsMap() && pfr.rValue.Kind() != reflect.Slice
}

func (pfr *ProtobufFieldReflection) isMap() bool {
	return pfr.descriptor.Kind() == protoreflect.MessageKind && pfr.descriptor.IsMap()
}

func (pfr *ProtobufFieldReflection) isList() bool {
	return pfr.descriptor.IsList() && pfr.rValue.Kind() == reflect.Slice
}

// ProtobufMessageReflection represents the metadata and values of a protobuf message
type ProtobufMessageReflection struct {
	descriptor protoreflect.MessageDescriptor
	message    protoreflect.Message
	rValue     reflect.Value
	schemaOptions
	fields []ProtobufMessageFieldReflection
}

func (psr ProtobufMessageReflection) unmarshallAny() ProtobufMessageReflection {
	if psr.descriptor.FullName() == "google.protobuf.Any" && psr.rValue.IsValid() {
		for psr.rValue.Type().Kind() == reflect.Ptr {
			psr.rValue = reflect.Indirect(psr.rValue)
		}
		fieldValueAsAny, _ := psr.rValue.Interface().(anypb.Any)
		msg, _ := fieldValueAsAny.UnmarshalNew()

		v := reflect.ValueOf(msg)
		for v.Kind() == reflect.Ptr {
			v = reflect.Indirect(v)
		}

		return ProtobufMessageReflection{
			descriptor:    msg.ProtoReflect().Descriptor(),
			message:       msg.ProtoReflect(),
			rValue:        v,
			schemaOptions: psr.schemaOptions,
		}
	} else {
		return psr
	}
}

func (psr ProtobufMessageReflection) getArrowFields() []arrow.Field {
	var fields []arrow.Field

	for pfr := range psr.generateStructFields() {
		fields = append(fields, arrow.Field{
			Name:     pfr.name(),
			Type:     pfr.getDataType(),
			Nullable: true,
		})
	}

	return fields
}

type protobufListReflection struct {
	ProtobufFieldReflection
}

func (pfr *ProtobufFieldReflection) asList() protobufListReflection {
	return protobufListReflection{*pfr}
}

func (plr protobufListReflection) getDataType() arrow.DataType {
	for li := range plr.generateListItems() {
		return arrow.ListOf(li.getDataType())
	}
	pfr := ProtobufFieldReflection{
		descriptor:    plr.descriptor,
		schemaOptions: plr.schemaOptions,
	}
	return arrow.ListOf(pfr.getDataType())
}

type protobufUnionReflection struct {
	ProtobufFieldReflection
}

func (pfr *ProtobufFieldReflection) asUnion() protobufUnionReflection {
	return protobufUnionReflection{*pfr}
}

func (pur protobufUnionReflection) isThisOne() bool {
	for pur.rValue.Kind() == reflect.Ptr || pur.rValue.Kind() == reflect.Interface {
		pur.rValue = pur.rValue.Elem()
	}
	return pur.rValue.Field(0).String() == pur.prValue.String()
}

func (pur protobufUnionReflection) whichOne() arrow.UnionTypeCode {
	fds := pur.descriptor.ContainingOneof().Fields()
	for i := 0; i < fds.Len(); i++ {
		pfr := pur.parent.getFieldByName(string(fds.Get(i).Name()))
		if pfr.asUnion().isThisOne() {
			return pur.getUnionTypeCode(int32(pfr.descriptor.Number()))
		}
	}
	// i.e. all null
	return -1
}

func (pur protobufUnionReflection) getField() *ProtobufFieldReflection {
	fds := pur.descriptor.ContainingOneof().Fields()
	for i := 0; i < fds.Len(); i++ {
		pfr := pur.parent.getFieldByName(string(fds.Get(i).Name()))
		if pfr.asUnion().isThisOne() {
			return pfr
		}
	}
	// i.e. all null
	return nil
}

func (pur protobufUnionReflection) getUnionTypeCode(n int32) arrow.UnionTypeCode {
	//We use the index of the field number as there is a limit on the arrow.UnionTypeCode (127)
	//which a protobuf Number could realistically exceed
	fds := pur.descriptor.ContainingOneof().Fields()
	for i := 0; i < fds.Len(); i++ {
		if n == int32(fds.Get(i).Number()) {
			return int8(i)
		}
	}
	return -1
}

func (pur protobufUnionReflection) generateUnionFields() chan *ProtobufFieldReflection {
	out := make(chan *ProtobufFieldReflection)
	go func() {
		defer close(out)
		fds := pur.descriptor.ContainingOneof().Fields()
		for i := 0; i < fds.Len(); i++ {
			pfr := pur.parent.getFieldByName(string(fds.Get(i).Name()))
			// Do not get stuck in a recursion loop
			pfr.oneOfHandler = OneOfNull
			if pfr.exclusionPolicy(pfr) {
				continue
			}
			out <- pfr
		}
	}()

	return out
}

func (pur protobufUnionReflection) getArrowFields() []arrow.Field {
	var fields []arrow.Field

	for pfr := range pur.generateUnionFields() {
		fields = append(fields, pfr.arrowField())
	}

	return fields
}

func (pur protobufUnionReflection) getDataType() arrow.DataType {
	fds := pur.getArrowFields()
	typeCodes := make([]arrow.UnionTypeCode, len(fds))
	for i := 0; i < len(fds); i++ {
		typeCodes[i] = arrow.UnionTypeCode(i)
	}
	return arrow.DenseUnionOf(fds, typeCodes)
}

type protobufDictReflection struct {
	ProtobufFieldReflection
}

func (pfr *ProtobufFieldReflection) asDictionary() protobufDictReflection {
	return protobufDictReflection{*pfr}
}

func (pdr protobufDictReflection) getDataType() arrow.DataType {
	return &arrow.DictionaryType{
		IndexType: arrow.PrimitiveTypes.Int32,
		ValueType: arrow.BinaryTypes.String,
		Ordered:   false,
	}
}

func (pdr protobufDictReflection) getDictValues(mem memory.Allocator) arrow.Array {
	enumValues := pdr.descriptor.Enum().Values()
	bldr := array.NewStringBuilder(mem)
	for i := 0; i < enumValues.Len(); i++ {
		bldr.Append(string(enumValues.Get(i).Name()))
	}
	return bldr.NewArray()
}

type protobufMapReflection struct {
	ProtobufFieldReflection
}

func (pfr *ProtobufFieldReflection) asMap() protobufMapReflection {
	return protobufMapReflection{*pfr}
}

func (pmr protobufMapReflection) getDataType() arrow.DataType {
	for kvp := range pmr.generateKeyValuePairs() {
		return kvp.getDataType()
	}
	return protobufMapKeyValuePairReflection{
		k: ProtobufFieldReflection{
			parent:        pmr.parent,
			descriptor:    pmr.descriptor.MapKey(),
			schemaOptions: pmr.schemaOptions,
		},
		v: ProtobufFieldReflection{
			parent:        pmr.parent,
			descriptor:    pmr.descriptor.MapValue(),
			schemaOptions: pmr.schemaOptions,
		},
	}.getDataType()
}

type protobufMapKeyValuePairReflection struct {
	k ProtobufFieldReflection
	v ProtobufFieldReflection
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
				k: ProtobufFieldReflection{
					parent:        pmr.parent,
					descriptor:    pmr.descriptor.MapKey(),
					prValue:       getMapKey(k),
					rValue:        k,
					schemaOptions: pmr.schemaOptions,
				},
				v: ProtobufFieldReflection{
					parent:        pmr.parent,
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

func (psr ProtobufMessageReflection) generateStructFields() chan *ProtobufFieldReflection {
	out := make(chan *ProtobufFieldReflection)

	go func() {
		defer close(out)
		fds := psr.descriptor.Fields()
		for i := 0; i < fds.Len(); i++ {
			pfr := psr.getFieldByName(string(fds.Get(i).Name()))
			if psr.exclusionPolicy(pfr) {
				continue
			}
			if pfr.arrowType() == arrow.DENSE_UNION {
				if pfr.descriptor.Number() != pfr.descriptor.ContainingOneof().Fields().Get(0).Number() {
					continue
				}
			}
			out <- pfr
		}
	}()

	return out
}

func (psr ProtobufMessageReflection) generateFields() chan *ProtobufFieldReflection {
	out := make(chan *ProtobufFieldReflection)

	go func() {
		defer close(out)
		fds := psr.descriptor.Fields()
		for i := 0; i < fds.Len(); i++ {
			pfr := psr.getFieldByName(string(fds.Get(i).Name()))
			if psr.exclusionPolicy(pfr) {
				continue
			}
			if pfr.arrowType() == arrow.DENSE_UNION {
				if pfr.descriptor.Number() != pfr.descriptor.ContainingOneof().Fields().Get(0).Number() {
					continue
				}
			}
			out <- pfr
		}
	}()

	return out
}

func (pfr *ProtobufFieldReflection) asStruct() ProtobufMessageReflection {
	psr := ProtobufMessageReflection{
		descriptor:    pfr.descriptor.Message(),
		rValue:        pfr.rValue,
		schemaOptions: pfr.schemaOptions,
	}
	if pfr.prValue.IsValid() {
		psr.message = pfr.prValue.Message()
	}
	psr = psr.unmarshallAny()
	return psr
}

func (psr ProtobufMessageReflection) getDataType() arrow.DataType {
	return arrow.StructOf(psr.getArrowFields()...)
}

func (psr ProtobufMessageReflection) getFieldByName(n string) *ProtobufFieldReflection {
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
	pfr := ProtobufFieldReflection{
		parent:        &psr,
		descriptor:    fd,
		rValue:        fv,
		schemaOptions: psr.schemaOptions,
	}
	if psr.message != nil {
		pfr.prValue = psr.message.Get(fd)
	}
	return &pfr
}

func (plr protobufListReflection) generateListItems() chan ProtobufFieldReflection {
	out := make(chan ProtobufFieldReflection)

	go func() {
		defer close(out)
		for i := 0; i < plr.prValue.List().Len(); i++ {
			out <- ProtobufFieldReflection{
				descriptor:    plr.descriptor,
				prValue:       plr.prValue.List().Get(i),
				rValue:        plr.rValue.Index(i),
				schemaOptions: plr.schemaOptions,
			}
		}
	}()

	return out
}

func (pfr *ProtobufFieldReflection) getDataType() arrow.DataType {
	switch pfr.arrowType() {
	case arrow.DENSE_UNION:
		return pfr.asUnion().getDataType()
	case arrow.DICTIONARY:
		return pfr.asDictionary().getDataType()
	case arrow.LIST:
		return pfr.asList().getDataType()
	case arrow.MAP:
		return pfr.asMap().getDataType()
	case arrow.STRUCT:
		return pfr.asStruct().getDataType()
	case arrow.INT32:
		return arrow.PrimitiveTypes.Int32
	case arrow.INT64:
		return arrow.PrimitiveTypes.Int64
	case arrow.UINT32:
		return arrow.PrimitiveTypes.Uint32
	case arrow.UINT64:
		return arrow.PrimitiveTypes.Uint64
	case arrow.FLOAT32:
		return arrow.PrimitiveTypes.Float32
	case arrow.FLOAT64:
		return arrow.PrimitiveTypes.Float64
	case arrow.STRING:
		return arrow.BinaryTypes.String
	case arrow.BINARY:
		return arrow.BinaryTypes.Binary
	case arrow.BOOL:
		return arrow.FixedWidthTypes.Boolean
	}
	return nil
}

type protobufReflection interface {
	name() string
	arrowType() arrow.Type
	protoreflectValue() protoreflect.Value
	reflectValue() reflect.Value
	GetDescriptor() protoreflect.FieldDescriptor
	isNull() bool
	isEnum() bool
	asDictionary() protobufDictReflection
	isList() bool
	asList() protobufListReflection
	isMap() bool
	asMap() protobufMapReflection
	isStruct() bool
	asStruct() ProtobufMessageReflection
	isOneOf() bool
	asUnion() protobufUnionReflection
}

// ProtobufMessageFieldReflection links together the message and it's fields
type ProtobufMessageFieldReflection struct {
	parent *ProtobufMessageReflection
	protobufReflection
	arrow.Field
}

// Schema returns an arrow.Schema representing a protobuf message
func (msg ProtobufMessageReflection) Schema() *arrow.Schema {
	var fields []arrow.Field
	for _, f := range msg.fields {
		fields = append(fields, f.Field)
	}
	return arrow.NewSchema(fields, nil)
}

// Record returns an arrow.Record for a protobuf message
func (msg ProtobufMessageReflection) Record(mem memory.Allocator) arrow.Record {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	schema := msg.Schema()

	recordBuilder := array.NewRecordBuilder(mem, schema)

	var fieldNames []string
	for i, f := range msg.fields {
		f.AppendValueOrNull(recordBuilder.Field(i), mem)
		fieldNames = append(fieldNames, f.protobufReflection.name())
	}

	var arrays []arrow.Array
	for _, bldr := range recordBuilder.Fields() {
		a := bldr.NewArray()
		arrays = append(arrays, a)
	}

	structArray, _ := array.NewStructArray(arrays, fieldNames)

	return array.RecordFromStructArray(structArray, schema)
}

// NewProtobufMessageReflection initialises a ProtobufMessageReflection
// can be used to convert a protobuf message into an arrow Record
func NewProtobufMessageReflection(msg proto.Message, options ...option) *ProtobufMessageReflection {
	v := reflect.ValueOf(msg)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	includeAll := func(pfr *ProtobufFieldReflection) bool {
		return false
	}
	noFormatting := func(str string) string {
		return str
	}
	psr := &ProtobufMessageReflection{
		descriptor: msg.ProtoReflect().Descriptor(),
		message:    msg.ProtoReflect(),
		rValue:     v,
		schemaOptions: schemaOptions{
			exclusionPolicy:    includeAll,
			fieldNameFormatter: noFormatting,
			oneOfHandler:       OneOfNull,
			enumHandler:        EnumDictionary,
		},
	}

	for _, opt := range options {
		opt(psr)
	}

	var fields []ProtobufMessageFieldReflection

	for pfr := range psr.generateFields() {
		fields = append(fields, ProtobufMessageFieldReflection{
			parent:             psr,
			protobufReflection: pfr,
			Field:              pfr.arrowField(),
		})
	}

	psr.fields = fields

	return psr
}

type option func(*ProtobufMessageReflection)

// WithExclusionPolicy is an option for a ProtobufMessageReflection
// WithExclusionPolicy acts as a deny filter on the fields of a protobuf message
// i.e. prevents them from being included in the schema.
// A use case for this is to exclude fields containing PII.
func WithExclusionPolicy(ex func(pfr *ProtobufFieldReflection) bool) option {
	return func(psr *ProtobufMessageReflection) {
		psr.exclusionPolicy = ex
	}
}

// WithFieldNameFormatter is an option for a ProtobufMessageReflection
// WithFieldNameFormatter enables customisation of the field names in the arrow schema
// By default, the field names are taken from the protobuf message (.proto file)
func WithFieldNameFormatter(formatter func(str string) string) option {
	return func(psr *ProtobufMessageReflection) {
		psr.fieldNameFormatter = formatter
	}
}

// WithOneOfHandler is an option for a ProtobufMessageReflection
// WithOneOfHandler enables customisation of the protobuf oneOf type in the arrow schema
// By default, the oneOfs are mapped to separate columns
func WithOneOfHandler(oneOfHandler ProtobufTypeHandler) option {
	return func(psr *ProtobufMessageReflection) {
		psr.oneOfHandler = oneOfHandler
	}
}

// WithEnumHandler is an option for a ProtobufMessageReflection
// WithEnumHandler enables customisation of the protobuf Enum type in the arrow schema
// By default, the Enums are mapped to arrow.Dictionary
func WithEnumHandler(enumHandler ProtobufTypeHandler) option {
	return func(psr *ProtobufMessageReflection) {
		psr.enumHandler = enumHandler
	}
}

// AppendValueOrNull add the value of a protobuf field to an arrow array builder
func (f ProtobufMessageFieldReflection) AppendValueOrNull(b array.Builder, mem memory.Allocator) error {
	pv := f.protoreflectValue()
	fd := f.GetDescriptor()

	if f.isNull() {
		b.AppendNull()
		return nil
	}

	switch b.Type().ID() {
	case arrow.STRING:
		if f.protobufReflection.isEnum() {
			b.(*array.StringBuilder).Append(string(fd.Enum().Values().ByNumber(pv.Enum()).Name()))
		} else {
			b.(*array.StringBuilder).Append(pv.String())
		}
	case arrow.BINARY:
		b.(*array.BinaryBuilder).Append(pv.Bytes())
	case arrow.INT32:
		if f.protobufReflection.isEnum() {
			b.(*array.Int32Builder).Append(int32(f.reflectValue().Int()))
		} else {
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
	case arrow.DENSE_UNION:
		ub := b.(array.UnionBuilder)
		pur := f.asUnion()
		if pur.whichOne() == -1 {
			ub.AppendNull()
			break
		}
		ub.Append(pur.whichOne())
		cb := ub.Child(int(pur.whichOne()))
		err := ProtobufMessageFieldReflection{
			parent:             f.parent,
			protobufReflection: pur.getField(),
			Field:              pur.arrowField(),
		}.AppendValueOrNull(cb, mem)
		if err != nil {
			return err
		}
	case arrow.DICTIONARY:
		pdr := f.asDictionary()
		db := b.(*array.BinaryDictionaryBuilder)
		err := db.InsertStringDictValues(pdr.getDictValues(mem).(*array.String))
		if err != nil {
			return err
		}
		enumNum := int(f.reflectValue().Int())
		enumVal := fd.Enum().Values().ByNumber(protoreflect.EnumNumber(enumNum)).Name()
		err = db.AppendValueFromString(string(enumVal))
		if err != nil {
			return err
		}
	case arrow.STRUCT:
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		child := ProtobufMessageFieldReflection{
			parent: f.parent,
		}
		for i, field := range f.Field.Type.(*arrow.StructType).Fields() {
			child.protobufReflection = f.asStruct().getFieldByName(field.Name)
			child.Field = field
			err := child.AppendValueOrNull(sb.FieldBuilder(i), mem)
			if err != nil {
				return err
			}
		}
	case arrow.LIST:
		lb := b.(*array.ListBuilder)
		l := pv.List().Len()
		if l == 0 {
			lb.AppendEmptyValue()
			break
		}
		lb.ValueBuilder().Reserve(l)
		lb.Append(true)
		child := ProtobufMessageFieldReflection{
			parent: f.parent,
			Field:  f.Field.Type.(*arrow.ListType).ElemField(),
		}
		for li := range f.asList().generateListItems() {
			child.protobufReflection = &li
			err := child.AppendValueOrNull(lb.ValueBuilder(), mem)
			if err != nil {
				return err
			}
		}
	case arrow.MAP:
		mb := b.(*array.MapBuilder)
		l := pv.Map().Len()
		if l == 0 {
			mb.AppendEmptyValue()
			break
		}
		mb.KeyBuilder().Reserve(l)
		mb.ItemBuilder().Reserve(l)
		mb.Append(true)
		k := ProtobufMessageFieldReflection{
			parent: f.parent,
			Field:  f.Field.Type.(*arrow.MapType).KeyField(),
		}
		v := ProtobufMessageFieldReflection{
			parent: f.parent,
			Field:  f.Field.Type.(*arrow.MapType).ItemField(),
		}
		for kvp := range f.asMap().generateKeyValuePairs() {
			k.protobufReflection = &kvp.k
			err := k.AppendValueOrNull(mb.KeyBuilder(), mem)
			if err != nil {
				return err
			}
			v.protobufReflection = &kvp.v
			err = v.AppendValueOrNull(mb.ItemBuilder(), mem)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("not able to appendValueOrNull for type %s", b.Type().ID())
	}
	return nil
}

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

type OneOfHandler int

const (
	// Null means do not wrap oneOfs in a union, they are treated as separate fields
	Null OneOfHandler = iota
	// DenseUnion maps the protobuf OneOf to an arrow.DENSE_UNION
	DenseUnion
)

type schemaOptions struct {
	exclusionPolicy    func(pfr *protobufFieldReflection) bool
	fieldNameFormatter func(str string) string
	oneOfHandler       OneOfHandler
}

// ProtobufStructReflection represents the metadata and values of a protobuf message
type ProtobufStructReflection struct {
	descriptor protoreflect.MessageDescriptor
	message    protoreflect.Message
	rValue     reflect.Value
	schemaOptions
}

type protobufReflection interface {
	name() string
	arrowType() arrow.Type
	protoreflectValue() protoreflect.Value
	reflectValue() reflect.Value
	getDescriptor() protoreflect.FieldDescriptor
	isNull() bool
	asDictionary() protobufDictReflection
	asList() protobufListReflection
	asMap() protobufMapReflection
	asStruct() ProtobufStructReflection
	asUnion() protobufUnionReflection
}

type SuperField struct {
	parent *ProtobufStructReflection
	protobufReflection
	arrow.Field
}

type SuperMessage struct {
	superFields []SuperField
}

func (sm SuperMessage) Schema() *arrow.Schema {
	var fields []arrow.Field
	for _, sf := range sm.superFields {
		fields = append(fields, sf.Field)
	}
	return arrow.NewSchema(fields, nil)
}

func (sm SuperMessage) Record(mem memory.Allocator) arrow.Record {
	if mem == nil {
		mem = memory.NewGoAllocator()
	}

	schema := sm.Schema()

	recordBuilder := array.NewRecordBuilder(mem, schema)

	var fieldNames []string
	for i, sf := range sm.superFields {
		sf.AppendValueOrNull(recordBuilder.Field(i), mem)
		fieldNames = append(fieldNames, sf.protobufReflection.name())
	}

	var arrays []arrow.Array
	for _, bldr := range recordBuilder.Fields() {
		a := bldr.NewArray()
		arrays = append(arrays, a)
	}

	structArray, _ := array.NewStructArray(arrays, fieldNames)

	return array.RecordFromStructArray(structArray, schema)
}

func NewSuperMessage(msg proto.Message, options ...option) *SuperMessage {
	v := reflect.ValueOf(msg)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	includeAll := func(pfr *protobufFieldReflection) bool {
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
			oneOfHandler:       Null,
		},
	}

	for _, opt := range options {
		opt(psr)
	}

	var fields []SuperField

	for pfr := range psr.generateFields() {
		fields = append(fields, SuperField{
			parent:             psr,
			protobufReflection: pfr,
			Field:              pfr.arrowField(),
		})
	}

	return &SuperMessage{superFields: fields}
}

type option func(*ProtobufStructReflection)

// WithExclusionPolicy is an option for a ProtobufStructReflection
// WithExclusionPolicy acts as a deny filter on the fields of a protobuf message
// i.e. prevents them from being included in the schema.
// A use case for this is to exclude fields containing PII.
func WithExclusionPolicy(ex func(pfr *protobufFieldReflection) bool) option {
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

// WithOneOfHandler is an option for a ProtobufStructReflection
// WithOneOfHandler enables customisation of the protobuf oneOf type in the arrow schema
// By default, the oneOfs are mapped to separate columns
func WithOneOfHandler(oneOfHandler OneOfHandler) option {
	return func(psr *ProtobufStructReflection) {
		psr.oneOfHandler = oneOfHandler
	}
}

func (psr ProtobufStructReflection) unmarshallAny() ProtobufStructReflection {
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
			Name:     pfr.name(),
			Type:     pfr.getDataType(),
			Nullable: true,
		})
	}

	return fields
}

type protobufListReflection struct {
	protobufFieldReflection
}

func (pfr *protobufFieldReflection) asList() protobufListReflection {
	return protobufListReflection{*pfr}
}

func (plr protobufListReflection) getDataType() arrow.DataType {
	for li := range plr.generateListItems() {
		return arrow.ListOf(li.getDataType())
	}
	pfr := protobufFieldReflection{
		descriptor:    plr.descriptor,
		schemaOptions: plr.schemaOptions,
	}
	return arrow.ListOf(pfr.getDataType())
}

type protobufUnionReflection struct {
	protobufFieldReflection
}

func (pfr *protobufFieldReflection) asUnion() protobufUnionReflection {
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

func (pur protobufUnionReflection) getField() *protobufFieldReflection {
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

func (pur protobufUnionReflection) generateUnionFields() chan *protobufFieldReflection {
	out := make(chan *protobufFieldReflection)
	go func() {
		defer close(out)
		fds := pur.descriptor.ContainingOneof().Fields()
		for i := 0; i < fds.Len(); i++ {
			pfr := pur.parent.getFieldByName(string(fds.Get(i).Name()))
			// Do not get stuck in a recursion loop
			pfr.oneOfHandler = Null
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
	protobufFieldReflection
}

func (pfr *protobufFieldReflection) asDictionary() protobufDictReflection {
	return protobufDictReflection{*pfr}
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

func (pfr *protobufFieldReflection) asMap() protobufMapReflection {
	return protobufMapReflection{*pfr}
}

func (pmr protobufMapReflection) getDataType() arrow.DataType {
	for kvp := range pmr.generateKeyValuePairs() {
		return kvp.getDataType()
	}
	return protobufMapKeyValuePairReflection{
		k: protobufFieldReflection{
			parent:        pmr.parent,
			descriptor:    pmr.descriptor.MapKey(),
			schemaOptions: pmr.schemaOptions,
		},
		v: protobufFieldReflection{
			parent:        pmr.parent,
			descriptor:    pmr.descriptor.MapValue(),
			schemaOptions: pmr.schemaOptions,
		},
	}.getDataType()
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
					parent:        pmr.parent,
					descriptor:    pmr.descriptor.MapKey(),
					prValue:       getMapKey(k),
					rValue:        k,
					schemaOptions: pmr.schemaOptions,
				},
				v: protobufFieldReflection{
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

func (psr ProtobufStructReflection) generateStructFields() chan *protobufFieldReflection {
	out := make(chan *protobufFieldReflection)

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

func (psr ProtobufStructReflection) generateFields() chan *protobufFieldReflection {
	out := make(chan *protobufFieldReflection)

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

func (pfr *protobufFieldReflection) asStruct() ProtobufStructReflection {
	psr := ProtobufStructReflection{
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

func (psr ProtobufStructReflection) getDataType() arrow.DataType {
	return arrow.StructOf(psr.getArrowFields()...)
}

func (psr ProtobufStructReflection) getFieldByName(n string) *protobufFieldReflection {
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
	pfr := protobufFieldReflection{
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

type protobufFieldReflection struct {
	parent     *ProtobufStructReflection
	descriptor protoreflect.FieldDescriptor
	prValue    protoreflect.Value
	rValue     reflect.Value
	schemaOptions
}

func (pfr *protobufFieldReflection) isNull() bool {
	for pfr.rValue.Kind() == reflect.Ptr {
		if pfr.rValue.IsNil() {
			return true
		}
		pfr.rValue = pfr.rValue.Elem()
	}

	if !pfr.rValue.IsValid() && !pfr.prValue.IsValid() {
		return true
	}
	return false
}

func (pfr *protobufFieldReflection) arrowField() arrow.Field {
	return arrow.Field{
		Name:     pfr.name(),
		Type:     pfr.getDataType(),
		Nullable: true,
	}
}

func (pfr *protobufFieldReflection) protoreflectValue() protoreflect.Value {
	return pfr.prValue
}

func (pfr *protobufFieldReflection) reflectValue() reflect.Value {
	return pfr.rValue
}

func (pfr *protobufFieldReflection) getDescriptor() protoreflect.FieldDescriptor {
	return pfr.descriptor
}

func (pfr *protobufFieldReflection) name() string {
	if pfr.isOneOf() && pfr.schemaOptions.oneOfHandler != Null {
		return pfr.fieldNameFormatter(string(pfr.descriptor.ContainingOneof().Name()))
	}
	return pfr.fieldNameFormatter(string(pfr.descriptor.Name()))
}

func (pfr *protobufFieldReflection) arrowType() arrow.Type {
	if pfr.isOneOf() && pfr.schemaOptions.oneOfHandler == DenseUnion {
		return arrow.DENSE_UNION
	}
	if pfr.isEnum() {
		return arrow.DICTIONARY
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
	return arrow.NULL
}

func (pfr *protobufFieldReflection) isOneOf() bool {
	return pfr.descriptor.ContainingOneof() != nil
}

func (pfr *protobufFieldReflection) isEnum() bool {
	return pfr.descriptor.Kind() == protoreflect.EnumKind
}

func (pfr *protobufFieldReflection) isStruct() bool {
	return pfr.descriptor.Kind() == protoreflect.MessageKind && !pfr.descriptor.IsMap() && pfr.rValue.Kind() != reflect.Slice
}

func (pfr *protobufFieldReflection) isMap() bool {
	return pfr.descriptor.Kind() == protoreflect.MessageKind && pfr.descriptor.IsMap()
}

func (pfr *protobufFieldReflection) isList() bool {
	return pfr.descriptor.IsList() && pfr.rValue.Kind() == reflect.Slice
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

func (pfr *protobufFieldReflection) getDataType() arrow.DataType {
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
	case arrow.DENSE_UNION:
		dt = pfr.asUnion().getDataType()
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

func (sf SuperField) AppendValueOrNull(b array.Builder, mem memory.Allocator) {
	pv := sf.protoreflectValue()
	fd := sf.getDescriptor()

	if sf.isNull() {
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
	case arrow.DENSE_UNION:
		ub := b.(array.UnionBuilder)
		pur := sf.asUnion()
		if pur.whichOne() == -1 {
			ub.AppendNull()
			break
		}
		ub.Append(pur.whichOne())
		cb := ub.Child(int(pur.whichOne()))
		SuperField{
			parent:             sf.parent,
			protobufReflection: pur.getField(),
			Field:              pur.arrowField(),
		}.AppendValueOrNull(cb, mem)
	case arrow.DICTIONARY:
		db := b.(array.DictionaryBuilder)
		err := db.AppendValueFromString(string(fd.Enum().Values().ByNumber(pv.Enum()).Name()))
		if err != nil {
			fmt.Println(err)
		}
	case arrow.STRUCT:
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		for i, field := range sf.Field.Type.(*arrow.StructType).Fields() {
			SuperField{
				parent:             sf.parent,
				protobufReflection: sf.asStruct().getFieldByName(field.Name),
				Field:              field,
			}.AppendValueOrNull(sb.FieldBuilder(i), mem)
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
		for li := range sf.asList().generateListItems() {
			SuperField{
				parent:             sf.parent,
				protobufReflection: &li,
				Field:              sf.Field.Type.(*arrow.ListType).ElemField(),
			}.AppendValueOrNull(lb.ValueBuilder(), mem)
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

		for kvp := range sf.asMap().generateKeyValuePairs() {
			SuperField{
				parent:             sf.parent,
				protobufReflection: &kvp.k,
				Field:              sf.Field.Type.(*arrow.MapType).KeyField(),
			}.AppendValueOrNull(mb.KeyBuilder(), mem)
			SuperField{
				parent:             sf.parent,
				protobufReflection: &kvp.v,
				Field:              sf.Field.Type.(*arrow.MapType).ItemField(),
			}.AppendValueOrNull(mb.ItemBuilder(), mem)
		}
	default:
		fmt.Printf("No logic for type %s", b.Type().ID())
	}
}

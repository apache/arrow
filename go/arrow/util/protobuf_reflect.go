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

type OneOfHandler int

const (
	// Null means do not wrap oneOfs in a union, they are treated as separate fields
	Null OneOfHandler = iota
	// DenseUnion maps the protobuf OneOf to an arrow.DENSE_UNION
	DenseUnion
)

type schemaOptions struct {
	exclusionPolicy    func(pfr protobufFieldReflection) bool
	fieldNameFormatter func(str string) string
	oneOfHandler       OneOfHandler
}

// ProtobufStructReflection represents the metadata and values of a protobuf message
type ProtobufStructReflection struct {
	descriptor protoreflect.MessageDescriptor
	message    protoreflect.Message
	rValue     reflect.Value
	schemaOptions
	superMap map[string]SuperField
}

func (psr ProtobufStructReflection) makeSuperMap() map[string]SuperField {
	superMap := make(map[string]SuperField)
	for pfr := range psr.generateStructFields() {
		superMap[pfr.name()] = SuperField{
			parent:             &psr,
			protobufReflection: pfr,
			Field: arrow.Field{
				Name:     pfr.name(),
				Type:     pfr.getDataType(),
				Nullable: true,
			},
		}
	}
	return superMap
}

// ProtobufMessageReflection represents the metadata and values of a protobuf message and maps them to arrow fields
type ProtobufMessageReflection struct {
	ProtobufStructReflection
}

type protobufReflection interface {
	arrowType() arrow.Type
	protoreflectValue() protoreflect.Value
	reflectValue() reflect.Value
	getDescriptor() protoreflect.FieldDescriptor
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
	name string
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
		sf.AppendValueOrNull3(recordBuilder.Field(i), mem)
		fieldNames = append(fieldNames, sf.name)
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
			oneOfHandler:       Null,
		},
	}

	for _, opt := range options {
		opt(psr)
	}

	var superFields []SuperField

	for sf := range psr.generateSuperFields() {
		superFields = append(superFields, sf)
	}

	return &SuperMessage{superFields: superFields}
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
			oneOfHandler:       Null,
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

// WithOneOfHandler is an option for a ProtobufStructReflection
// WithOneOfHandler enables customisation of the protobuf oneOf type in the arrow schema
// By default, the oneOfs are mapped to separate columns
func WithOneOfHandler(oneOfHandler OneOfHandler) option {
	return func(psr *ProtobufStructReflection) {
		psr.oneOfHandler = oneOfHandler
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

func (pfr protobufFieldReflection) asList() protobufListReflection {
	return protobufListReflection{pfr}
}

func (plr protobufListReflection) getDataType() arrow.DataType {
	for li := range plr.generateListItems() {
		return arrow.ListOf(li.getDataType())
	}
	return nil
}

type protobufUnionReflection struct {
	protobufFieldReflection
}

func (pfr protobufFieldReflection) asUnion() protobufUnionReflection {
	return protobufUnionReflection{pfr}
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
			return &pfr
		}
	}
	// i.e. all null
	return nil
}

func (pur protobufUnionReflection) getUnionTypeCode(n int32) arrow.UnionTypeCode {
	//We use the index of the field number as there is a limit on the arrow.UnionTypeCode (127)
	//which a protobuf Number could realistically exceed
	fmt.Println(pur.protobufFieldReflection.descriptor.FullName())
	fmt.Println(n)
	fds := pur.descriptor.ContainingOneof().Fields()
	for i := 0; i < fds.Len(); i++ {
		if n == int32(fds.Get(i).Number()) {
			fmt.Println(i)
			return int8(i)
		}
	}
	return -1
}

func (pur protobufUnionReflection) generateUnionFields() chan protobufFieldReflection {
	out := make(chan protobufFieldReflection)
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
		fields = append(fields, arrow.Field{
			Name:     pfr.name(),
			Type:     pfr.getDataType(),
			Nullable: true,
		})
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

func (psr ProtobufStructReflection) generateSuperFields() chan SuperField {
	out := make(chan SuperField)

	go func() {
		defer close(out)
		fds := psr.descriptor.Fields()
		for i := 0; i < fds.Len(); i++ {
			pfr := psr.getFieldByName(string(fds.Get(i).Name()))
			if psr.exclusionPolicy(pfr) {
				continue
			}
			sf := SuperField{
				parent:             &psr,
				protobufReflection: pfr,
				Field: arrow.Field{
					Name:     pfr.name(),
					Type:     pfr.getDataType(),
					Nullable: false,
					Metadata: arrow.Metadata{},
				},
			}
			if pfr.arrowType() == arrow.DENSE_UNION {
				if pfr.descriptor.Number() != pfr.descriptor.ContainingOneof().Fields().Get(0).Number() {
					continue
				}
			}
			out <- sf
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
	// TODO need to do something here to go from the parent name `oneof` to the children
	// func (psr) get_field_by_name, maps the arrow schema to protobuf fields
	// could also look at tagging the schema by number, but I would prefer not to do that
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
		&psr,
		fd,
		psr.message.Get(fd),
		fv,
		psr.schemaOptions,
	}
}

type protobufFieldReflection struct {
	parent     *ProtobufStructReflection
	descriptor protoreflect.FieldDescriptor
	prValue    protoreflect.Value
	rValue     reflect.Value
	schemaOptions
}

func (pfr protobufFieldReflection) protoreflectValue() protoreflect.Value {
	return pfr.prValue
}

func (pfr protobufFieldReflection) reflectValue() reflect.Value {
	return pfr.rValue
}

func (pfr protobufFieldReflection) getDescriptor() protoreflect.FieldDescriptor {
	return pfr.descriptor
}

func (pfr protobufFieldReflection) name() string {
	if pfr.descriptor.ContainingOneof() != nil && pfr.schemaOptions.oneOfHandler != Null {
		return pfr.fieldNameFormatter(string(pfr.descriptor.ContainingOneof().Name()))
	}
	return pfr.fieldNameFormatter(string(pfr.descriptor.Name()))
}

func (pfr protobufFieldReflection) arrowType() arrow.Type {
	if pfr.descriptor.ContainingOneof() != nil && pfr.schemaOptions.oneOfHandler == DenseUnion {
		return arrow.DENSE_UNION
	}
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
	case arrow.DENSE_UNION:
		//u := pfr
		ub := b.(array.UnionBuilder)
		// TODO this needs to be set in the pur
		// We can't use the pfr.descriptor.Number() incase it goes above 127, the limit
		ub.Append(arrow.UnionTypeCode(pfr.descriptor.Number()))

	case arrow.DICTIONARY:
		db := b.(array.DictionaryBuilder)
		err := db.AppendValueFromString(string(fd.Enum().Values().ByNumber(pv.Enum()).Name()))
		if err != nil {
			fmt.Println(err)
		}
	case arrow.STRUCT:
		psr := pfr.asStruct()
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		for i, field := range f.Type.(*arrow.StructType).Fields() {
			SuperField{
				parent:             &psr,
				protobufReflection: psr.getFieldByName(field.Name),
				Field:              field,
				name:               field.Name,
			}.AppendValueOrNull3(sb.FieldBuilder(i), mem)
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

func AppendValueOrNull2(b array.Builder, pr protobufReflection, f arrow.Field, mem memory.Allocator) {
	v := pr.reflectValue()
	pv := pr.protoreflectValue()
	fd := pr.getDescriptor()

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
	case arrow.DENSE_UNION:
		ub := b.(array.UnionBuilder)
		//TODO this shouldn't be nil, need to get the parent from somewhere
		pur := pr.(*protobufUnionReflection)
		//pur := pr.asUnion(nil)
		if pur.whichOne() == -1 {
			ub.AppendNull()
			break
		}
		ub.Append(pur.whichOne())
		//cb := ub.Child(int(pur.whichOne()))
		//SuperField{
		//	protobufReflection: pur,
		//	Field:              arrow.Field{},
		//	name:               "",
		//}.AppendValueOrNull3(cb, mem)
		////AppendValueOrNull2(cb, pur.getFieldByName(field.Name), field, mem)
	case arrow.DICTIONARY:
		db := b.(array.DictionaryBuilder)
		err := db.AppendValueFromString(string(fd.Enum().Values().ByNumber(pv.Enum()).Name()))
		if err != nil {
			fmt.Println(err)
		}
	case arrow.STRUCT:
		psr := pr.asStruct()
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		for i, field := range f.Type.(*arrow.StructType).Fields() {
			AppendValueOrNull2(sb.FieldBuilder(i), psr.getFieldByName(field.Name), field, mem)
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
		field := f.Type.(*arrow.ListType).ElemField()
		for li := range pr.asList().generateListItems() {
			AppendValueOrNull2(lb.ValueBuilder(), li, field, mem)
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

		for kvp := range pr.asMap().generateKeyValuePairs() {
			AppendValueOrNull2(mb.KeyBuilder(), kvp.k, f.Type.(*arrow.MapType).KeyField(), mem)
			AppendValueOrNull2(mb.ItemBuilder(), kvp.v, f.Type.(*arrow.MapType).ItemField(), mem)
		}
	default:
		fmt.Printf("No logic for type %s", b.Type().ID())
	}

}

func (sf SuperField) AppendValueOrNull3(b array.Builder, mem memory.Allocator) {
	v := sf.reflectValue()
	pv := sf.protoreflectValue()
	fd := sf.getDescriptor()

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
	case arrow.DENSE_UNION:
		ub := b.(array.UnionBuilder)
		//TODO this shouldn't be nil, need to get the parent from somewhere
		pur := sf.asUnion()
		if pur.whichOne() == -1 {
			ub.AppendNull()
			break
		}
		ub.Append(pur.whichOne())
		cb := ub.Child(int(pur.whichOne()))
		fmt.Println(pur.whichOne())
		fmt.Printf("%+v\n", pur.getField())
		SuperField{
			parent:             sf.parent,
			protobufReflection: pur.getField(),
			Field:              arrow.Field{},
			name:               pur.name(),
		}.AppendValueOrNull3(cb, mem)

		//AppendValueOrNull2(cb, pur., field, mem)
		//AppendValueOrNull2(cb, pur.getFieldByName(field.Name), field, mem)
	case arrow.DICTIONARY:
		db := b.(array.DictionaryBuilder)
		err := db.AppendValueFromString(string(fd.Enum().Values().ByNumber(pv.Enum()).Name()))
		if err != nil {
			fmt.Println(err)
		}
	case arrow.STRUCT:
		psr := sf.asStruct()
		sb := b.(*array.StructBuilder)
		sb.Append(true)
		for i, field := range sf.Field.Type.(*arrow.StructType).Fields() {
			SuperField{
				parent:             sf.parent,
				protobufReflection: psr.getFieldByName(field.Name),
				Field:              field,
				name:               psr.getFieldByName(field.Name).name(),
			}.AppendValueOrNull3(sb.FieldBuilder(i), mem)
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
				protobufReflection: li,
				Field:              sf.Field.Type.(*arrow.ListType).ElemField(),
				name:               li.name(),
			}.AppendValueOrNull3(lb.ValueBuilder(), mem)
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
				protobufReflection: kvp.k,
				Field:              sf.Field.Type.(*arrow.MapType).KeyField(),
				name:               kvp.k.name(),
			}.AppendValueOrNull3(mb.KeyBuilder(), mem)
			SuperField{
				parent:             sf.parent,
				protobufReflection: kvp.v,
				Field:              sf.Field.Type.(*arrow.MapType).ItemField(),
				name:               kvp.v.name(),
			}.AppendValueOrNull3(mb.ItemBuilder(), mem)
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

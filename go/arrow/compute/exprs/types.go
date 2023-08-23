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
	"fmt"
	"hash/maphash"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/extensions"
	"github.com/substrait-io/substrait-go/types"
)

const (
	// URI for official Arrow Substrait Extension Types
	ArrowExtTypesUri          = "https://github.com/apache/arrow/blob/main/format/substrait/extension_types.yaml"
	SubstraitDefaultURIPrefix = extensions.SubstraitDefaultURIPrefix
	// URI for official Substrait Arithemetic funcs extensions
	SubstraitArithmeticFuncsURI = SubstraitDefaultURIPrefix + "functions_arithmetic.yaml"
	// URI for official Substrait Comparison funcs extensions
	SubstraitComparisonFuncsURI = SubstraitDefaultURIPrefix + "functions_comparison.yaml"

	TimestampTzTimezone = "UTC"
)

var hashSeed maphash.Seed

// the default extension registry that will contain the Arrow extension
// type variations and types.
var DefaultExtensionIDRegistry = NewExtensionIDRegistry()

func init() {
	hashSeed = maphash.MakeSeed()

	types := []struct {
		dt   arrow.DataType
		name string
	}{
		{arrow.PrimitiveTypes.Uint8, "u8"},
		{arrow.PrimitiveTypes.Uint16, "u16"},
		{arrow.PrimitiveTypes.Uint32, "u32"},
		{arrow.PrimitiveTypes.Uint64, "u64"},
		{arrow.FixedWidthTypes.Float16, "fp16"},
		{arrow.Null, "null"},
		{arrow.FixedWidthTypes.MonthInterval, "interval_month"},
		{arrow.FixedWidthTypes.DayTimeInterval, "interval_day_milli"},
		{arrow.FixedWidthTypes.MonthDayNanoInterval, "interval_month_day_nano"},
	}

	for _, t := range types {
		err := DefaultExtensionIDRegistry.RegisterType(extensions.ID{
			URI: ArrowExtTypesUri, Name: t.name}, t.dt)
		if err != nil {
			panic(err)
		}
	}

	for _, fn := range []string{"add", "subtract", "multiply", "divide", "power", "sqrt", "abs"} {
		err := DefaultExtensionIDRegistry.AddSubstraitScalarToArrow(
			extensions.ID{URI: SubstraitArithmeticFuncsURI, Name: fn},
			decodeOptionlessOverflowableArithmetic(fn))
		if err != nil {
			panic(err)
		}
	}

	for _, fn := range []string{"add", "subtract", "multiply", "divide"} {
		err := DefaultExtensionIDRegistry.AddArrowToSubstrait(fn,
			encodeOptionlessOverflowableArithmetic(extensions.ID{
				URI: SubstraitArithmeticFuncsURI, Name: fn}))
		if err != nil {
			panic(err)
		}
	}

	for _, fn := range []string{"equal", "not_equal", "lt", "lte", "gt", "gte"} {
		err := DefaultExtensionIDRegistry.AddSubstraitScalarToArrow(
			extensions.ID{URI: SubstraitComparisonFuncsURI, Name: fn},
			simpleMapSubstraitToArrowFunc)
		if err != nil {
			panic(err)
		}
	}

	for _, fn := range []string{"equal", "not_equal", "less", "less_equal", "greater", "greater_equal"} {
		err := DefaultExtensionIDRegistry.AddArrowToSubstrait(fn,
			simpleMapArrowToSubstraitFunc(SubstraitComparisonFuncsURI))
		if err != nil {
			panic(err)
		}
	}
}

type overflowBehavior string

const (
	overflowSILENT   = "SILENT"
	overflowSATURATE = "SATURATE"
	overflowERROR    = "ERROR"
)

type enumParser[typ ~string] struct {
	values map[typ]struct{}
}

func (e *enumParser[typ]) parse(v string) (typ, error) {
	out := typ(v)
	if _, ok := e.values[out]; ok {
		return out, nil
	}
	return "", arrow.ErrNotFound
}

var overflowParser = enumParser[overflowBehavior]{
	values: map[overflowBehavior]struct{}{
		overflowSILENT:   {},
		overflowSATURATE: {},
		overflowERROR:    {},
	},
}

func parseOption[typ ~string](sf *expr.ScalarFunction, optionName string, parser *enumParser[typ], implemented []typ, def typ) (typ, error) {
	opts := sf.GetOption(optionName)
	if len(opts) == 0 {
		return def, nil
	}

	for _, o := range opts {
		p, err := parser.parse(o)
		if err != nil {
			return def, arrow.ErrInvalid
		}
		for _, i := range implemented {
			if i == p {
				return p, nil
			}
		}
	}

	return def, arrow.ErrNotImplemented
}

type substraitToArrow = func(*expr.ScalarFunction) (fname string, opts compute.FunctionOptions, err error)
type arrowToSubstrait = func(fname string) (extensions.ID, []*types.FunctionOption, error)

var substraitToArrowFuncMap = map[string]string{
	"lt":  "less",
	"gt":  "greater",
	"lte": "less_equal",
	"gte": "greater_equal",
}

var arrowToSubstraitFuncMap = map[string]string{
	"less":          "lt",
	"greater":       "gt",
	"less_equal":    "lte",
	"greater_equal": "gte",
}

func simpleMapSubstraitToArrowFunc(sf *expr.ScalarFunction) (fname string, opts compute.FunctionOptions, err error) {
	fname, _, _ = strings.Cut(sf.Name(), ":")
	f, ok := substraitToArrowFuncMap[fname]
	if ok {
		fname = f
	}
	return
}

func simpleMapArrowToSubstraitFunc(uri string) arrowToSubstrait {
	return func(fname string) (extensions.ID, []*types.FunctionOption, error) {
		f, ok := arrowToSubstraitFuncMap[fname]
		if ok {
			fname = f
		}
		return extensions.ID{URI: uri, Name: fname}, nil, nil
	}
}

func decodeOptionlessOverflowableArithmetic(n string) substraitToArrow {
	return func(sf *expr.ScalarFunction) (fname string, opts compute.FunctionOptions, err error) {
		overflow, err := parseOption(sf, "overflow", &overflowParser, []overflowBehavior{overflowSILENT, overflowERROR}, overflowSILENT)
		if err != nil {
			return n, nil, err
		}

		switch overflow {
		case overflowSILENT:
			return n + "_unchecked", nil, nil
		case overflowERROR:
			return n, nil, nil
		default:
			return n, nil, arrow.ErrNotImplemented
		}
	}
}

func encodeOptionlessOverflowableArithmetic(id extensions.ID) arrowToSubstrait {
	return func(fname string) (extensions.ID, []*types.FunctionOption, error) {
		fn, _, ok := strings.Cut(fname, ":")
		if ok {
			id.Name = fname
			fname = fn
		}

		opts := make([]*types.FunctionOption, 0, 1)
		if strings.HasSuffix(fname, "_unchecked") {
			opts = append(opts, &types.FunctionOption{
				Name: "overflow", Preference: []string{"SILENT"}})
		} else {
			opts = append(opts, &types.FunctionOption{
				Name: "overflow", Preference: []string{"ERROR"}})
		}

		return id, opts, nil
	}
}

// NewExtensionSetDefault is a convenince function to create a new extension
// set using the Default arrow extension ID registry.
//
// See NewExtensionSet for more info.
func NewExtensionSetDefault(set expr.ExtensionRegistry) ExtensionIDSet {
	return &extensionSet{ExtensionRegistry: set, reg: DefaultExtensionIDRegistry}
}

// NewExtensionSet creates a new extension set given a substrait extension registry,
// and an Arrow <--> Substrait registry for mapping substrait extensions to
// their Arrow equivalents. This extension set can then be used to manage a
// particular set of extensions in use by an expression or plan, so when
// serializing you only need to serialize the extensions that have been
// inserted into the extension set.
func NewExtensionSet(set expr.ExtensionRegistry, reg *ExtensionIDRegistry) ExtensionIDSet {
	return &extensionSet{ExtensionRegistry: set, reg: reg}
}

type extensionSet struct {
	expr.ExtensionRegistry
	reg *ExtensionIDRegistry
}

func (e *extensionSet) GetArrowRegistry() *ExtensionIDRegistry       { return e.reg }
func (e *extensionSet) GetSubstraitRegistry() expr.ExtensionRegistry { return e.ExtensionRegistry }

func (e *extensionSet) DecodeTypeArrow(anchor uint32) (extensions.ID, arrow.DataType, bool) {
	id, ok := e.Set.DecodeType(anchor)
	if !ok {
		if id, ok = e.Set.DecodeTypeVariation(anchor); !ok {
			return id, nil, false
		}
	}

	dt, ok := e.reg.GetTypeByID(id)
	return id, dt, ok
}

func (e *extensionSet) DecodeFunction(ref uint32) (extensions.ID, substraitToArrow, bool) {
	id, ok := e.Set.DecodeFunc(ref)
	if !ok {
		return id, nil, false
	}

	conv, ok := e.reg.GetSubstraitScalarToArrow(id)
	if !ok {
		id.Name, _, ok = strings.Cut(id.Name, ":")
		if ok {
			conv, ok = e.reg.GetSubstraitScalarToArrow(id)
		}
	}
	return id, conv, ok
}

func (e *extensionSet) EncodeTypeVariation(dt arrow.DataType) (extensions.ID, uint32, bool) {
	id, ok := e.reg.GetIDByType(dt)
	if !ok {
		return extensions.ID{}, 0, false
	}

	return id, e.Set.GetTypeVariationAnchor(id), true
}

func (e *extensionSet) EncodeType(dt arrow.DataType) (extensions.ID, uint32, bool) {
	id, ok := e.reg.GetIDByType(dt)
	if !ok {
		return extensions.ID{}, 0, false
	}

	return id, e.Set.GetTypeAnchor(id), true
}

func (e *extensionSet) EncodeFunction(id extensions.ID) uint32 {
	return e.Set.GetFuncAnchor(id)
}

// ExtensionIDRegistry manages a set of mappings between Arrow types
// and functions and their substrait equivalents.
type ExtensionIDRegistry struct {
	typeList []arrow.DataType
	ids      []extensions.ID

	substraitToIdx map[extensions.ID]int
	arrowToIdx     map[uint64]int

	substraitToArrowFn map[extensions.ID]substraitToArrow
	arrowToSubstrait   map[string]arrowToSubstrait
}

// NewExtensionIDRegistry initializes a new registry for use.
func NewExtensionIDRegistry() *ExtensionIDRegistry {
	return &ExtensionIDRegistry{
		typeList:           make([]arrow.DataType, 0),
		ids:                make([]extensions.ID, 0),
		substraitToIdx:     make(map[extensions.ID]int),
		arrowToIdx:         make(map[uint64]int),
		substraitToArrowFn: make(map[extensions.ID]substraitToArrow),
		arrowToSubstrait:   make(map[string]arrowToSubstrait),
	}
}

// RegisterType creates a mapping between the given extension ID and the
// provided Arrow data type. If this extension ID or arrow type are already
// registered, an arrow.ErrInvalid error will be returned.
func (e *ExtensionIDRegistry) RegisterType(id extensions.ID, dt arrow.DataType) error {
	if _, ok := e.substraitToIdx[id]; ok {
		return fmt.Errorf("%w: type id already registered", arrow.ErrInvalid)
	}

	dthash := arrow.HashType(hashSeed, dt)
	if _, ok := e.arrowToIdx[dthash]; ok {
		return fmt.Errorf("%w: type already registered", arrow.ErrInvalid)
	}

	idx := len(e.ids)
	e.typeList = append(e.typeList, dt)
	e.ids = append(e.ids, id)
	e.substraitToIdx[id] = idx
	e.arrowToIdx[dthash] = idx
	return nil
}

// AddSubstraitScalarToArrow creates a mapping between a given extension ID
// and a function which should return the corresponding Arrow compute function
// name along with any relevant FunctionOptions based on the ScalarFunction
// instance passed to it.
//
// Any relevant options should be parsed from the ScalarFunction's options
// and used to ensure the correct arrow compute function is used and necessary
// options are passed.
func (e *ExtensionIDRegistry) AddSubstraitScalarToArrow(id extensions.ID, toArrow substraitToArrow) error {
	if _, ok := e.substraitToArrowFn[id]; ok {
		return fmt.Errorf("%w: extension id already registered as function", arrow.ErrInvalid)
	}

	e.substraitToArrowFn[id] = toArrow
	return nil
}

// AddArrowToSubstrait creates a mapping between the provided arrow compute function
// and a function which should provide the correct substrait ExtensionID and function
// options from that name.
func (e *ExtensionIDRegistry) AddArrowToSubstrait(name string, fn arrowToSubstrait) error {
	if _, ok := e.arrowToSubstrait[name]; ok {
		return fmt.Errorf("%w: function name '%s' already registered for conversion to substrait", arrow.ErrInvalid, name)
	}

	e.arrowToSubstrait[name] = fn
	return nil
}

// GetTypeByID returns the mapped arrow data type from the provided substrait
// extension id. If no mapping exists for this substrait extension id,
// the second return value will be false.
func (e *ExtensionIDRegistry) GetTypeByID(id extensions.ID) (arrow.DataType, bool) {
	idx, ok := e.substraitToIdx[id]
	if !ok {
		return nil, false
	}

	return e.typeList[idx], true
}

// GetIDByType is the inverse of GetTypeByID, returning the mapped substrait
// extension ID corresponding to the provided arrow data type. The second
// return is false if there is no mapping found.
func (e *ExtensionIDRegistry) GetIDByType(typ arrow.DataType) (extensions.ID, bool) {
	dthash := arrow.HashType(hashSeed, typ)
	idx, ok := e.arrowToIdx[dthash]
	if !ok {
		return extensions.ID{}, false
	}

	return e.ids[idx], true
}

// GetSubstraitScalarToArrow returns the mapped conversion function for a
// given substrait extension ID to convert a substrait ScalarFunction to
// the corresponding Arrow compute function call. False is returned as
// the second value if there is no mapping available.
func (e *ExtensionIDRegistry) GetSubstraitScalarToArrow(id extensions.ID) (substraitToArrow, bool) {
	conv, ok := e.substraitToArrowFn[id]
	if !ok {
		return nil, ok
	}

	return conv, true
}

// GetArrowToSubstrait returns the mapped function to convert an arrow compute
// function to the corresponding Substrait ScalarFunction extension ID and options.
// False is returned as the second value if there is no mapping found.
func (e *ExtensionIDRegistry) GetArrowToSubstrait(name string) (conv arrowToSubstrait, ok bool) {
	conv, ok = e.arrowToSubstrait[name]
	if !ok {
		fn, _, found := strings.Cut(name, ":")
		if found {
			conv, ok = e.arrowToSubstrait[fn]
		}
	}
	return
}

// ExtensionIDSet is an interface for managing the mapping between arrow
// and substrait types and function extensions.
type ExtensionIDSet interface {
	GetArrowRegistry() *ExtensionIDRegistry
	GetSubstraitRegistry() expr.ExtensionRegistry

	DecodeTypeArrow(anchor uint32) (extensions.ID, arrow.DataType, bool)
	DecodeFunction(ref uint32) (extensions.ID, substraitToArrow, bool)

	EncodeType(dt arrow.DataType) (extensions.ID, uint32, bool)
	EncodeTypeVariation(dt arrow.DataType) (extensions.ID, uint32, bool)
}

// IsNullable is a convenience method to return whether or not
// a substrait type has Nullability set to NullabilityRequired or not.
func IsNullable(t types.Type) bool {
	return t.GetNullability() != types.NullabilityRequired
}

// FieldsFromSubstrait produces a list of arrow fields from a list of
// substrait types (such as the fields of a StructType) using nextName
// to determine the names for the fields.
func FieldsFromSubstrait(typeList []types.Type, nextName func() string, ext ExtensionIDSet) (out []arrow.Field, err error) {
	out = make([]arrow.Field, len(typeList))
	for i, t := range typeList {
		out[i].Name = nextName()
		out[i].Nullable = IsNullable(t)

		if st, ok := t.(*types.StructType); ok {
			fields, err := FieldsFromSubstrait(st.Types, nextName, ext)
			if err != nil {
				return nil, err
			}
			out[i].Type = arrow.StructOf(fields...)
		} else {
			out[i].Type, _, err = FromSubstraitType(t, ext)
			if err != nil {
				return nil, err
			}
		}
	}
	return
}

// ToSubstraitType converts an arrow data type to a Substrait Type. Since
// arrow types don't have a nullable flag (it is in the arrow.Field) but
// Substrait types do, the nullability must be passed in here.
func ToSubstraitType(dt arrow.DataType, nullable bool, ext ExtensionIDSet) (types.Type, error) {
	var nullability types.Nullability
	if nullable {
		nullability = types.NullabilityNullable
	} else {
		nullability = types.NullabilityRequired
	}

	switch dt.ID() {
	case arrow.BOOL:
		return &types.BooleanType{Nullability: nullability}, nil
	case arrow.INT8:
		return &types.Int8Type{Nullability: nullability}, nil
	case arrow.INT16:
		return &types.Int16Type{Nullability: nullability}, nil
	case arrow.INT32:
		return &types.Int32Type{Nullability: nullability}, nil
	case arrow.INT64:
		return &types.Int64Type{Nullability: nullability}, nil
	case arrow.UINT8:
		_, anchor, ok := ext.EncodeTypeVariation(dt)
		if !ok {
			return nil, arrow.ErrNotFound
		}
		return &types.Int8Type{
			Nullability:      nullability,
			TypeVariationRef: anchor,
		}, nil
	case arrow.UINT16:
		_, anchor, ok := ext.EncodeTypeVariation(dt)
		if !ok {
			return nil, arrow.ErrNotFound
		}
		return &types.Int16Type{
			Nullability:      nullability,
			TypeVariationRef: anchor,
		}, nil
	case arrow.UINT32:
		_, anchor, ok := ext.EncodeTypeVariation(dt)
		if !ok {
			return nil, arrow.ErrNotFound
		}
		return &types.Int32Type{
			Nullability:      nullability,
			TypeVariationRef: anchor,
		}, nil
	case arrow.UINT64:
		_, anchor, ok := ext.EncodeTypeVariation(dt)
		if !ok {
			return nil, arrow.ErrNotFound
		}
		return &types.Int64Type{
			Nullability:      nullability,
			TypeVariationRef: anchor,
		}, nil
	case arrow.FLOAT16:
		_, anchor, ok := ext.EncodeTypeVariation(dt)
		if !ok {
			return nil, arrow.ErrNotFound
		}
		return &types.Int16Type{
			Nullability:      nullability,
			TypeVariationRef: anchor,
		}, nil
	case arrow.FLOAT32:
		return &types.Float32Type{Nullability: nullability}, nil
	case arrow.FLOAT64:
		return &types.Float64Type{Nullability: nullability}, nil
	case arrow.STRING:
		return &types.StringType{Nullability: nullability}, nil
	case arrow.BINARY:
		return &types.BinaryType{Nullability: nullability}, nil
	case arrow.DATE32:
		return &types.DateType{Nullability: nullability}, nil
	case arrow.EXTENSION:
		dt := dt.(arrow.ExtensionType)
		switch dt.ExtensionName() {
		case "uuid":
			return &types.UUIDType{Nullability: nullability}, nil
		case "fixed_char":
			return &types.FixedCharType{
				Nullability: nullability,
				Length:      int32(dt.StorageType().(*arrow.FixedSizeBinaryType).ByteWidth),
			}, nil
		case "varchar":
			return &types.VarCharType{Nullability: nullability, Length: -1}, nil
		case "interval_year":
			return &types.IntervalYearType{Nullability: nullability}, nil
		case "interval_day":
			return &types.IntervalDayType{Nullability: nullability}, nil
		default:
			_, anchor, ok := ext.EncodeType(dt)
			if !ok {
				return nil, arrow.ErrNotFound
			}
			return &types.UserDefinedType{
				Nullability:   nullability,
				TypeReference: anchor,
			}, nil
		}
	case arrow.FIXED_SIZE_BINARY:
		return &types.FixedBinaryType{Nullability: nullability,
			Length: int32(dt.(*arrow.FixedSizeBinaryType).ByteWidth)}, nil
	case arrow.DECIMAL128, arrow.DECIMAL256:
		dt := dt.(arrow.DecimalType)
		return &types.DecimalType{Nullability: nullability,
			Precision: dt.GetPrecision(), Scale: dt.GetScale()}, nil
	case arrow.STRUCT:
		dt := dt.(*arrow.StructType)
		fields := make([]types.Type, len(dt.Fields()))
		var err error
		for i, f := range dt.Fields() {
			fields[i], err = ToSubstraitType(f.Type, f.Nullable, ext)
			if err != nil {
				return nil, err
			}
		}

		return &types.StructType{
			Nullability: nullability,
			Types:       fields,
		}, nil
	case arrow.LIST, arrow.FIXED_SIZE_LIST, arrow.LARGE_LIST:
		dt := dt.(arrow.NestedType)
		elemType, err := ToSubstraitType(dt.Fields()[0].Type, dt.Fields()[0].Nullable, ext)
		if err != nil {
			return nil, err
		}
		return &types.ListType{
			Nullability: nullability,
			Type:        elemType,
		}, nil
	case arrow.MAP:
		dt := dt.(*arrow.MapType)
		keyType, err := ToSubstraitType(dt.KeyType(), false, ext)
		if err != nil {
			return nil, err
		}
		valueType, err := ToSubstraitType(dt.ItemType(), dt.ItemField().Nullable, ext)
		if err != nil {
			return nil, err
		}

		return &types.MapType{
			Nullability: nullability,
			Key:         keyType,
			Value:       valueType,
		}, nil
	}

	return nil, arrow.ErrNotImplemented
}

// FromSubstraitType returns the appropriate Arrow data type for the given
// substrait type, using the extension set if necessary.
// Since Substrait types contain their nullability also, the nullability
// returned along with the data type.
func FromSubstraitType(t types.Type, ext ExtensionIDSet) (arrow.DataType, bool, error) {
	nullable := IsNullable(t)

	if t.GetTypeVariationReference() > 0 {
		_, dt, ok := ext.DecodeTypeArrow(t.GetTypeVariationReference())
		if ok {
			return dt, nullable, nil
		}
	}

	switch t := t.(type) {
	case *types.BooleanType:
		return arrow.FixedWidthTypes.Boolean, nullable, nil
	case *types.Int8Type:
		return arrow.PrimitiveTypes.Int8, nullable, nil
	case *types.Int16Type:
		return arrow.PrimitiveTypes.Int16, nullable, nil
	case *types.Int32Type:
		return arrow.PrimitiveTypes.Int32, nullable, nil
	case *types.Int64Type:
		return arrow.PrimitiveTypes.Int64, nullable, nil
	case *types.Float32Type:
		return arrow.PrimitiveTypes.Float32, nullable, nil
	case *types.Float64Type:
		return arrow.PrimitiveTypes.Float64, nullable, nil
	case *types.StringType:
		return arrow.BinaryTypes.String, nullable, nil
	case *types.BinaryType:
		return arrow.BinaryTypes.Binary, nullable, nil
	case *types.TimestampType:
		return &arrow.TimestampType{Unit: arrow.Microsecond}, nullable, nil
	case *types.TimestampTzType:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: TimestampTzTimezone},
			nullable, nil
	case *types.DateType:
		return arrow.FixedWidthTypes.Date32, nullable, nil
	case *types.TimeType:
		return &arrow.Time64Type{Unit: arrow.Microsecond}, nullable, nil
	case *types.IntervalYearType:
		return intervalYear(), nullable, nil
	case *types.IntervalDayType:
		return intervalDay(), nullable, nil
	case *types.UUIDType:
		return uuid(), nullable, nil
	case *types.FixedCharType:
		return fixedChar(t.Length), nullable, nil
	case *types.VarCharType:
		return varChar(t.Length), nullable, nil
	case *types.FixedBinaryType:
		return &arrow.FixedSizeBinaryType{ByteWidth: int(t.Length)}, nullable, nil
	case *types.DecimalType:
		return &arrow.Decimal128Type{
			Precision: t.Precision,
			Scale:     t.Scale,
		}, nullable, nil
	case *types.StructType:
		i := 0
		fields, err := FieldsFromSubstrait(t.Types, func() string {
			i++
			return strconv.Itoa(i)
		}, ext)
		if err != nil {
			return nil, false, err
		}

		return arrow.StructOf(fields...), nullable, nil
	case *types.ListType:
		elem, elemNullable, err := FromSubstraitType(t.Type, ext)
		if err != nil {
			return nil, false, err
		}
		return arrow.ListOfField(arrow.Field{Name: "item", Type: elem, Nullable: elemNullable}),
			nullable, nil
	case *types.MapType:
		key, keyNullable, err := FromSubstraitType(t.Key, ext)
		if err != nil {
			return nil, false, err
		}
		if keyNullable {
			return nil, false, fmt.Errorf("%w: encountered nullable key field when converting to arrow.Map",
				arrow.ErrInvalid)
		}

		value, valueNullable, err := FromSubstraitType(t.Value, ext)
		if err != nil {
			return nil, false, err
		}
		ret := arrow.MapOf(key, value)
		ret.SetItemNullable(valueNullable)
		return ret, nullable, nil
	case *types.UserDefinedType:
		anchor := t.TypeReference
		_, dt, ok := ext.DecodeTypeArrow(anchor)
		if !ok {
			return nil, false, arrow.ErrNotImplemented
		}
		return dt, nullable, nil
	}

	return nil, false, arrow.ErrNotImplemented
}

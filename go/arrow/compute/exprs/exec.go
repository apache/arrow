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
	"context"
	"fmt"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/compute/exec"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/endian"
	"github.com/apache/arrow/go/v14/arrow/internal/debug"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/arrow/scalar"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/extensions"
	"github.com/substrait-io/substrait-go/types"
)

func makeExecBatch(ctx context.Context, schema *arrow.Schema, partial compute.Datum) (out compute.ExecBatch, err error) {
	// cleanup if we get an error
	defer func() {
		if err != nil {
			for _, v := range out.Values {
				if v != nil {
					v.Release()
				}
			}
		}
	}()

	if partial.Kind() == compute.KindRecord {
		partialBatch := partial.(*compute.RecordDatum).Value
		batchSchema := partialBatch.Schema()

		out.Values = make([]compute.Datum, len(schema.Fields()))
		out.Len = partialBatch.NumRows()

		for i, field := range schema.Fields() {
			idxes := batchSchema.FieldIndices(field.Name)
			switch len(idxes) {
			case 0:
				out.Values[i] = compute.NewDatum(scalar.MakeNullScalar(field.Type))
			case 1:
				col := partialBatch.Column(idxes[0])
				if !arrow.TypeEqual(col.DataType(), field.Type) {
					// referenced field was present but didn't have expected type
					// we'll cast this case for now
					col, err = compute.CastArray(ctx, col, compute.SafeCastOptions(field.Type))
					if err != nil {
						return compute.ExecBatch{}, err
					}
					defer col.Release()
				}
				out.Values[i] = compute.NewDatum(col)
			default:
				err = fmt.Errorf("%w: exec batch field '%s' ambiguous, more than one match",
					arrow.ErrInvalid, field.Name)
				return compute.ExecBatch{}, err
			}
		}
		return
	}

	part, ok := partial.(compute.ArrayLikeDatum)
	if !ok {
		return out, fmt.Errorf("%w: MakeExecBatch from %s", arrow.ErrNotImplemented, partial)
	}

	// wasteful but useful for testing
	if part.Type().ID() == arrow.STRUCT {
		switch part := part.(type) {
		case *compute.ArrayDatum:
			arr := part.MakeArray().(*array.Struct)
			defer arr.Release()

			batch := array.RecordFromStructArray(arr, nil)
			defer batch.Release()
			return makeExecBatch(ctx, schema, compute.NewDatumWithoutOwning(batch))
		case *compute.ScalarDatum:
			out.Len = 1
			out.Values = make([]compute.Datum, len(schema.Fields()))

			s := part.Value.(*scalar.Struct)
			dt := s.Type.(*arrow.StructType)

			for i, field := range schema.Fields() {
				idx, found := dt.FieldIdx(field.Name)
				if !found {
					out.Values[i] = compute.NewDatum(scalar.MakeNullScalar(field.Type))
					continue
				}

				val := s.Value[idx]
				if !arrow.TypeEqual(val.DataType(), field.Type) {
					// referenced field was present but didn't have the expected
					// type. for now we'll cast this
					val, err = val.CastTo(field.Type)
					if err != nil {
						return compute.ExecBatch{}, err
					}
				}
				out.Values[i] = compute.NewDatum(val)
			}
			return
		}
	}

	return out, fmt.Errorf("%w: MakeExecBatch from %s", arrow.ErrNotImplemented, partial)
}

// ToArrowSchema takes a substrait NamedStruct and an extension set (for
// type resolution mapping) and creates the equivalent Arrow Schema.
func ToArrowSchema(base types.NamedStruct, ext ExtensionIDSet) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(base.Names))
	for i, typ := range base.Struct.Types {
		dt, nullable, err := FromSubstraitType(typ, ext)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{
			Name:     base.Names[i],
			Type:     dt,
			Nullable: nullable,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

type (
	regCtxKey struct{}
	extCtxKey struct{}
)

func WithExtensionRegistry(ctx context.Context, reg *ExtensionIDRegistry) context.Context {
	return context.WithValue(ctx, regCtxKey{}, reg)
}

func GetExtensionRegistry(ctx context.Context) *ExtensionIDRegistry {
	v, ok := ctx.Value(regCtxKey{}).(*ExtensionIDRegistry)
	if !ok {
		v = DefaultExtensionIDRegistry
	}
	return v
}

func WithExtensionIDSet(ctx context.Context, ext ExtensionIDSet) context.Context {
	return context.WithValue(ctx, extCtxKey{}, ext)
}

func GetExtensionIDSet(ctx context.Context) ExtensionIDSet {
	v, ok := ctx.Value(extCtxKey{}).(ExtensionIDSet)
	if !ok {
		return NewExtensionSet(
			expr.NewEmptyExtensionRegistry(&extensions.DefaultCollection),
			GetExtensionRegistry(ctx))
	}
	return v
}

func literalToDatum(mem memory.Allocator, lit expr.Literal, ext ExtensionIDSet) (compute.Datum, error) {
	switch v := lit.(type) {
	case *expr.PrimitiveLiteral[bool]:
		return compute.NewDatum(scalar.NewBooleanScalar(v.Value)), nil
	case *expr.PrimitiveLiteral[int8]:
		return compute.NewDatum(scalar.NewInt8Scalar(v.Value)), nil
	case *expr.PrimitiveLiteral[int16]:
		return compute.NewDatum(scalar.NewInt16Scalar(v.Value)), nil
	case *expr.PrimitiveLiteral[int32]:
		return compute.NewDatum(scalar.NewInt32Scalar(v.Value)), nil
	case *expr.PrimitiveLiteral[int64]:
		return compute.NewDatum(scalar.NewInt64Scalar(v.Value)), nil
	case *expr.PrimitiveLiteral[float32]:
		return compute.NewDatum(scalar.NewFloat32Scalar(v.Value)), nil
	case *expr.PrimitiveLiteral[float64]:
		return compute.NewDatum(scalar.NewFloat64Scalar(v.Value)), nil
	case *expr.PrimitiveLiteral[string]:
		return compute.NewDatum(scalar.NewStringScalar(v.Value)), nil
	case *expr.PrimitiveLiteral[types.Timestamp]:
		return compute.NewDatum(scalar.NewTimestampScalar(arrow.Timestamp(v.Value), &arrow.TimestampType{Unit: arrow.Microsecond})), nil
	case *expr.PrimitiveLiteral[types.TimestampTz]:
		return compute.NewDatum(scalar.NewTimestampScalar(arrow.Timestamp(v.Value),
			&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: TimestampTzTimezone})), nil
	case *expr.PrimitiveLiteral[types.Date]:
		return compute.NewDatum(scalar.NewDate32Scalar(arrow.Date32(v.Value))), nil
	case *expr.PrimitiveLiteral[types.Time]:
		return compute.NewDatum(scalar.NewTime64Scalar(arrow.Time64(v.Value), &arrow.Time64Type{Unit: arrow.Microsecond})), nil
	case *expr.PrimitiveLiteral[types.FixedChar]:
		length := int(v.Type.(*types.FixedCharType).Length)
		return compute.NewDatum(scalar.NewExtensionScalar(
			scalar.NewFixedSizeBinaryScalar(memory.NewBufferBytes([]byte(v.Value)),
				&arrow.FixedSizeBinaryType{ByteWidth: length}), fixedChar(int32(length)))), nil
	case *expr.ByteSliceLiteral[[]byte]:
		return compute.NewDatum(scalar.NewBinaryScalar(memory.NewBufferBytes(v.Value), arrow.BinaryTypes.Binary)), nil
	case *expr.ByteSliceLiteral[types.UUID]:
		return compute.NewDatum(scalar.NewExtensionScalar(scalar.NewFixedSizeBinaryScalar(
			memory.NewBufferBytes(v.Value), uuid().(arrow.ExtensionType).StorageType()), uuid())), nil
	case *expr.ByteSliceLiteral[types.FixedBinary]:
		return compute.NewDatum(scalar.NewFixedSizeBinaryScalar(memory.NewBufferBytes(v.Value),
			&arrow.FixedSizeBinaryType{ByteWidth: int(v.Type.(*types.FixedBinaryType).Length)})), nil
	case *expr.NullLiteral:
		dt, _, err := FromSubstraitType(v.Type, ext)
		if err != nil {
			return nil, err
		}
		return compute.NewDatum(scalar.MakeNullScalar(dt)), nil
	case *expr.ListLiteral:
		var elemType arrow.DataType

		values := make([]scalar.Scalar, len(v.Value))
		for i, val := range v.Value {
			d, err := literalToDatum(mem, val, ext)
			if err != nil {
				return nil, err
			}
			defer d.Release()
			values[i] = d.(*compute.ScalarDatum).Value
			if elemType != nil {
				if !arrow.TypeEqual(values[i].DataType(), elemType) {
					return nil, fmt.Errorf("%w: %s has a value whose type doesn't match the other list values",
						arrow.ErrInvalid, v)
				}
			} else {
				elemType = values[i].DataType()
			}
		}

		bldr := array.NewBuilder(memory.DefaultAllocator, elemType)
		defer bldr.Release()
		if err := scalar.AppendSlice(bldr, values); err != nil {
			return nil, err
		}
		arr := bldr.NewArray()
		defer arr.Release()
		return compute.NewDatum(scalar.NewListScalar(arr)), nil
	case *expr.MapLiteral:
		dt, _, err := FromSubstraitType(v.Type, ext)
		if err != nil {
			return nil, err
		}

		mapType, ok := dt.(*arrow.MapType)
		if !ok {
			return nil, fmt.Errorf("%w: map literal with non-map type", arrow.ErrInvalid)
		}

		keys, values := make([]scalar.Scalar, len(v.Value)), make([]scalar.Scalar, len(v.Value))
		for i, kv := range v.Value {
			k, err := literalToDatum(mem, kv.Key, ext)
			if err != nil {
				return nil, err
			}
			defer k.Release()
			scalarKey := k.(*compute.ScalarDatum).Value

			v, err := literalToDatum(mem, kv.Value, ext)
			if err != nil {
				return nil, err
			}
			defer v.Release()
			scalarValue := v.(*compute.ScalarDatum).Value

			if !arrow.TypeEqual(mapType.KeyType(), scalarKey.DataType()) {
				return nil, fmt.Errorf("%w: key type mismatch for %s, got key with type %s",
					arrow.ErrInvalid, mapType, scalarKey.DataType())
			}
			if !arrow.TypeEqual(mapType.ItemType(), scalarValue.DataType()) {
				return nil, fmt.Errorf("%w: value type mismatch for %s, got value with type %s",
					arrow.ErrInvalid, mapType, scalarValue.DataType())
			}

			keys[i], values[i] = scalarKey, scalarValue
		}

		keyBldr, valBldr := array.NewBuilder(mem, mapType.KeyType()), array.NewBuilder(mem, mapType.ItemType())
		defer keyBldr.Release()
		defer valBldr.Release()

		if err := scalar.AppendSlice(keyBldr, keys); err != nil {
			return nil, err
		}
		if err := scalar.AppendSlice(valBldr, values); err != nil {
			return nil, err
		}

		keyArr, valArr := keyBldr.NewArray(), valBldr.NewArray()
		defer keyArr.Release()
		defer valArr.Release()

		kvArr, err := array.NewStructArray([]arrow.Array{keyArr, valArr}, []string{"key", "value"})
		if err != nil {
			return nil, err
		}
		defer kvArr.Release()

		return compute.NewDatumWithoutOwning(scalar.NewMapScalar(kvArr)), nil
	case *expr.StructLiteral:
		fields := make([]scalar.Scalar, len(v.Value))
		names := make([]string, len(v.Value))

		for i, l := range v.Value {
			lit, err := literalToDatum(mem, l, ext)
			if err != nil {
				return nil, err
			}
			fields[i] = lit.(*compute.ScalarDatum).Value
		}

		s, err := scalar.NewStructScalarWithNames(fields, names)
		return compute.NewDatum(s), err
	case *expr.ProtoLiteral:
		switch v := v.Value.(type) {
		case *types.Decimal:
			if len(v.Value) != arrow.Decimal128SizeBytes {
				return nil, fmt.Errorf("%w: decimal literal had %d bytes (expected %d)",
					arrow.ErrInvalid, len(v.Value), arrow.Decimal128SizeBytes)
			}

			var val decimal128.Num
			data := (*(*[arrow.Decimal128SizeBytes]byte)(unsafe.Pointer(&val)))[:]
			copy(data, v.Value)
			if endian.IsBigEndian {
				// reverse the bytes
				for i := len(data)/2 - 1; i >= 0; i-- {
					opp := len(data) - 1 - i
					data[i], data[opp] = data[opp], data[i]
				}
			}

			return compute.NewDatum(scalar.NewDecimal128Scalar(val,
				&arrow.Decimal128Type{Precision: v.Precision, Scale: v.Scale})), nil
		case *types.UserDefinedLiteral: // not yet implemented
		case *types.IntervalYearToMonth:
			bldr := array.NewInt32Builder(memory.DefaultAllocator)
			defer bldr.Release()
			typ := intervalYear()
			bldr.Append(v.Years)
			bldr.Append(v.Months)
			arr := bldr.NewArray()
			defer arr.Release()
			return &compute.ScalarDatum{Value: scalar.NewExtensionScalar(
				scalar.NewFixedSizeListScalar(arr), typ)}, nil
		case *types.IntervalDayToSecond:
			bldr := array.NewInt32Builder(memory.DefaultAllocator)
			defer bldr.Release()
			typ := intervalDay()
			bldr.Append(v.Days)
			bldr.Append(v.Seconds)
			arr := bldr.NewArray()
			defer arr.Release()
			return &compute.ScalarDatum{Value: scalar.NewExtensionScalar(
				scalar.NewFixedSizeListScalar(arr), typ)}, nil
		case *types.VarChar:
			return compute.NewDatum(scalar.NewExtensionScalar(
				scalar.NewStringScalar(v.Value), varChar(int32(v.Length)))), nil
		}
	}

	return nil, arrow.ErrNotImplemented
}

// ExecuteScalarExpression executes the given substrait expression using the provided datum as input.
// It will first create an exec batch using the input schema and the datum.
// The datum may have missing or incorrectly ordered columns while the input schema
// should describe the expected input schema for the expression. Missing fields will
// be replaced with null scalars and incorrectly ordered columns will be re-ordered
// according to the schema.
//
// You can provide an allocator to use through the context via compute.WithAllocator.
//
// You can provide the ExtensionIDSet to use through the context via WithExtensionIDSet.
func ExecuteScalarExpression(ctx context.Context, inputSchema *arrow.Schema, expression expr.Expression, partialInput compute.Datum) (compute.Datum, error) {
	if expression == nil {
		return nil, arrow.ErrInvalid
	}

	batch, err := makeExecBatch(ctx, inputSchema, partialInput)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, v := range batch.Values {
			v.Release()
		}
	}()

	return executeScalarBatch(ctx, batch, expression, GetExtensionIDSet(ctx))
}

// ExecuteScalarSubstrait uses the provided Substrait extended expression to
// determine the expected input schema (replacing missing fields in the partial
// input datum with null scalars and re-ordering columns if necessary) and
// ExtensionIDSet to use. You can provide the extension registry to use
// through the context via WithExtensionRegistry, otherwise the default
// Arrow registry will be used. You can provide a memory.Allocator to use
// the same way via compute.WithAllocator.
func ExecuteScalarSubstrait(ctx context.Context, expression *expr.Extended, partialInput compute.Datum) (compute.Datum, error) {
	if expression == nil {
		return nil, arrow.ErrInvalid
	}

	var toExecute expr.Expression

	switch len(expression.ReferredExpr) {
	case 0:
		return nil, fmt.Errorf("%w: no referred expression to execute", arrow.ErrInvalid)
	case 1:
		if toExecute = expression.ReferredExpr[0].GetExpr(); toExecute == nil {
			return nil, fmt.Errorf("%w: measures not implemented", arrow.ErrNotImplemented)
		}
	default:
		return nil, fmt.Errorf("%w: only single referred expression implemented", arrow.ErrNotImplemented)
	}

	reg := GetExtensionRegistry(ctx)
	set := NewExtensionSet(expr.NewExtensionRegistry(expression.Extensions, &extensions.DefaultCollection), reg)
	sc, err := ToArrowSchema(expression.BaseSchema, set)
	if err != nil {
		return nil, err
	}

	return ExecuteScalarExpression(WithExtensionIDSet(ctx, set), sc, toExecute, partialInput)
}

func execFieldRef(ctx context.Context, e *expr.FieldReference, input compute.ExecBatch, ext ExtensionIDSet) (compute.Datum, error) {
	if e.Root != expr.RootReference {
		return nil, fmt.Errorf("%w: only RootReference is implemented", arrow.ErrNotImplemented)
	}

	ref, ok := e.Reference.(expr.ReferenceSegment)
	if !ok {
		return nil, fmt.Errorf("%w: only direct references are implemented", arrow.ErrNotImplemented)
	}

	expectedType, _, err := FromSubstraitType(e.GetType(), ext)
	if err != nil {
		return nil, err
	}

	var param compute.Datum
	if sref, ok := ref.(*expr.StructFieldRef); ok {
		if sref.Field < 0 || sref.Field >= int32(len(input.Values)) {
			return nil, arrow.ErrInvalid
		}
		param = input.Values[sref.Field]
		ref = ref.GetChild()
	}

	out, err := GetReferencedValue(compute.GetAllocator(ctx), ref, param, ext)
	if err == compute.ErrEmpty {
		out = compute.NewDatum(param)
	} else if err != nil {
		return nil, err
	}
	if !arrow.TypeEqual(out.(compute.ArrayLikeDatum).Type(), expectedType) {
		return nil, fmt.Errorf("%w: referenced field %s was %s, but should have been %s",
			arrow.ErrInvalid, ref, out.(compute.ArrayLikeDatum).Type(), expectedType)
	}

	return out, nil
}

func executeScalarBatch(ctx context.Context, input compute.ExecBatch, exp expr.Expression, ext ExtensionIDSet) (compute.Datum, error) {
	if !exp.IsScalar() {
		return nil, fmt.Errorf("%w: ExecuteScalarExpression cannot execute non-scalar expressions",
			arrow.ErrInvalid)
	}

	switch e := exp.(type) {
	case expr.Literal:
		return literalToDatum(compute.GetAllocator(ctx), e, ext)
	case *expr.FieldReference:
		return execFieldRef(ctx, e, input, ext)
	case *expr.Cast:
		if e.Input == nil {
			return nil, fmt.Errorf("%w: cast without argument to cast", arrow.ErrInvalid)
		}

		arg, err := executeScalarBatch(ctx, input, e.Input, ext)
		if err != nil {
			return nil, err
		}
		defer arg.Release()

		dt, _, err := FromSubstraitType(e.Type, ext)
		if err != nil {
			return nil, fmt.Errorf("%w: could not determine type for cast", err)
		}

		var opts *compute.CastOptions
		switch e.FailureBehavior {
		case types.BehaviorThrowException:
			opts = compute.UnsafeCastOptions(dt)
		case types.BehaviorUnspecified:
			return nil, fmt.Errorf("%w: cast behavior unspecified", arrow.ErrInvalid)
		case types.BehaviorReturnNil:
			return nil, fmt.Errorf("%w: cast behavior return nil", arrow.ErrNotImplemented)
		}
		return compute.CastDatum(ctx, arg, opts)
	case *expr.ScalarFunction:
		var (
			err       error
			allScalar = true
			args      = make([]compute.Datum, e.NArgs())
			argTypes  = make([]arrow.DataType, e.NArgs())
		)
		for i := 0; i < e.NArgs(); i++ {
			switch v := e.Arg(i).(type) {
			case types.Enum:
				args[i] = compute.NewDatum(scalar.NewStringScalar(string(v)))
			case expr.Expression:
				args[i], err = executeScalarBatch(ctx, input, v, ext)
				if err != nil {
					return nil, err
				}
				defer args[i].Release()

				if args[i].Kind() != compute.KindScalar {
					allScalar = false
				}
			default:
				return nil, arrow.ErrNotImplemented
			}

			argTypes[i] = args[i].(compute.ArrayLikeDatum).Type()
		}

		_, conv, ok := ext.DecodeFunction(e.FuncRef())
		if !ok {
			return nil, arrow.ErrNotImplemented
		}

		fname, opts, err := conv(e)
		if err != nil {
			return nil, err
		}

		ectx := compute.GetExecCtx(ctx)
		fn, ok := ectx.Registry.GetFunction(fname)
		if !ok {
			return nil, arrow.ErrInvalid
		}

		if fn.Kind() != compute.FuncScalar {
			return nil, arrow.ErrInvalid
		}

		k, err := fn.DispatchBest(argTypes...)
		if err != nil {
			return nil, err
		}

		kctx := &exec.KernelCtx{Ctx: ctx, Kernel: k}
		init := k.GetInitFn()
		kinitArgs := exec.KernelInitArgs{Kernel: k, Inputs: argTypes, Options: opts}
		if init != nil {
			kctx.State, err = init(kctx, kinitArgs)
			if err != nil {
				return nil, err
			}
		}

		executor := compute.NewScalarExecutor()
		if err := executor.Init(kctx, kinitArgs); err != nil {
			return nil, err
		}

		batch := compute.ExecBatch{Values: args}
		if allScalar {
			batch.Len = 1
		} else {
			batch.Len = input.Len
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan compute.Datum, ectx.ExecChannelSize)
		go func() {
			defer close(ch)
			if err = executor.Execute(ctx, &batch, ch); err != nil {
				cancel()
			}
		}()

		result := executor.WrapResults(ctx, ch, false)
		if err == nil {
			debug.Assert(executor.CheckResultType(result) == nil, "invalid result type")
		}

		if ctx.Err() == context.Canceled && result != nil {
			result.Release()
		}

		return result, nil
	}

	return nil, arrow.ErrNotImplemented
}

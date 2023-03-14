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
	"context"
	"fmt"
	"unsafe"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v12/arrow/decimal128"
	"github.com/apache/arrow/go/v12/arrow/endian"
	"github.com/apache/arrow/go/v12/arrow/internal/debug"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow/scalar"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/types"
)

func LiteralToDatum(lit expr.Literal, ext ExtensionIDSet) (compute.Datum, error) {
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
				&arrow.FixedSizeBinaryType{ByteWidth: length}), FixedChar(int32(length)))), nil
	case *expr.ByteSliceLiteral[[]byte]:
		return compute.NewDatum(scalar.NewBinaryScalar(memory.NewBufferBytes(v.Value), arrow.BinaryTypes.Binary)), nil
	case *expr.ByteSliceLiteral[types.UUID]:
		return compute.NewDatum(scalar.NewExtensionScalar(scalar.NewFixedSizeBinaryScalar(
			memory.NewBufferBytes(v.Value), Uuid().(arrow.ExtensionType).StorageType()), Uuid())), nil
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
		// not yet implemented, need to implement AppendScalars for builders
	case *expr.MapLiteral:
		// not yet implemented, need to implement AppendScalars for builders
	case *expr.StructLiteral:
		fields := make([]scalar.Scalar, len(v.Value))
		names := make([]string, len(v.Value))

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
			typ := IntervalYear()
			bldr.Append(v.Years)
			bldr.Append(v.Months)
			arr := bldr.NewArray()
			defer arr.Release()
			return &compute.ScalarDatum{Value: scalar.NewExtensionScalar(
				scalar.NewFixedSizeListScalar(arr), typ)}, nil
		case *types.IntervalDayToSecond:
			bldr := array.NewInt32Builder(memory.DefaultAllocator)
			defer bldr.Release()
			typ := IntervalDay()
			bldr.Append(v.Days)
			bldr.Append(v.Seconds)
			arr := bldr.NewArray()
			defer arr.Release()
			return &compute.ScalarDatum{Value: scalar.NewExtensionScalar(
				scalar.NewFixedSizeListScalar(arr), typ)}, nil
		case *types.VarChar:
			return compute.NewDatum(scalar.NewExtensionScalar(
				scalar.NewStringScalar(v.Value), VarChar(int32(v.Length)))), nil
		}
	}

	return nil, arrow.ErrNotImplemented
}

func ExecuteScalarExpression(ctx context.Context, input compute.ExecBatch, exp expr.Expression, ext ExtensionIDSet) (compute.Datum, error) {
	if !exp.IsScalar() {
		return nil, fmt.Errorf("%w: ExecuteScalarExpression cannot execute non-scalar expressions",
			arrow.ErrInvalid)
	}

	switch e := exp.(type) {
	case expr.Literal:
		return LiteralToDatum(e, ext)
	case *expr.FieldReference:
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

		out, err := GetReferencedValue(ref, param, ext)
		if err == compute.ErrEmpty {
			out = param
		} else if err != nil {
			return nil, err
		}
		if !arrow.TypeEqual(out.(compute.ArrayLikeDatum).Type(), expectedType) {
			return nil, fmt.Errorf("%w: referenced field %s was %s, but should have been %s",
				arrow.ErrInvalid, ref, out.(compute.ArrayLikeDatum).Type(), expectedType)
		}
		return out, nil
	case *expr.ScalarFunction:
		var (
			err       error
			allScalar = true
			args      = make([]compute.Datum, len(e.Args))
			argTypes  = make([]arrow.DataType, len(e.Args))
		)
		for i, a := range e.Args {
			switch v := a.(type) {
			case types.Enum:
				args[i] = compute.NewDatum(scalar.NewStringScalar(string(v)))
			case expr.Expression:
				args[i], err = ExecuteScalarExpression(ctx, input, v, ext)
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

		_, conv, ok := ext.DecodeFunction(e.FuncRef)
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

		var newArgs []compute.Datum
		// cast arguments if necessary
		for i, arg := range args {
			if !arrow.TypeEqual(argTypes[i], arg.(compute.ArrayLikeDatum).Type()) {
				if newArgs == nil {
					newArgs = make([]compute.Datum, len(args))
					copy(newArgs, args)
				}
				newArgs[i], err = compute.CastDatum(ctx, arg, compute.SafeCastOptions(argTypes[i]))
				if err != nil {
					return nil, err
				}
				defer newArgs[i].Release()
			}
		}
		if newArgs != nil {
			args = newArgs
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

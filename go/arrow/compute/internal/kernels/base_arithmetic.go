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

package kernels

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v11/arrow/decimal128"
	"github.com/apache/arrow/go/v11/arrow/decimal256"
	"github.com/apache/arrow/go/v11/arrow/internal/debug"
	"golang.org/x/exp/constraints"
)

type ArithmeticOp int8

const (
	OpAdd ArithmeticOp = iota
	OpSub

	OpAddChecked
	OpSubChecked
)

func getGoArithmeticBinary[T exec.NumericTypes](op func(a, b T, e *error) T) binaryOps[T, T, T] {
	return binaryOps[T, T, T]{
		arrArr: func(_ *exec.KernelCtx, left, right, out []T) error {
			var err error
			for i := range out {
				out[i] = op(left[i], right[i], &err)
			}
			return err
		},
		arrScalar: func(ctx *exec.KernelCtx, left []T, right T, out []T) error {
			var err error
			for i := range out {
				out[i] = op(left[i], right, &err)
			}
			return err
		},
		scalarArr: func(ctx *exec.KernelCtx, left T, right, out []T) error {
			var err error
			for i := range out {
				out[i] = op(left, right[i], &err)
			}
			return err
		},
	}
}

var errOverflow = fmt.Errorf("%w: overflow", arrow.ErrInvalid)

func getGoArithmeticBinaryOpIntegral[T exec.UintTypes | exec.IntTypes](op ArithmeticOp) exec.ArrayKernelExec {
	switch op {
	case OpAdd:
		return ScalarBinary(getGoArithmeticBinary(func(a, b T, _ *error) T { return a + b }))
	case OpSub:
		return ScalarBinary(getGoArithmeticBinary(func(a, b T, _ *error) T { return a - b }))
	case OpAddChecked:
		shiftBy := (SizeOf[T]() * 8) - 1
		// ie: uint32 does a >> 31 at the end, int32 does >> 30
		if ^T(0) < 0 {
			shiftBy--
		}
		return ScalarBinaryNotNull(func(_ *exec.KernelCtx, a, b T, e *error) (out T) {
			out = a + b
			// see math/bits/bits.go Add64 for explanation of logic
			carry := ((a & b) | ((a | b) &^ out)) >> shiftBy
			if carry > 0 {
				*e = errOverflow
			}
			return
		})
	case OpSubChecked:
		shiftBy := (SizeOf[T]() * 8) - 1
		// ie: uint32 does a >> 31 at the end, int32 does >> 30
		if ^T(0) < 0 {
			shiftBy--
		}
		return ScalarBinaryNotNull(func(_ *exec.KernelCtx, a, b T, e *error) (out T) {
			out = a - b
			// see math/bits/bits.go Sub64 for explanation of bit logic
			carry := ((^a & b) | (^(a ^ b) & out)) >> shiftBy
			if carry > 0 {
				*e = errOverflow
			}
			return
		})
	}
	debug.Assert(false, "invalid arithmetic op")
	return nil
}

func getGoArithmeticBinaryOpFloating[T constraints.Float](op ArithmeticOp) exec.ArrayKernelExec {
	if op >= OpAddChecked {
		op -= OpAddChecked // floating checked is the same as floating unchecked
	}
	switch op {
	case OpAdd:
		return ScalarBinary(getGoArithmeticBinary(func(a, b T, _ *error) T { return a + b }))
	case OpSub:
		return ScalarBinary(getGoArithmeticBinary(func(a, b T, _ *error) T { return a - b }))
	}
	debug.Assert(false, "invalid arithmetic op")
	return nil
}

type decOps[T decimal128.Num | decimal256.Num] struct {
	Add func(T, T) T
	Sub func(T, T) T
	Div func(T, T) T
	Mul func(T, T) T
}

var dec128Ops = decOps[decimal128.Num]{
	Add: func(a, b decimal128.Num) decimal128.Num { return a.Add(b) },
	Sub: func(a, b decimal128.Num) decimal128.Num { return a.Sub(b) },
	Mul: func(a, b decimal128.Num) decimal128.Num { return a.Mul(b) },
	Div: func(a, b decimal128.Num) decimal128.Num {
		a, _ = a.Div(b)
		return a
	},
}

var dec256Ops = decOps[decimal256.Num]{
	Add: func(a, b decimal256.Num) decimal256.Num { return a.Add(b) },
	Sub: func(a, b decimal256.Num) decimal256.Num { return a.Sub(b) },
	Mul: func(a, b decimal256.Num) decimal256.Num { return a.Mul(b) },
	Div: func(a, b decimal256.Num) decimal256.Num {
		a, _ = a.Div(b)
		return a
	},
}

func getArithmeticBinaryOpDecimalImpl[T decimal128.Num | decimal256.Num](op ArithmeticOp, fns decOps[T]) exec.ArrayKernelExec {
	if op >= OpAddChecked {
		op -= OpAddChecked // decimal128/256 checked is the same as unchecked
	}

	switch op {
	case OpAdd:
		return ScalarBinaryNotNull(func(_ *exec.KernelCtx, arg0, arg1 T, _ *error) T {
			return fns.Add(arg0, arg1)
		})
	case OpSub:
		return ScalarBinaryNotNull(func(_ *exec.KernelCtx, arg0, arg1 T, _ *error) T {
			return fns.Sub(arg0, arg1)
		})
	}
	debug.Assert(false, "unimplemented arithemtic op")
	return nil
}

func getArithmeticBinaryDecimal[T decimal128.Num | decimal256.Num](op ArithmeticOp) exec.ArrayKernelExec {
	var def T
	switch any(def).(type) {
	case decimal128.Num:
		return getArithmeticBinaryOpDecimalImpl(op, dec128Ops)
	case decimal256.Num:
		return getArithmeticBinaryOpDecimalImpl(op, dec256Ops)
	}
	panic("should never get here")
}

func ArithmeticExec(ty arrow.Type, op ArithmeticOp) exec.ArrayKernelExec {
	switch ty {
	case arrow.INT8:
		return getArithmeticBinaryOpIntegral[int8](op)
	case arrow.UINT8:
		return getArithmeticBinaryOpIntegral[uint8](op)
	case arrow.INT16:
		return getArithmeticBinaryOpIntegral[int16](op)
	case arrow.UINT16:
		return getArithmeticBinaryOpIntegral[uint16](op)
	case arrow.INT32:
		return getArithmeticBinaryOpIntegral[int32](op)
	case arrow.UINT32:
		return getArithmeticBinaryOpIntegral[uint32](op)
	case arrow.INT64, arrow.TIMESTAMP:
		return getArithmeticBinaryOpIntegral[int64](op)
	case arrow.UINT64:
		return getArithmeticBinaryOpIntegral[uint64](op)
	case arrow.FLOAT32:
		return getArithmeticBinaryOpFloating[float32](op)
	case arrow.FLOAT64:
		return getArithmeticBinaryOpFloating[float64](op)
	}
	debug.Assert(false, "invalid arithmetic type")
	return nil
}

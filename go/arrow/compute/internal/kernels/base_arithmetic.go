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

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
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
	case arrow.INT64:
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

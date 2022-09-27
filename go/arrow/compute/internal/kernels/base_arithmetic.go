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
	"math/bits"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"golang.org/x/exp/constraints"
)

type ArithmeticOp int8

const (
	OpAdd ArithmeticOp = iota
	OpAddChecked
	OpSub
	OpSubChecked
)

func getGoArithmeticBinaryOpsFloating[T constraints.Float](op ArithmeticOp) binaryOps[T, T, T] {
	Op := map[ArithmeticOp]func(a, b T, e *error) T{
		OpAdd:        func(a, b T, _ *error) T { return a + b },
		OpAddChecked: func(a, b T, _ *error) T { return a + b },
		OpSub:        func(a, b T, _ *error) T { return a - b },
		OpSubChecked: func(a, b T, _ *error) T { return a - b },
	}[op]

	return binaryOps[T, T, T]{
		arrArr: func(_ *exec.KernelCtx, left, right, out []T) error {
			var err error
			for i := range out {
				out[i] = Op(left[i], right[i], &err)
			}
			return err
		},
		arrScalar: func(ctx *exec.KernelCtx, left []T, right T, out []T) error {
			var err error
			for i := range out {
				out[i] = Op(left[i], right, &err)
			}
			return err
		},
		scalarArr: func(ctx *exec.KernelCtx, left T, right, out []T) error {
			var err error
			for i := range out {
				out[i] = Op(left, right[i], &err)
			}
			return err
		},
	}
}

func getGoArithmeticBinaryOpsIntegral[T exec.UintTypes | exec.IntTypes](op ArithmeticOp) binaryOps[T, T, T] {
	Op := map[ArithmeticOp]func(a, b T, e *error) T{
		OpAdd: func(a, b T, _ *error) T { return a + b },
		OpAddChecked: func(a, b T, e *error) T {
			out, carry := bits.Add64(uint64(a), uint64(b), 0)
			if carry > 0 {
				*e = fmt.Errorf("%w: overflow", arrow.ErrInvalid)
			}
			return T(out)
		},
		OpSub: func(a, b T, _ *error) T { return a - b },
		OpSubChecked: func(a, b T, e *error) T {
			out, carry := bits.Sub64(uint64(a), uint64(b), 0)
			if carry > 0 {
				*e = fmt.Errorf("%w: overflow", arrow.ErrInvalid)
			}
			return T(out)
		},
	}[op]

	return binaryOps[T, T, T]{
		arrArr: func(_ *exec.KernelCtx, left, right, out []T) error {
			var err error
			for i := range out {
				out[i] = Op(left[i], right[i], &err)
			}
			return err
		},
		arrScalar: func(ctx *exec.KernelCtx, left []T, right T, out []T) error {
			var err error
			for i := range out {
				out[i] = Op(left[i], right, &err)
			}
			return err
		},
		scalarArr: func(ctx *exec.KernelCtx, left T, right, out []T) error {
			var err error
			for i := range out {
				out[i] = Op(left, right[i], &err)
			}
			return err
		},
	}
}

func ArithmeticExec(ty arrow.Type, op ArithmeticOp) exec.ArrayKernelExec {
	switch ty {
	case arrow.INT8:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsIntegral[int8](op))
	case arrow.UINT8:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsIntegral[uint8](op))
	case arrow.INT16:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsIntegral[int16](op))
	case arrow.UINT16:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsIntegral[uint16](op))
	case arrow.INT32:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsIntegral[int32](op))
	case arrow.UINT32:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsIntegral[uint32](op))
	case arrow.INT64:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsIntegral[int64](op))
	case arrow.UINT64:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsIntegral[uint64](op))
	case arrow.FLOAT32:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsFloating[float32](op))
	case arrow.FLOAT64:
		return ScalarBinaryEqualTypes(getArithmeticBinaryOpsFloating[float64](op))
	}
	debug.Assert(false, "invalid arithmetic type")
	return nil
}

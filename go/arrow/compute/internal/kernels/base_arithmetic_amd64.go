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

//go:build !noasm

package kernels

import (
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"golang.org/x/exp/constraints"
	"golang.org/x/sys/cpu"
)

func getAvx2ArithmeticBinaryNumeric[T exec.NumericTypes](op ArithmeticOp) binaryOps[T, T, T] {
	typ := exec.GetType[T]()
	return binaryOps[T, T, T]{
		arrArr: func(_ *exec.KernelCtx, Arg0, Arg1, Out []T) error {
			arithmeticAvx2(typ, op, exec.GetBytes(Arg0), exec.GetBytes(Arg1), exec.GetBytes(Out), len(Out))
			return nil
		},
		arrScalar: func(_ *exec.KernelCtx, Arg0 []T, Arg1 T, Out []T) error {
			arithmeticArrScalarAvx2(typ, op, exec.GetBytes(Arg0), unsafe.Pointer(&Arg1), exec.GetBytes(Out), len(Out))
			return nil
		},
		scalarArr: func(_ *exec.KernelCtx, Arg0 T, Arg1, Out []T) error {
			arithmeticScalarArrAvx2(typ, op, unsafe.Pointer(&Arg0), exec.GetBytes(Arg1), exec.GetBytes(Out), len(Out))
			return nil
		},
	}
}

func getSSE4ArithmeticBinaryNumeric[T exec.NumericTypes](op ArithmeticOp) binaryOps[T, T, T] {
	typ := exec.GetType[T]()
	return binaryOps[T, T, T]{
		arrArr: func(_ *exec.KernelCtx, Arg0, Arg1, Out []T) error {
			arithmeticSSE4(typ, op, exec.GetBytes(Arg0), exec.GetBytes(Arg1), exec.GetBytes(Out), len(Out))
			return nil
		},
		arrScalar: func(_ *exec.KernelCtx, Arg0 []T, Arg1 T, Out []T) error {
			arithmeticArrScalarSSE4(typ, op, exec.GetBytes(Arg0), unsafe.Pointer(&Arg1), exec.GetBytes(Out), len(Out))
			return nil
		},
		scalarArr: func(_ *exec.KernelCtx, Arg0 T, Arg1, Out []T) error {
			arithmeticScalarArrSSE4(typ, op, unsafe.Pointer(&Arg0), exec.GetBytes(Arg1), exec.GetBytes(Out), len(Out))
			return nil
		},
	}
}

func getArithmeticBinaryOpIntegral[T exec.UintTypes | exec.IntTypes](op ArithmeticOp) exec.ArrayKernelExec {
	if op >= OpAddChecked {
		// integral checked funcs need to use ScalarBinaryNotNull
		return getGoArithmeticBinaryOpIntegral[T](op)
	}

	if cpu.X86.HasAVX2 {
		return ScalarBinary(getAvx2ArithmeticBinaryNumeric[T](op))
	} else if cpu.X86.HasSSE42 {
		return ScalarBinary(getSSE4ArithmeticBinaryNumeric[T](op))
	}

	return getGoArithmeticBinaryOpIntegral[T](op)
}

func getArithmeticBinaryOpFloating[T constraints.Float](op ArithmeticOp) exec.ArrayKernelExec {
	if cpu.X86.HasAVX2 {
		return ScalarBinary(getAvx2ArithmeticBinaryNumeric[T](op))
	} else if cpu.X86.HasSSE42 {
		return ScalarBinary(getSSE4ArithmeticBinaryNumeric[T](op))
	}

	return getGoArithmeticBinaryOpFloating[T](op)
}

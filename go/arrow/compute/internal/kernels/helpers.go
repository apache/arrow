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
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v10/internal/bitutils"
)

// ScalarUnary returns a kernel for performing a unary operation on
// FixedWidth types which is implemented using the passed in function
// which will receive a slice containing the raw input data along with
// a slice to populate for the output data.
//
// Note that bool is not included in exec.FixedWidthTypes since it is
// represented as a bitmap, not as a slice of bool.
func ScalarUnary[OutT, Arg0T exec.FixedWidthTypes](op func(*exec.KernelCtx, []Arg0T, []OutT) error) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		arg0 := in.Values[0].Array
		inData := exec.GetSpanValues[Arg0T](&arg0, 1)
		outData := exec.GetSpanValues[OutT](out, 1)
		return op(ctx, inData, outData)
	}
}

// ScalarUnaryBoolOutput is like ScalarUnary only it is for cases of boolean
// output. The function should take in a slice of the input type and a slice
// of bytes to fill with the output boolean bitmap.
func ScalarUnaryBoolOutput[Arg0T exec.FixedWidthTypes](op func(*exec.KernelCtx, []Arg0T, []byte) error) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		arg0 := in.Values[0].Array
		inData := exec.GetSpanValues[Arg0T](&arg0, 1)
		return op(ctx, inData, out.Buffers[1].Buf)
	}
}

// ScalarUnaryNotNullBinaryArgBoolOut creates a unary kernel that accepts
// a binary type input (Binary [offset int32], String [offset int32],
// LargeBinary [offset int64], LargeString [offset int64]) and returns
// a boolean output which is never null.
//
// It implements the handling to iterate the offsets and values calling
// the provided function on each byte slice. The provided default value
// will be used as the output for elements of the input that are null.
func ScalarUnaryNotNullBinaryArgBoolOut[OffsetT int32 | int64](defVal bool, op func(*exec.KernelCtx, []byte) (bool, error)) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		var (
			arg0        = in.Values[0].Array
			outData     = out.Buffers[1].Buf
			outPos      = 0
			arg0Offsets = exec.GetSpanOffsets[OffsetT](&arg0, 1)
			arg0Data    = arg0.Buffers[2].Buf
			bitmap      = arg0.Buffers[0].Buf
			err         error
			res         bool
		)

		bitutils.VisitBitBlocks(bitmap, arg0.Offset, arg0.Len,
			func(pos int64) {
				v := arg0Data[arg0Offsets[pos]:arg0Offsets[pos+1]]
				res, err = op(ctx, v)
				bitutil.SetBitTo(outData, int(out.Offset)+outPos, res)
				outPos++
			}, func() {
				bitutil.SetBitTo(outData, int(out.Offset)+outPos, defVal)
				outPos++
			})
		return err
	}
}

// ScalarUnaryNotNullBinaryArg creates a unary kernel that accepts
// a binary type input (Binary [offset int32], String [offset int32],
// LargeBinary [offset int64], LargeString [offset int64]) and returns
// a FixedWidthType output which is never null.
//
// It implements the handling to iterate the offsets and values calling
// the provided function on each byte slice. The zero value of the OutT
// will be used as the output for elements of the input that are null.
func ScalarUnaryNotNullBinaryArg[OutT exec.FixedWidthTypes, OffsetT int32 | int64](op func(*exec.KernelCtx, []byte) (OutT, error)) exec.ArrayKernelExec {
	return func(ctx *exec.KernelCtx, in *exec.ExecSpan, out *exec.ExecResult) error {
		var (
			arg0        = in.Values[0].Array
			outData     = exec.GetSpanValues[OutT](out, 1)
			outPos      = 0
			arg0Offsets = exec.GetSpanOffsets[OffsetT](&arg0, 1)
			def         OutT
			arg0Data    = arg0.Buffers[2].Buf
			bitmap      = arg0.Buffers[0].Buf
			err         error
		)

		bitutils.VisitBitBlocks(bitmap, arg0.Offset, arg0.Len,
			func(pos int64) {
				v := arg0Data[arg0Offsets[pos]:arg0Offsets[pos+1]]
				outData[outPos], err = op(ctx, v)
				outPos++
			}, func() {
				outData[outPos] = def
				outPos++
			})
		return err
	}
}

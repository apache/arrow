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
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
)

type CastOptions struct {
	ToType               arrow.DataType `compute:"to_type"`
	AllowIntOverflow     bool           `compute:"allow_int_overflow"`
	AllowTimeTruncate    bool           `compute:"allow_time_truncate"`
	AllowTimeOverflow    bool           `compute:"allow_time_overflow"`
	AllowDecimalTruncate bool           `compute:"allow_decimal_truncate"`
	AllowFloatTruncate   bool           `compute:"allow_float_truncate"`
	AllowInvalidUtf8     bool           `compute:"allow_invalid_utf8"`
}

func (CastOptions) TypeName() string { return "CastOptions" }

type CastState = CastOptions

func ZeroCopyCastExec(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	dt := out.Type
	*out = batch.Values[0].Array
	out.Type = dt
	return nil
}

func recursiveSetSelfAlloc(arr *exec.ArraySpan) {
	for i := range arr.Buffers {
		if len(arr.Buffers[i].Buf) > 0 {
			arr.Buffers[i].SelfAlloc = true
			if arr.Buffers[i].Owner != nil {
				arr.Buffers[i].Owner.Retain()
			}
		}
	}

	for i := range arr.Children {
		recursiveSetSelfAlloc(&arr.Children[i])
	}
}

func CastFromNull(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	arr := array.MakeArrayOfNull(exec.GetAllocator(ctx.Ctx), out.Type, int(batch.Len))
	defer arr.Release()

	out.SetMembers(arr.Data())
	recursiveSetSelfAlloc(out)
	return nil
}

func OutputAllNull(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	out.Nulls = batch.Len
	return nil
}

func canCastFromDict(id arrow.Type) bool {
	return arrow.IsPrimitive(id) || arrow.IsBaseBinary(id) || arrow.IsFixedSizeBinary(id)
}

func GetZeroCastKernel(inID arrow.Type, inType exec.InputType, out exec.OutputType) exec.ScalarKernel {
	k := exec.NewScalarKernel([]exec.InputType{inType}, out, ZeroCopyCastExec, nil)
	k.NullHandling = exec.NullComputedNoPrealloc
	k.MemAlloc = exec.MemNoPrealloc
	return k
}

func GetCommonCastKernels(outID arrow.Type, outType arrow.DataType) (out []exec.ScalarKernel) {
	out = make([]exec.ScalarKernel, 0, 2)

	kernel := exec.NewScalarKernel([]exec.InputType{exec.NewExactInput(arrow.Null)}, exec.NewOutputType(outType),
		CastFromNull, nil)
	kernel.NullHandling = exec.NullComputedNoPrealloc
	kernel.MemAlloc = exec.MemNoPrealloc
	out = append(out, kernel)

	if canCastFromDict(outID) {
		// dictionary unpacking not implemented for boolean or nested types
		// TODO dict cast
		// panic(fmt.Errorf("%w: dictionary casting", arrow.ErrNotImplemented))
	}

	// Cast from extension
	return
}

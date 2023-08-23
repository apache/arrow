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

package compute_test

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/compute"
	"github.com/apache/arrow/go/v14/arrow/compute/exec"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// This example demonstrates how to register a custom scalar function.
func Example_customFunction() {
	pool := memory.NewGoAllocator()

	ctx := context.Background()
	execCtx := compute.DefaultExecCtx()
	ctx = compute.SetExecCtx(ctx, execCtx)

	add42 := compute.NewScalarFunction("add_42", compute.Arity{
		NArgs: 1,
	}, compute.FunctionDoc{
		Summary:  "Returns the input values plus 42",
		ArgNames: []string{"input"},
	})

	if err := add42.AddNewKernel(
		[]exec.InputType{
			// We accept a single argument (array) of Int8 type.
			{
				Kind: exec.InputExact,
				Type: arrow.PrimitiveTypes.Int8,
			},
		},
		// We'll return a single Int8 array.
		exec.NewOutputType(arrow.PrimitiveTypes.Int8),
		func(ctx *exec.KernelCtx, span *exec.ExecSpan, result *exec.ExecResult) error {
			// The second buffer contains the values. Both for the input and the output arrays.
			for i, x := range span.Values[0].Array.Buffers[1].Buf {
				result.Buffers[1].Buf[i] = x + 42
			}
			return nil
		},
		nil,
	); err != nil {
		log.Fatal(err)
	}
	execCtx.Registry.AddFunction(add42, true)

	inputArrayBuilder := array.NewInt8Builder(pool)
	for i := 0; i < 16; i++ {
		inputArrayBuilder.Append(int8(i))
	}
	inputArray := inputArrayBuilder.NewArray()

	outputArrayDatum, err := compute.CallFunction(
		compute.SetExecCtx(context.Background(), execCtx),
		"add_42",
		nil,
		&compute.ArrayDatum{Value: inputArray.Data()},
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(array.NewInt8Data(outputArrayDatum.(*compute.ArrayDatum).Value).Int8Values())

	// Output:
	// [42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57]
}

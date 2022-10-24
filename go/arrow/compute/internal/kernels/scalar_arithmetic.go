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
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
)

// scalar kernel that ignores (assumed all-null inputs) and returns null
func NullToNullExec(_ *exec.KernelCtx, _ *exec.ExecSpan, _ *exec.ExecResult) error {
	return nil
}

func NullExecKernel(nargs int) exec.ScalarKernel {
	in := make([]exec.InputType, nargs)
	for i := range in {
		in[i] = exec.NewIDInput(arrow.NULL)
	}
	return exec.NewScalarKernel(in, exec.NewOutputType(arrow.Null), NullToNullExec, nil)
}

func GetArithmeticKernels(op ArithmeticOp) []exec.ScalarKernel {
	kernels := make([]exec.ScalarKernel, 0)
	for _, ty := range numericTypes {
		kernels = append(kernels, exec.NewScalarKernel(
			[]exec.InputType{exec.NewExactInput(ty), exec.NewExactInput(ty)},
			exec.NewOutputType(ty), ArithmeticExec(ty.ID(), op), nil))
	}
	return append(kernels, NullExecKernel(2))
}

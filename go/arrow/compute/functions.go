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

package compute

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/compute/internal/exec"
)

type Function interface {
	Name() string
	Kind() FuncKind
	Arity() Arity
	Doc() FunctionDoc
	NumKernels() int
	Execute(context.Context, FunctionOptions, ...Datum) (Datum, error)
	DispatchExact(...arrow.DataType) (exec.Kernel, error)
	DispatchBest(...arrow.DataType) (exec.Kernel, error)
	DefaultOptions() FunctionOptions
	Validate() error
}

type Arity struct {
	NArgs     int
	IsVarArgs bool
}

func Nullary() Arity            { return Arity{0, false} }
func Unary() Arity              { return Arity{1, false} }
func Binary() Arity             { return Arity{2, false} }
func Ternary() Arity            { return Arity{3, false} }
func VarArgs(minArgs int) Arity { return Arity{minArgs, true} }

type FunctionDoc struct {
	Summary         string
	Description     string
	ArgNames        []string
	OptionsClass    string
	OptionsRequired bool
}

var EmptyFuncDoc FunctionDoc

type FuncKind int8

const (
	FuncScalar    FuncKind = iota // Scalar
	FuncVector                    // Vector
	FuncScalarAgg                 // ScalarAggregate
	FuncHashAgg                   // HashAggregate
	FuncMeta                      // Meta
)

func ValidateFunctionSummary(summary string) error {
	if strings.Contains(summary, "\n") {
		return fmt.Errorf("%w: summary contains a newline", arrow.ErrInvalid)
	}
	if summary[len(summary)-1] == '.' {
		return fmt.Errorf("%w: summary ends with a point", arrow.ErrInvalid)
	}
	return nil
}

func ValidateFunctionDescription(desc string) error {
	if len(desc) != 0 && desc[len(desc)-1] == '\n' {
		return fmt.Errorf("%w: description ends with a newline", arrow.ErrInvalid)
	}

	const maxLineSize = 78
	for _, ln := range strings.Split(desc, "\n") {
		if len(ln) > maxLineSize {
			return fmt.Errorf("%w: description line length exceeds %d characters", arrow.ErrInvalid, maxLineSize)
		}
	}
	return nil
}

type baseFunction struct {
	name        string
	kind        FuncKind
	arity       Arity
	doc         FunctionDoc
	defaultOpts FunctionOptions
}

func (b *baseFunction) Name() string                    { return b.name }
func (b *baseFunction) Kind() FuncKind                  { return b.kind }
func (b *baseFunction) Arity() Arity                    { return b.arity }
func (b *baseFunction) Doc() FunctionDoc                { return b.doc }
func (b *baseFunction) DefaultOptions() FunctionOptions { return b.defaultOpts }
func (b *baseFunction) Validate() error {
	if b.doc.Summary == "" {
		return nil
	}

	argCount := len(b.doc.ArgNames)
	if argCount != b.arity.NArgs && !(b.arity.IsVarArgs && argCount == b.arity.NArgs+1) {
		return fmt.Errorf("in function '%s': number of argument names for function doc != function arity", b.name)
	}

	if err := ValidateFunctionSummary(b.doc.Summary); err != nil {
		return err
	}
	return ValidateFunctionDescription(b.doc.Description)
}

func checkOptions(fn Function, opts FunctionOptions) error {
	if opts == nil && fn.Doc().OptionsRequired {
		return fmt.Errorf("%w: function '%s' cannot be called without options", arrow.ErrInvalid, fn.Name())
	}
	return nil
}

func (b *baseFunction) checkArity(nargs int) error {
	switch {
	case b.arity.IsVarArgs && nargs < b.arity.NArgs:
		return fmt.Errorf("%w: varargs function '%s' needs at least %d arguments, but only %d passed",
			arrow.ErrInvalid, b.name, b.arity.NArgs, nargs)
	case !b.arity.IsVarArgs && nargs != b.arity.NArgs:
		return fmt.Errorf("%w: function '%s' accepts %d arguments but %d passed",
			arrow.ErrInvalid, b.name, b.arity.NArgs, nargs)
	}
	return nil
}

type kernelType interface {
	exec.ScalarKernel

	exec.Kernel
}

type funcImpl[KT kernelType] struct {
	baseFunction

	kernels []KT
}

func (fi *funcImpl[KT]) DispatchExact(vals ...arrow.DataType) (*KT, error) {
	if err := fi.checkArity(len(vals)); err != nil {
		return nil, err
	}

	for i := range fi.kernels {
		if fi.kernels[i].GetSig().MatchesInputs(vals) {
			return &fi.kernels[i], nil
		}
	}

	return nil, fmt.Errorf("%w: function '%s' has no kernel matching input types %s",
		arrow.ErrNotImplemented, fi.name, arrow.TypesToString(vals))
}

func (fi *funcImpl[KT]) NumKernels() int { return len(fi.kernels) }
func (fi *funcImpl[KT]) Kernels() []*KT {
	res := make([]*KT, len(fi.kernels))
	for i := range fi.kernels {
		res[i] = &fi.kernels[i]
	}
	return res
}

type ScalarFunction struct {
	funcImpl[exec.ScalarKernel]
}

func NewScalarFunction(name string, arity Arity, doc FunctionDoc) *ScalarFunction {
	return &ScalarFunction{
		funcImpl: funcImpl[exec.ScalarKernel]{
			baseFunction: baseFunction{
				name:  name,
				arity: arity,
				doc:   doc,
				kind:  FuncScalar,
			},
		},
	}
}

func (s *ScalarFunction) SetDefaultOptions(opts FunctionOptions) {
	s.defaultOpts = opts
}

func (s *ScalarFunction) DispatchExact(vals ...arrow.DataType) (exec.Kernel, error) {
	return s.funcImpl.DispatchExact(vals...)
}

func (s *ScalarFunction) DispatchBest(vals ...arrow.DataType) (exec.Kernel, error) {
	return s.DispatchExact(vals...)
}

func (s *ScalarFunction) AddNewKernel(inTypes []exec.InputType, outType exec.OutputType, execFn exec.ArrayKernelExec, init exec.KernelInitFn) error {
	if err := s.checkArity(len(inTypes)); err != nil {
		return err
	}

	if s.arity.IsVarArgs && len(inTypes) != 1 {
		return fmt.Errorf("%w: varargs signatures must have exactly one input type", arrow.ErrInvalid)
	}

	sig := &exec.KernelSignature{
		InputTypes: inTypes,
		OutType:    outType,
		IsVarArgs:  s.arity.IsVarArgs,
	}

	s.kernels = append(s.kernels, exec.NewScalarKernelWithSig(sig, execFn, init))
	return nil
}

func (s *ScalarFunction) AddKernel(k exec.ScalarKernel) error {
	if err := s.checkArity(len(k.Signature.InputTypes)); err != nil {
		return err
	}

	if s.arity.IsVarArgs && !k.Signature.IsVarArgs {
		return fmt.Errorf("%w: function accepts varargs but kernel signature does not", arrow.ErrInvalid)
	}

	s.kernels = append(s.kernels, k)
	return nil
}

func (s *ScalarFunction) Execute(ctx context.Context, opts FunctionOptions, args ...Datum) (Datum, error) {
	return execInternal(ctx, s, opts, -1, args...)
}

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
)

type Function interface {
	Name() string
	Kind() FuncKind
	Arity() Arity
	Doc() FunctionDoc
	NumKernels() int
	Execute(context.Context, FunctionOptions, ...Datum) (Datum, error)
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

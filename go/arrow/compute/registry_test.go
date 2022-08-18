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

package compute_test

import (
	"strconv"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v10/arrow/compute"
	"github.com/apache/arrow/go/v10/arrow/scalar"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

var registry compute.FunctionRegistry

func init() {
	// make tests fail if there's a problem initializing the global
	// function registry
	registry = compute.GetFunctionRegistry()
}

func TestRegistryBasics(t *testing.T) {
	tests := []struct {
		name          string
		factory       func() compute.FunctionRegistry
		nfuncs        int
		expectedNames []string
	}{
		{"default", compute.NewRegistry, 0, []string{}},
		{"nested", func() compute.FunctionRegistry {
			return compute.NewChildRegistry(registry)
		}, registry.NumFunctions(), registry.GetFunctionNames()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := tt.factory()
			assert.Equal(t, tt.nfuncs, registry.NumFunctions())

			fn := compute.NewScalarFunction("f1", compute.Unary(), compute.EmptyFuncDoc)
			assert.True(t, registry.AddFunction(fn, false))
			assert.Equal(t, tt.nfuncs+1, registry.NumFunctions())

			f1, ok := registry.GetFunction("f1")
			assert.True(t, ok)
			assert.Same(t, fn, f1)

			// non-existent
			_, ok = registry.GetFunction("f2")
			assert.False(t, ok)

			// name collision
			f2 := compute.NewScalarFunction("f1", compute.Unary(), compute.EmptyFuncDoc)
			assert.False(t, registry.AddFunction(f2, false))

			// allow overwriting
			assert.True(t, registry.AddFunction(f2, true))
			f1, ok = registry.GetFunction("f1")
			assert.True(t, ok)
			assert.Same(t, f2, f1)

			expected := append(tt.expectedNames, "f1")
			slices.Sort(expected)
			assert.Equal(t, expected, registry.GetFunctionNames())

			// aliases
			assert.False(t, registry.AddAlias("f33", "f3")) // doesn't exist
			assert.True(t, registry.AddAlias("f11", "f1"))
			f1, ok = registry.GetFunction("f11")
			assert.True(t, ok)
			assert.Same(t, f2, f1)
		})
	}
}

func TestRegistry(t *testing.T) {
	defaultRegistry := registry
	t.Run("RegisterTempFunctions", func(t *testing.T) {
		const rounds = 3
		for i := 0; i < rounds; i++ {
			registry := compute.NewChildRegistry(registry)
			for _, v := range []string{"f1", "f2"} {
				fn := compute.NewScalarFunction(v, compute.Unary(), compute.EmptyFuncDoc)
				assert.True(t, registry.CanAddFunction(fn, false))
				assert.True(t, registry.AddFunction(fn, false))
				assert.False(t, registry.CanAddFunction(fn, false))
				assert.False(t, registry.AddFunction(fn, false))
				assert.True(t, defaultRegistry.CanAddFunction(fn, false))
			}
		}
	})

	t.Run("RegisterTempAliases", func(t *testing.T) {
		funcNames := defaultRegistry.GetFunctionNames()
		const rounds = 3
		for i := 0; i < rounds; i++ {
			registry := compute.NewChildRegistry(registry)
			for _, funcName := range funcNames {
				alias := "alias_of_" + funcName
				_, ok := registry.GetFunction(alias)
				assert.False(t, ok)
				assert.True(t, registry.CanAddAlias(alias, funcName))
				assert.True(t, registry.AddAlias(alias, funcName))
				_, ok = registry.GetFunction(alias)
				assert.True(t, ok)
				_, ok = defaultRegistry.GetFunction(funcName)
				assert.True(t, ok)
				_, ok = defaultRegistry.GetFunction(alias)
				assert.False(t, ok)
			}
		}
	})
}

type ExampleOptions[T int32 | uint64] struct {
	Value scalar.Scalar
}

func (ExampleOptions[T]) TypeName() string { return "example" }

type ExampleOptionsType[T int32 | uint64] struct{}

func (ExampleOptionsType[T]) TypeName() string {
	return "example" + strconv.Itoa(int(unsafe.Sizeof(T(0))))
}

func (ExampleOptionsType[T]) Compare(lhs, rhs compute.FunctionOptions) bool {
	return true
}

func (ExampleOptionsType[T]) Copy(opts compute.FunctionOptions) compute.FunctionOptions {
	o := opts.(ExampleOptions[T])
	return ExampleOptions[T]{Value: o.Value}
}

func TestRegistryTempFunctionOptionsType(t *testing.T) {
	defaultRegistry := registry
	optsTypes := []compute.FunctionOptionsType{
		ExampleOptionsType[int32]{},
		ExampleOptionsType[uint64]{},
	}
	const rounds = 3
	for i := 0; i < rounds; i++ {
		registry := compute.NewChildRegistry(defaultRegistry)
		for _, opttype := range optsTypes {
			assert.True(t, registry.CanAddFunctionOptionsType(opttype, false))
			assert.True(t, registry.AddFunctionOptionsType(opttype, false))
			assert.False(t, registry.CanAddFunctionOptionsType(opttype, false))
			assert.False(t, registry.AddFunctionOptionsType(opttype, false))
			assert.True(t, defaultRegistry.CanAddFunctionOptionsType(opttype, false))
		}
	}
}

func TestRegistryRegisterNestedFunction(t *testing.T) {
	defaultRegistry := registry
	func1 := compute.NewScalarFunction("f1", compute.Unary(), compute.EmptyFuncDoc)
	func2 := compute.NewScalarFunction("f2", compute.Unary(), compute.EmptyFuncDoc)
	const rounds = 3
	for i := 0; i < rounds; i++ {
		registry1 := compute.NewChildRegistry(defaultRegistry)

		assert.True(t, registry1.CanAddFunction(func1, false))
		assert.True(t, registry1.AddFunction(func1, false))
		for j := 0; j < rounds; j++ {
			registry2 := compute.NewChildRegistry(registry1)
			assert.True(t, registry2.CanAddFunction(func2, false))
			assert.True(t, registry2.AddFunction(func2, false))
			assert.False(t, registry2.CanAddFunction(func2, false))
			assert.False(t, registry2.AddFunction(func2, false))
			assert.True(t, defaultRegistry.CanAddFunction(func2, false))
		}
		assert.False(t, registry1.CanAddFunction(func1, false))
		assert.False(t, registry1.AddFunction(func1, false))
		assert.True(t, defaultRegistry.CanAddFunction(func1, false))
	}
}

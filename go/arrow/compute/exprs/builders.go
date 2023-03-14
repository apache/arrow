// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build go1.18

package exprs

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/compute/exprs/internal/substrait"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/extensions"
	"github.com/substrait-io/substrait-go/types"
)

var defaultCollection = &extensions.DefaultCollection

func NewScalarCall(fn string, opts []*types.FunctionOption, args ...types.FuncArg) (*expr.ScalarFunction, error) {
	conv, ok := substrait.DefaultExtensionIDRegistry.GetArrowToSubstrait(fn)
	if !ok {
		return nil, arrow.ErrNotFound
	}

	id, convOpts, err := conv(fn)
	if err != nil {
		return nil, err
	}

	opts = append(opts, convOpts...)

	variant, ok := defaultCollection.GetScalarFunc(id)
	if !ok {
		return nil, arrow.ErrNotFound
	}

	argTypes := make([]types.Type, 0, len(args))
	for _, a := range args {
		if e, ok := a.(expr.Expression); ok {
			argTypes = append(argTypes, e.GetType())
		}
	}

	outType, err := variant.ResolveType(argTypes)
	if err != nil {
		return nil, err
	}

	return &expr.ScalarFunction{
		ID:          id,
		Declaration: variant,
		Args:        args,
		Options:     opts,
		OutputType:  outType,
	}, nil
}

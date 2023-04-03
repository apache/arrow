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

package exprs

import (
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/substrait-io/substrait-go/expr"
	"github.com/substrait-io/substrait-go/types"
	"golang.org/x/exp/slices"
)

type funccall interface {
	expr.Expression
	Name() string
	FuncRef() uint32
	NArgs() int
	Arg(int) types.FuncArg
}

// flattenAssociativeChain is a helper for unboxing an expression composed
// of associative function calls. Such expressions can frequently
// be rearranged to a semantically equivalent expression for
// more optimal execution or more straightforward manipulation.
// For example, (a + ((b + 3) + 4)) is equivalent to (((4 + 3) + a) + b)
// and the latter can be trivially constant folded to ((7 + a) + b).
//
// currently only looks for Scalar/Window/Aggregate functions
//
// exprs: all "branch" expressions in a flattened chain. For example given
// (a + ((b + 3) + 4)) this would return [(a + ((b + 3) + 4)), ((b + 3) + 4), (b + 3)]
//
// fringe: all leaf expressions in a flattened chain. For example, given
// (a + ((b + 3) + 4)) the fringe would be [a, b, 3, 4]
func flattenAssociativeChain(e expr.Expression) (exprs []expr.Expression, fringe []types.FuncArg, alreadyLeftFold bool) {
	alreadyLeftFold = true

	exprs = []expr.Expression{e}
	call, ok := e.(funccall)
	if !ok {
		return
	}

	getArgs := func(c funccall) []types.FuncArg {
		out := make([]types.FuncArg, call.NArgs())
		for i := 0; i < call.NArgs(); i++ {
			out[i] = call.Arg(i)
		}
		return out
	}

	fringe = getArgs(call)

	for i := 0; i < len(fringe); {
		subCall, ok := fringe[i].(funccall)
		if !ok || subCall.FuncRef() != call.FuncRef() {
			i++
			continue
		}

		if i != 0 {
			alreadyLeftFold = false
		}

		exprs = append(exprs, subCall)
		fringe = slices.Delete(fringe, i, i+1)

		fringe = slices.Insert(fringe, i, getArgs(subCall)...)
		// no increment so we hit subCalls first argument on the next iteration
	}

	return
}

func guaranteeConjunctionMembers(guaranteedTruePredicate expr.Expression) []types.FuncArg {
	guarantee, ok := guaranteedTruePredicate.(*expr.ScalarFunction)
	if !ok || (guarantee.Name() != "and" && !strings.HasPrefix(guarantee.Name(), "and:")) {
		return []types.FuncArg{guaranteedTruePredicate}
	}

	_, fringe, _ := flattenAssociativeChain(guaranteedTruePredicate)
	return fringe
}

// extractOneFieldValue extracts an equality from an expression.
//
// Recognizes expressions of the form: "equal(a, 2)" or "is_null(a)"
func extractOneFieldValue(ext ExtensionIDSet, guarantee types.FuncArg) (*expr.FieldReference, expr.Literal, error) {
	call, ok := guarantee.(funccall)
	if !ok {
		return nil, nil, arrow.ErrNotFound
	}

	switch call.Name() {
	case "equal":
		// search for an equality condition between a field and a literal
		ref, ok := call.Arg(0).(*expr.FieldReference)
		if !ok {
			return nil, nil, arrow.ErrNotFound
		}

		lit, ok := call.Arg(1).(expr.Literal)
		if !ok {
			return nil, nil, arrow.ErrNotFound
		}

		return ref, lit, nil
	case "is_null":
		ref, ok := call.Arg(0).(*expr.FieldReference)
		if !ok {
			return nil, nil, arrow.ErrNotFound
		}

		return ref, &expr.NullLiteral{}, nil
	default:
		return nil, nil, arrow.ErrNotFound
	}
}

type knownFieldValues map[uint64]expr.Literal

func (k knownFieldValues) insert(ref *expr.FieldReference, d expr.Literal) {
	k[hashFieldRef(ref)] = d
}

func (k knownFieldValues) get(ref *expr.FieldReference) (expr.Literal, bool) {
	d, ok := k[hashFieldRef(ref)]
	return d, ok
}

// extractKnownFieldValues extracts conjunction members and fills the known ones into knownFieldValues
// returning a filtered version of the conjunction members slice
func extractKnownFieldValuesMembers(ext ExtensionIDSet, members []types.FuncArg, known knownFieldValues) ([]types.FuncArg, error) {
	out := make([]types.FuncArg, 0, len(members))
	for _, e := range members {
		ref, d, err := extractOneFieldValue(ext, e)
		if err != nil {
			if err == arrow.ErrNotFound {
				out = append(out, e)
				continue
			}
			return nil, err
		}

		known.insert(ref, d)
	}
	return out, nil
}

func extractKnownFieldValues(ext ExtensionIDSet, guaranteedTruePredicate expr.Expression) (knownFieldValues, error) {
	knownValues := make(knownFieldValues)
	members := guaranteeConjunctionMembers(guaranteedTruePredicate)
	_, err := extractKnownFieldValuesMembers(ext, members, knownValues)
	if err != nil {
		return nil, err
	}
	return knownValues, nil
}

func replaceFieldsWithKnownValues(ext ExtensionIDSet, knownValues knownFieldValues, e expr.Expression) (expr.Expression, error) {
	visitor := func(e expr.Expression) expr.Expression {
		ref, ok := e.(*expr.FieldReference)
		if !ok {
			return e
		}

		lit, found := knownValues.get(ref)
		if !found {
			return e
		}

		if lit.GetType().Equals(e.GetType()) {
			return lit
		}

		return &expr.Cast{
			Input:           lit,
			Type:            e.GetType(),
			FailureBehavior: types.BehaviorThrowException,
		}
	}

	e = visitor(e)
	_, ok := e.(funccall)
	if !ok {
		return e, nil
	}

	return e.Visit(visitor), nil
}

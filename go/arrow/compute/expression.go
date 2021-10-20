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
	"hash/maphash"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/scalar"
)

var hashSeed = maphash.MakeSeed()

type Expression interface {
	IsBound() bool
	IsScalarExpr() bool
	IsNullLiteral() bool
	IsSatisfiable() bool
	FieldRef() *FieldRef
	Descr() ValueDescr
	Type() arrow.DataType
	Hash() uint64
	Equals(Expression) bool
}

type Literal struct {
	Literal Datum
}

func (Literal) FieldRef() *FieldRef     { return nil }
func (l *Literal) Type() arrow.DataType { return l.Literal.(ArrayLikeDatum).Type() }
func (l *Literal) IsBound() bool        { return l.Type() != nil }
func (l *Literal) IsScalarExpr() bool   { return l.Literal.Kind() == KindScalar }

func (l *Literal) Equals(other Expression) bool {
	if rhs, ok := other.(*Literal); ok {
		return l.Literal.Equals(rhs.Literal)
	}
	return false
}

func (l *Literal) IsNullLiteral() bool {
	if ad, ok := l.Literal.(ArrayLikeDatum); ok {
		return ad.NullN() == ad.Len()
	}
	return true
}

func (l *Literal) IsSatisfiable() bool {
	if l.IsNullLiteral() {
		return false
	}

	if sc, ok := l.Literal.(*ScalarDatum); ok && sc.Type().ID() == arrow.BOOL {
		return sc.Value.(*scalar.Boolean).Value
	}

	return true
}

func (l *Literal) Descr() ValueDescr {
	if ad, ok := l.Literal.(ArrayLikeDatum); ok {
		return ad.Descr()
	}

	return ValueDescr{ShapeAny, nil}
}

func (l *Literal) Hash() uint64 {
	if l.IsScalarExpr() {
		return scalar.Hash(hashSeed, l.Literal.(*ScalarDatum).Value)
	}
	return 0
}

type Parameter struct {
	ref *FieldRef

	// post bind props
	descr ValueDescr
	index int
}

func (Parameter) IsNullLiteral() bool     { return false }
func (p *Parameter) Type() arrow.DataType { return p.descr.Type }
func (p *Parameter) IsBound() bool        { return p.Type() != nil }
func (p *Parameter) IsScalarExpr() bool   { return p.ref != nil }
func (p *Parameter) IsSatisfiable() bool  { return p.Type() == nil || p.Type().ID() != arrow.NULL }
func (p *Parameter) FieldRef() *FieldRef  { return p.ref }
func (p *Parameter) Descr() ValueDescr    { return p.descr }
func (p *Parameter) Hash() uint64         { return p.ref.Hash(hashSeed) }
func (p *Parameter) Equals(other Expression) bool {
	if rhs, ok := other.(*Parameter); ok {
		return p.ref.Equals(*rhs.ref)
	}

	return false
}

type Call struct {
	funcName string
	args     []Expression
	descr    ValueDescr
	options  FunctionOptions
}

func (c *Call) IsNullLiteral() bool  { return false }
func (c *Call) FieldRef() *FieldRef  { return nil }
func (c *Call) Descr() ValueDescr    { return c.descr }
func (c *Call) Type() arrow.DataType { return c.descr.Type }
func (c *Call) IsSatisfiable() bool  { return c.Type() == nil || c.Type().ID() != arrow.NULL }
func (c *Call) Hash() uint64 {
	var h maphash.Hash
	h.SetSeed(hashSeed)

	h.WriteString(c.funcName)
	hash := h.Sum64()
	for _, arg := range c.args {
		hash = hashCombine(hash, arg.Hash())
	}
	return hash
}

func (c *Call) IsScalarExpr() bool {
	for _, arg := range c.args {
		if !arg.IsScalarExpr() {
			return false
		}
	}
	return isFuncScalar(c.funcName)
}

func (c *Call) IsBound() bool {
	if c.Type() == nil {
		return false
	}

	for _, arg := range c.args {
		if !arg.IsBound() {
			return false
		}
	}
	return true
}

func (c *Call) Equals(other Expression) bool {
	rhs, ok := other.(*Call)
	if !ok {
		return false
	}

	if c.funcName != rhs.funcName || len(c.args) != len(rhs.args) {
		return false
	}

	for i := range c.args {
		if !c.args[i].Equals(rhs.args[i]) {
			return false
		}
	}

	return reflect.DeepEqual(c.options, rhs.options)
}

type FunctionOptions interface {
	TypeName() string
}

type MakeStructOptions struct {
	FieldNames       []string          `compute:"field_names"`
	FieldNullability []bool            `compute:"field_nullability"`
	FieldMetadata    []*arrow.Metadata `compute:"field_metadata"`
}

func (MakeStructOptions) TypeName() string { return "MakeStructOptions" }

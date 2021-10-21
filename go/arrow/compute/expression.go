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
	"errors"
	"hash/maphash"
	"reflect"
	"strconv"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
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
	Bind(context.Context, memory.Allocator, *arrow.Schema) (Expression, error)

	boundExpr() boundRef
}

type Literal struct {
	Literal Datum

	bound boundRef
}

func (Literal) FieldRef() *FieldRef     { return nil }
func (l *Literal) boundExpr() boundRef  { return l.bound }
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

func (l *Literal) Bind(ctx context.Context, mem memory.Allocator, schema *arrow.Schema) (Expression, error) {
	bound, _, _, err := bindExprSchema(ctx, mem, l, schema)
	if err != nil {
		return nil, err
	}

	return &Literal{l.Literal, bound}, nil
}

func (l *Literal) Release() {
	if l.bound != 0 {
		l.bound.release()
	}
}

type Parameter struct {
	ref *FieldRef

	// post bind props
	descr ValueDescr
	index int

	bound boundRef
}

func (Parameter) IsNullLiteral() bool     { return false }
func (p *Parameter) boundExpr() boundRef  { return p.bound }
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

func (p *Parameter) Bind(ctx context.Context, mem memory.Allocator, schema *arrow.Schema) (Expression, error) {
	bound, descr, index, err := bindExprSchema(ctx, mem, p, schema)
	if err != nil {
		return nil, err
	}

	return &Parameter{
		ref:   p.ref,
		index: index,
		descr: descr,
		bound: bound,
	}, nil
}

func (p *Parameter) Release() {
	if p.bound != 0 {
		p.bound.release()
	}
}

type Call struct {
	funcName string
	args     []Expression
	descr    ValueDescr
	options  FunctionOptions

	cachedHash uint64
	bound      boundRef
}

func (c *Call) boundExpr() boundRef  { return c.bound }
func (c *Call) IsNullLiteral() bool  { return false }
func (c *Call) FieldRef() *FieldRef  { return nil }
func (c *Call) Descr() ValueDescr    { return c.descr }
func (c *Call) Type() arrow.DataType { return c.descr.Type }
func (c *Call) IsSatisfiable() bool  { return c.Type() == nil || c.Type().ID() != arrow.NULL }
func (c *Call) Hash() uint64 {
	if c.cachedHash != 0 {
		return c.cachedHash
	}

	var h maphash.Hash
	h.SetSeed(hashSeed)

	h.WriteString(c.funcName)
	c.cachedHash = h.Sum64()
	for _, arg := range c.args {
		c.cachedHash = hashCombine(c.cachedHash, arg.Hash())
	}
	return c.cachedHash
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

func (c Call) Bind(ctx context.Context, mem memory.Allocator, schema *arrow.Schema) (Expression, error) {
	bound, descr, _, err := bindExprSchema(ctx, mem, &c, schema)
	if err != nil {
		return nil, err
	}

	c.descr = descr
	c.bound = bound
	return &c, nil
}

func (c *Call) Release() {
	if c.bound != 0 {
		c.bound.release()
	}
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

type NullOptions struct {
	NanIsNull bool `compute:"nan_is_null"`
}

func (NullOptions) TypeName() string { return "NullOptions" }

func NewLiteral(arg interface{}) Expression {
	return &Literal{Literal: NewDatum(arg)}
}

func NewRef(ref FieldRef) Expression {
	return &Parameter{ref: &ref, index: -1}
}

func NewFieldRef(field string) Expression {
	return NewRef(FieldRefName(field))
}

func NewCall(name string, args []Expression, opts FunctionOptions) Expression {
	return &Call{funcName: name, args: args, options: opts}
}

func Project(values []Expression, names []string) Expression {
	nulls := make([]bool, len(names))
	for i := range nulls {
		nulls[i] = true
	}
	meta := make([]*arrow.Metadata, len(names))
	return NewCall("make_struct", values,
		&MakeStructOptions{FieldNames: names, FieldNullability: nulls, FieldMetadata: meta})
}

func Equal(lhs, rhs Expression) Expression {
	return NewCall("equal", []Expression{lhs, rhs}, nil)
}

func NotEqual(lhs, rhs Expression) Expression {
	return NewCall("not_equal", []Expression{lhs, rhs}, nil)
}

func Less(lhs, rhs Expression) Expression {
	return NewCall("less", []Expression{lhs, rhs}, nil)
}

func LessEqual(lhs, rhs Expression) Expression {
	return NewCall("less_equal", []Expression{lhs, rhs}, nil)
}

func Greater(lhs, rhs Expression) Expression {
	return NewCall("greater", []Expression{lhs, rhs}, nil)
}

func GreaterEqual(lhs, rhs Expression) Expression {
	return NewCall("greater_equal", []Expression{lhs, rhs}, nil)
}

func IsNull(lhs Expression, nanIsNull bool) Expression {
	return NewCall("less", []Expression{lhs}, &NullOptions{nanIsNull})
}

func IsValid(lhs Expression) Expression {
	return NewCall("is_valid", []Expression{lhs}, nil)
}

type binop func(lhs, rhs Expression) Expression

func foldLeft(op binop, args ...Expression) Expression {
	switch len(args) {
	case 0:
		return nil
	case 1:
		return args[0]
	}

	folded := args[0]
	for _, a := range args[1:] {
		folded = op(folded, a)
	}
	return folded
}

func and(lhs, rhs Expression) Expression {
	return NewCall("and_kleene", []Expression{lhs, rhs}, nil)
}

func And(ops ...Expression) Expression {
	folded := foldLeft(and, ops...)
	if folded != nil {
		return folded
	}
	return NewLiteral(true)
}

func or(lhs, rhs Expression) Expression {
	return NewCall("or_kleene", []Expression{lhs, rhs}, nil)
}

func Or(lhs, rhs Expression, ops ...Expression) Expression {
	folded := foldLeft(or, append([]Expression{lhs, rhs}, ops...)...)
	if folded != nil {
		return folded
	}
	return NewLiteral(false)
}

func Not(expr Expression) Expression {
	return NewCall("invert", []Expression{expr}, nil)
}

// SerializeExpr serializes expressions by converting them to Metadata and
// storing this in the schema of a Record. Embedded arrays and scalars are
// stored in its columns. Finally the record is written as an IPC file
func SerializeExpr(expr Expression, mem memory.Allocator) (*memory.Buffer, error) {
	var (
		cols      []array.Interface
		metaKey   []string
		metaValue []string
		visit     func(Expression) error
	)

	addScalar := func(s scalar.Scalar) (string, error) {
		ret := len(cols)
		arr, err := scalar.MakeArrayFromScalar(s, 1, mem)
		if err != nil {
			return "", err
		}
		cols = append(cols, arr)
		return strconv.Itoa(ret), nil
	}

	visit = func(e Expression) error {
		switch e := e.(type) {
		case *Literal:
			if !e.IsScalarExpr() {
				return errors.New("not implemented: serialization of non-scalar literals")
			}
			metaKey = append(metaKey, "literal")
			s, err := addScalar(e.Literal.(*ScalarDatum).Value)
			if err != nil {
				return err
			}
			metaValue = append(metaValue, s)
		case *Parameter:
			if e.ref.Name() == "" {
				return errors.New("not implemented: serialization of non-name field_ref")
			}

			metaKey = append(metaKey, "field_ref")
			metaValue = append(metaValue, e.ref.Name())
		case *Call:
			metaKey = append(metaKey, "call")
			metaValue = append(metaValue, e.funcName)

			for _, arg := range e.args {
				visit(arg)
			}

			if e.options != nil {
				st, err := scalar.ToScalar(e.options, mem)
				if err != nil {
					return err
				}
				metaKey = append(metaKey, "options")
				s, err := addScalar(st)
				if err != nil {
					return err
				}
				metaValue = append(metaValue, s)

				for _, f := range st.(*scalar.Struct).Value {
					switch s := f.(type) {
					case releasable:
						defer s.Release()
					}
				}
			}

			metaKey = append(metaKey, "end")
			metaValue = append(metaValue, e.funcName)
		}
		return nil
	}

	if err := visit(expr); err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i].Type = c.DataType()
		defer c.Release()
	}

	metadata := arrow.NewMetadata(metaKey, metaValue)
	rec := array.NewRecord(arrow.NewSchema(fields, &metadata), cols, 1)
	defer rec.Release()

	buf := &bufferWriteSeeker{mem: mem}
	wr, err := ipc.NewFileWriter(buf, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(mem))
	if err != nil {
		return nil, err
	}

	wr.Write(rec)
	wr.Close()
	return buf.buf, nil
}

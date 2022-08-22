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

package internal

import (
	"context"
	"fmt"
	"hash/maphash"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

type ctxAllocKey struct{}

func WithAllocator(ctx context.Context, mem memory.Allocator) context.Context {
	return context.WithValue(ctx, ctxAllocKey{}, mem)
}

func GetAllocator(ctx context.Context) memory.Allocator {
	mem, ok := ctx.Value(ctxAllocKey{}).(memory.Allocator)
	if !ok {
		return memory.DefaultAllocator
	}
	return mem
}

type Kernel interface {
	GetInitFn() KernelInitFn
	GetSig() *KernelSignature
}

type KernelCtx struct {
	Ctx    context.Context
	Kernel Kernel
	State  KernelState
}

func (k *KernelCtx) Allocate(bufsize int) *memory.Buffer {
	buf := memory.NewResizableBuffer(GetAllocator(k.Ctx))
	buf.Resize(bufsize)
	return buf
}

func (k *KernelCtx) AllocateBitmap(nbits int64) *memory.Buffer {
	nbytes := bitutil.BytesForBits(nbits)
	return k.Allocate(int(nbytes))
}

type TypeMatcher interface {
	fmt.Stringer
	Matches(typ arrow.DataType) bool
	Equals(other TypeMatcher) bool
}

type sameTypeIDMatcher struct {
	accepted arrow.Type
}

func (s sameTypeIDMatcher) Matches(typ arrow.DataType) bool { return s.accepted == typ.ID() }
func (s *sameTypeIDMatcher) Equals(other TypeMatcher) bool {
	if s == other {
		return true
	}

	o, ok := other.(*sameTypeIDMatcher)
	if !ok {
		return false
	}

	return s.accepted == o.accepted
}

func (s sameTypeIDMatcher) String() string {
	return "Type::" + s.accepted.String()
}

func SameTypeID(id arrow.Type) TypeMatcher { return &sameTypeIDMatcher{id} }

type timeUnitMatcher struct {
	id   arrow.Type
	unit arrow.TimeUnit
}

func (s timeUnitMatcher) Matches(typ arrow.DataType) bool {
	if typ.ID() != s.id {
		return false
	}
	return s.unit == typ.(arrow.TemporalWithUnit).TimeUnit()
}

func (s timeUnitMatcher) String() string {
	return strings.ToLower(s.id.String()) + "(" + s.unit.String() + ")"
}

func (s *timeUnitMatcher) Equals(other TypeMatcher) bool {
	if s == other {
		return true
	}

	o, ok := other.(*timeUnitMatcher)
	if !ok {
		return false
	}
	return o.id == s.id && o.unit == s.unit
}

func TimestampTypeUnit(unit arrow.TimeUnit) TypeMatcher {
	return &timeUnitMatcher{arrow.TIMESTAMP, unit}
}
func Time32TypeUnit(unit arrow.TimeUnit) TypeMatcher {
	return &timeUnitMatcher{arrow.TIME32, unit}
}
func Time64TypeUnit(unit arrow.TimeUnit) TypeMatcher {
	return &timeUnitMatcher{arrow.TIME64, unit}
}
func DurationTypeUnit(unit arrow.TimeUnit) TypeMatcher {
	return &timeUnitMatcher{arrow.DURATION, unit}
}

type integerMatcher struct{}

func (integerMatcher) String() string                  { return "integer" }
func (integerMatcher) Matches(typ arrow.DataType) bool { return arrow.IsInteger(typ.ID()) }
func (integerMatcher) Equals(other TypeMatcher) bool {
	_, ok := other.(integerMatcher)
	return ok
}

type binaryLikeMatcher struct{}

func (binaryLikeMatcher) String() string                  { return "binary-like" }
func (binaryLikeMatcher) Matches(typ arrow.DataType) bool { return arrow.IsBinaryLike(typ.ID()) }
func (binaryLikeMatcher) Equals(other TypeMatcher) bool {
	_, ok := other.(binaryLikeMatcher)
	return ok
}

type largeBinaryLikeMatcher struct{}

func (largeBinaryLikeMatcher) String() string { return "large-binary-like" }
func (largeBinaryLikeMatcher) Matches(typ arrow.DataType) bool {
	return arrow.IsLargeBinaryLike(typ.ID())
}
func (largeBinaryLikeMatcher) Equals(other TypeMatcher) bool {
	_, ok := other.(largeBinaryLikeMatcher)
	return ok
}

type fsbLikeMatcher struct{}

func (fsbLikeMatcher) String() string                  { return "fixed-size-binary-like" }
func (fsbLikeMatcher) Matches(typ arrow.DataType) bool { return arrow.IsFixedSizeBinary(typ.ID()) }
func (fsbLikeMatcher) Equals(other TypeMatcher) bool {
	_, ok := other.(fsbLikeMatcher)
	return ok
}

func Integer() TypeMatcher             { return integerMatcher{} }
func BinaryLike() TypeMatcher          { return binaryLikeMatcher{} }
func LargeBinaryLike() TypeMatcher     { return largeBinaryLikeMatcher{} }
func FixedSizeBinaryLike() TypeMatcher { return fsbLikeMatcher{} }

type primitiveMatcher struct{}

func (primitiveMatcher) String() string                  { return "primitive" }
func (primitiveMatcher) Matches(typ arrow.DataType) bool { return arrow.IsPrimitive(typ.ID()) }
func (primitiveMatcher) Equals(other TypeMatcher) bool {
	_, ok := other.(primitiveMatcher)
	return ok
}

func Primitive() TypeMatcher { return primitiveMatcher{} }

type InputKind int8

const (
	InputAny InputKind = iota
	InputExact
	InputUseMatcher
)

type InputType struct {
	Kind    InputKind
	Type    arrow.DataType
	Matcher TypeMatcher
}

func NewExactInput(dt arrow.DataType) InputType { return InputType{Kind: InputExact, Type: dt} }
func NewMatchedInput(match TypeMatcher) InputType {
	return InputType{Kind: InputUseMatcher, Matcher: match}
}
func NewIDInput(id arrow.Type) InputType { return NewMatchedInput(SameTypeID(id)) }

func (it InputType) String() string {
	switch it.Kind {
	case InputAny:
		return "any"
	case InputUseMatcher:
		return it.Matcher.String()
	case InputExact:
		return it.Type.String()
	}
	return ""
}

func (it *InputType) Equals(other *InputType) bool {
	if it == other {
		return true
	}

	if it.Kind != other.Kind {
		return false
	}

	switch it.Kind {
	case InputAny:
		return true
	case InputExact:
		return arrow.TypeEqual(it.Type, other.Type)
	case InputUseMatcher:
		return it.Matcher.Equals(other.Matcher)
	default:
		return false
	}
}

func (it InputType) Hash() uint64 {
	var h maphash.Hash

	h.SetSeed(hashSeed)
	result := HashCombine(h.Sum64(), uint64(it.Kind))
	switch it.Kind {
	case InputExact:
		result = HashCombine(result, arrow.HashType(hashSeed, it.Type))
	}
	return result
}

func (it InputType) Matches(dt arrow.DataType) bool {
	switch it.Kind {
	case InputExact:
		return arrow.TypeEqual(it.Type, dt)
	case InputUseMatcher:
		return it.Matcher.Matches(dt)
	default:
		return true
	}
}

type ResolveKind int8

const (
	ResolveFixed ResolveKind = iota
	ResolveComputed
)

type TypeResolver = func(*KernelCtx, []arrow.DataType) (arrow.DataType, error)

type OutputType struct {
	Kind     ResolveKind
	Type     arrow.DataType
	Resolver TypeResolver
}

func NewOutputType(dt arrow.DataType) OutputType {
	return OutputType{Kind: ResolveFixed, Type: dt}
}

func NewComputedOutputType(resolver TypeResolver) OutputType {
	return OutputType{Kind: ResolveComputed, Resolver: resolver}
}

func (o OutputType) String() string {
	if o.Kind == ResolveFixed {
		return o.Type.String()
	}
	return "computed"
}

func (o OutputType) Resolve(ctx *KernelCtx, types []arrow.DataType) (arrow.DataType, error) {
	switch o.Kind {
	case ResolveFixed:
		return o.Type, nil
	}

	return o.Resolver(ctx, types)
}

type NullHandling int8

const (
	NullIntersection NullHandling = iota
	NullComputedPrealloc
	NullComputedNoPrealloc
	NullNoOutput
)

type MemAlloc int8

const (
	MemPrealloc MemAlloc = iota
	MemNoPrealloc
)

type Options interface{}

type KernelState any

type KernelInitArgs struct {
	Kernel  Kernel
	Inputs  []arrow.DataType
	Options Options
}

type KernelInitFn = func(*KernelCtx, KernelInitArgs) (KernelState, error)

type KernelSignature struct {
	InputTypes []InputType
	OutType    OutputType
	IsVarArgs  bool

	hashCode uint64
}

func (k KernelSignature) String() string {
	var b strings.Builder
	if k.IsVarArgs {
		b.WriteString("varargs[")
	} else {
		b.WriteByte('(')
	}

	for i, t := range k.InputTypes {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString(t.String())
	}
	if k.IsVarArgs {
		b.WriteString("*]")
	} else {
		b.WriteByte(')')
	}

	b.WriteString(" -> ")
	b.WriteString(k.OutType.String())
	return b.String()
}

func (k KernelSignature) Equals(other KernelSignature) bool {
	if k.IsVarArgs != other.IsVarArgs {
		return false
	}

	return slices.EqualFunc(k.InputTypes, other.InputTypes, func(e1, e2 InputType) bool {
		return e1.Equals(&e2)
	})
}

func (k *KernelSignature) Hash() uint64 {
	if k.hashCode != 0 {
		return k.hashCode
	}

	var h maphash.Hash
	h.SetSeed(hashSeed)
	result := h.Sum64()
	for _, typ := range k.InputTypes {
		result = HashCombine(result, typ.Hash())
	}
	k.hashCode = result
	return result
}

func (k KernelSignature) MatchesInputs(types []arrow.DataType) bool {
	switch k.IsVarArgs {
	case true:
		for i, t := range types {
			if !k.InputTypes[min(i, len(k.InputTypes)-1)].Matches(t) {
				return false
			}
		}
	case false:
		if len(types) != len(k.InputTypes) {
			return false
		}
		for i, t := range types {
			if !k.InputTypes[i].Matches(t) {
				return false
			}
		}
	}
	return true
}

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

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
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/compute"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
	"github.com/stretchr/testify/assert"
)

func TestTypeMatcherSameTypeID(t *testing.T) {
	matcher := compute.SameTypeID(arrow.DECIMAL128)
	assert.True(t, matcher.Matches(&arrow.Decimal128Type{Precision: 12, Scale: 2}))
	assert.False(t, matcher.Matches(arrow.PrimitiveTypes.Int8))

	assert.Equal(t, "Type::DECIMAL128", matcher.String())

	assert.True(t, matcher.Equals(matcher))
	assert.True(t, matcher.Equals(compute.SameTypeID(arrow.DECIMAL)))
	assert.False(t, matcher.Equals(compute.SameTypeID(arrow.TIMESTAMP)))
}

func TestTypeMatcherTimestampTypeUnit(t *testing.T) {
	matcher := compute.TimestampTypeUnit(arrow.Millisecond)
	matcher2 := compute.Time32TypeUnit(arrow.Millisecond)

	assert.True(t, matcher.Matches(arrow.FixedWidthTypes.Timestamp_ms))
	assert.True(t, matcher.Matches(&arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "utc"}))
	assert.False(t, matcher.Matches(arrow.FixedWidthTypes.Timestamp_s))
	assert.False(t, matcher.Matches(arrow.FixedWidthTypes.Time32ms))
	assert.True(t, matcher2.Matches(arrow.FixedWidthTypes.Time32ms))

	// check String() representation
	assert.Equal(t, "timestamp(s)", compute.TimestampTypeUnit(arrow.Second).String())
	assert.Equal(t, "timestamp(ms)", compute.TimestampTypeUnit(arrow.Millisecond).String())
	assert.Equal(t, "timestamp(us)", compute.TimestampTypeUnit(arrow.Microsecond).String())
	assert.Equal(t, "timestamp(ns)", compute.TimestampTypeUnit(arrow.Nanosecond).String())

	// equals implementation
	assert.True(t, matcher.Equals(matcher))
	assert.True(t, matcher.Equals(compute.TimestampTypeUnit(arrow.Millisecond)))
	assert.False(t, matcher.Equals(compute.TimestampTypeUnit(arrow.Microsecond)))
	assert.False(t, matcher.Equals(compute.Time32TypeUnit(arrow.Millisecond)))
}

func TestInputTypeAnyType(t *testing.T) {
	var ty compute.InputType
	assert.Equal(t, compute.InputAny, ty.Kind)
}

func TestInputType(t *testing.T) {
	ty1 := compute.NewExactInput(arrow.PrimitiveTypes.Int8)
	assert.Equal(t, compute.InputExact, ty1.Kind)
	assert.True(t, arrow.TypeEqual(arrow.PrimitiveTypes.Int8, ty1.Type))
	assert.Equal(t, "int8", ty1.String())

	ty2 := compute.NewIDInput(arrow.DECIMAL)
	assert.Equal(t, compute.InputUseMatcher, ty2.Kind)
	assert.Equal(t, "Type::DECIMAL128", ty2.String())
	assert.True(t, ty2.Matcher.Matches(&arrow.Decimal128Type{Precision: 12, Scale: 2}))
	assert.False(t, ty2.Matcher.Matches(arrow.PrimitiveTypes.Int16))

	ty3 := compute.NewMatchedInput(compute.TimestampTypeUnit(arrow.Microsecond))
	assert.Equal(t, "timestamp(us)", ty3.String())

	var ty4 compute.InputType
	assert.Equal(t, "any", ty4.String())
}

func TestInputTypeEquals(t *testing.T) {
	t1 := compute.NewExactInput(arrow.PrimitiveTypes.Int8)
	t2 := compute.NewExactInput(arrow.PrimitiveTypes.Int8)
	t3 := compute.NewExactInput(arrow.PrimitiveTypes.Int32)

	t5 := compute.NewIDInput(arrow.DECIMAL)
	t6 := compute.NewIDInput(arrow.DECIMAL)

	assert.True(t, t1.Equals(&t2))
	assert.False(t, t1.Equals(&t3))
	assert.False(t, t1.Equals(&t5))
	assert.True(t, t5.Equals(&t5))
	assert.True(t, t5.Equals(&t6))

	// for now, an ID matcher for arrow.INT32 and a ExactInput for
	// arrow.PrimitiveTypes.Int32 are treated as being different.
	// this could be made equivalent later if desireable

	// check that field metadata is excluded from equality checks
	t7 := compute.NewExactInput(arrow.ListOfField(
		arrow.Field{Name: "item", Type: arrow.BinaryTypes.String,
			Nullable: true, Metadata: arrow.NewMetadata([]string{"foo"}, []string{"bar"})}))
	t8 := compute.NewExactInput(arrow.ListOf(arrow.BinaryTypes.String))
	assert.True(t, t7.Equals(&t8))
}

func TestInputTypeHash(t *testing.T) {
	var (
		t0 compute.InputType
		t1 = compute.NewExactInput(arrow.PrimitiveTypes.Int8)
		t2 = compute.NewIDInput(arrow.DECIMAL)
	)

	// these checks try to determine first of all whether hash
	// always returns the same value, and whether the elements
	// of the type are all incorporated into the hash
	assert.Equal(t, t0.Hash(), t0.Hash())
	assert.Equal(t, t1.Hash(), t1.Hash())
	assert.Equal(t, t2.Hash(), t2.Hash())
	assert.NotEqual(t, t0.Hash(), t1.Hash())
	assert.NotEqual(t, t0.Hash(), t2.Hash())
	assert.NotEqual(t, t1.Hash(), t2.Hash())
}

func TestInputTypeMatches(t *testing.T) {
	in1 := compute.NewExactInput(arrow.PrimitiveTypes.Int8)

	assert.True(t, in1.Matches(arrow.PrimitiveTypes.Int8))
	assert.False(t, in1.Matches(arrow.PrimitiveTypes.Int16))

	in2 := compute.NewIDInput(arrow.DECIMAL)
	assert.True(t, in2.Matches(&arrow.Decimal128Type{Precision: 12, Scale: 2}))

	ty2 := &arrow.Decimal128Type{Precision: 12, Scale: 2}
	ty3 := arrow.PrimitiveTypes.Float64

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	arr2 := array.MakeArrayOfNull(mem, ty2, 1)
	arr3 := array.MakeArrayOfNull(mem, ty3, 1)
	defer arr2.Release()
	defer arr3.Release()

	scalar2, err := scalar.GetScalar(arr2, 0)
	assert.NoError(t, err)

	datumArr := compute.NewDatum(arr2)
	defer datumArr.Release()
	datumScalar := compute.NewDatum(scalar2)
	defer datumScalar.Release()

	assert.True(t, in2.MatchValue(datumArr))
	assert.True(t, in2.MatchValue(datumScalar))
	assert.False(t, in2.Matches(ty3))
	assert.False(t, in2.Matches(arr3.DataType()))
}

func TestOutputType(t *testing.T) {
	ty1 := compute.NewOutputType(arrow.PrimitiveTypes.Int8)
	assert.Equal(t, compute.ResolveFixed, ty1.Kind)
	assert.True(t, arrow.TypeEqual(arrow.PrimitiveTypes.Int8, ty1.Type))

	dummyResolver := func(_ *compute.KernelCtx, args []arrow.DataType) (arrow.DataType, error) {
		return arrow.PrimitiveTypes.Int32, nil
	}

	ty2 := compute.NewComputedOutputType(dummyResolver)
	assert.Equal(t, compute.ResolveComputed, ty2.Kind)

	outType2, err := ty2.Resolve(nil, nil)
	assert.NoError(t, err)
	assert.Same(t, arrow.PrimitiveTypes.Int32, outType2)

	ty3 := ty1
	assert.Equal(t, compute.ResolveFixed, ty3.Kind)
	assert.True(t, arrow.TypeEqual(ty1.Type, ty3.Type))

	ty4 := ty2
	assert.Equal(t, compute.ResolveComputed, ty4.Kind)
	outType4, err := ty4.Resolve(nil, nil)
	assert.NoError(t, err)
	assert.Same(t, arrow.PrimitiveTypes.Int32, outType4)

	assert.Equal(t, "int8", ty3.String())
	assert.Equal(t, "computed", ty4.String())
}

func TestOutputTypeResolve(t *testing.T) {
	ty1 := compute.NewOutputType(arrow.PrimitiveTypes.Int32)

	result, err := ty1.Resolve(nil, nil)
	assert.NoError(t, err)
	assert.Same(t, arrow.PrimitiveTypes.Int32, result)

	result, err = ty1.Resolve(nil, []arrow.DataType{arrow.PrimitiveTypes.Int8})
	assert.NoError(t, err)
	assert.Same(t, arrow.PrimitiveTypes.Int32, result)

	result, err = ty1.Resolve(nil, []arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int8})
	assert.NoError(t, err)
	assert.Same(t, arrow.PrimitiveTypes.Int32, result)

	resolver := func(_ *compute.KernelCtx, args []arrow.DataType) (arrow.DataType, error) {
		return args[0], nil
	}
	ty2 := compute.NewComputedOutputType(resolver)

	result, err = ty2.Resolve(nil, []arrow.DataType{arrow.BinaryTypes.String})
	assert.NoError(t, err)
	assert.Same(t, arrow.BinaryTypes.String, result)

	// type resolver that returns an error
	ty3 := compute.NewComputedOutputType(func(_ *compute.KernelCtx, dt []arrow.DataType) (arrow.DataType, error) {
		// checking the value types versus the function arity should be validated
		// elsewhere. this is just for illustration purposes
		if len(dt) == 0 {
			return nil, fmt.Errorf("%w: need at least one argument", arrow.ErrInvalid)
		}
		return dt[0], nil
	})

	_, err = ty3.Resolve(nil, []arrow.DataType{})
	assert.ErrorIs(t, err, arrow.ErrInvalid)

	// resolver returns a fixed value
	ty4 := compute.NewComputedOutputType(func(*compute.KernelCtx, []arrow.DataType) (arrow.DataType, error) {
		return arrow.PrimitiveTypes.Int32, nil
	})
	result, err = ty4.Resolve(nil, []arrow.DataType{arrow.PrimitiveTypes.Int8})
	assert.NoError(t, err)
	assert.Same(t, arrow.PrimitiveTypes.Int32, result)
	result, err = ty4.Resolve(nil, []arrow.DataType{})
	assert.NoError(t, err)
	assert.Same(t, arrow.PrimitiveTypes.Int32, result)
}

func TestKernelSignatureEquals(t *testing.T) {
	sig1 := compute.KernelSignature{
		InputTypes: []compute.InputType{},
		OutType:    compute.NewOutputType(arrow.BinaryTypes.String)}
	sig1Copy := compute.KernelSignature{
		InputTypes: []compute.InputType{},
		OutType:    compute.NewOutputType(arrow.BinaryTypes.String)}
	sig2 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
		OutType: compute.NewOutputType(arrow.BinaryTypes.String),
	}

	// output type doesn't matter (for now)
	sig3 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
		OutType: compute.NewOutputType(arrow.PrimitiveTypes.Int32),
	}

	sig4 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8),
			compute.NewExactInput(arrow.PrimitiveTypes.Int16),
		},
		OutType: compute.NewOutputType(arrow.BinaryTypes.String),
	}
	sig4Copy := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8),
			compute.NewExactInput(arrow.PrimitiveTypes.Int16),
		},
		OutType: compute.NewOutputType(arrow.BinaryTypes.String),
	}
	sig5 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8),
			compute.NewExactInput(arrow.PrimitiveTypes.Int16),
			compute.NewExactInput(arrow.PrimitiveTypes.Int32),
		},
		OutType: compute.NewOutputType(arrow.BinaryTypes.String),
	}

	assert.True(t, sig1.Equals(&sig1))
	assert.True(t, sig2.Equals(&sig3))
	assert.False(t, sig3.Equals(&sig4))

	// different sig objects but same sig
	assert.True(t, sig1.Equals(&sig1Copy))
	assert.True(t, sig4.Equals(&sig4Copy))

	// match first 2 args, but not third
	assert.False(t, sig4.Equals(&sig5))
}

func TestKernelSignatureVarArgsEqual(t *testing.T) {
	sig1 := compute.KernelSignature{
		InputTypes: []compute.InputType{compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
		OutType:    compute.NewOutputType(arrow.BinaryTypes.String),
		IsVarArgs:  true,
	}
	sig2 := compute.KernelSignature{
		InputTypes: []compute.InputType{compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
		OutType:    compute.NewOutputType(arrow.BinaryTypes.String),
		IsVarArgs:  true,
	}
	sig3 := compute.KernelSignature{
		InputTypes: []compute.InputType{compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
		OutType:    compute.NewOutputType(arrow.BinaryTypes.String),
	}

	assert.True(t, sig1.Equals(&sig2))
	assert.False(t, sig2.Equals(&sig3))
}

func TestKernelSignatureHash(t *testing.T) {
	sig1 := compute.KernelSignature{
		InputTypes: []compute.InputType{},
		OutType:    compute.NewOutputType(arrow.BinaryTypes.String),
	}
	sig2 := compute.KernelSignature{
		InputTypes: []compute.InputType{compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
		OutType:    compute.NewOutputType(arrow.BinaryTypes.String),
	}
	sig3 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8),
			compute.NewExactInput(arrow.PrimitiveTypes.Int32)},
		OutType: compute.NewOutputType(arrow.BinaryTypes.String),
	}

	assert.Equal(t, sig1.Hash(), sig1.Hash())
	assert.Equal(t, sig2.Hash(), sig2.Hash())
	assert.NotEqual(t, sig1.Hash(), sig2.Hash())
	assert.NotEqual(t, sig2.Hash(), sig3.Hash())
}

func TestKernelSignatureMatchesInputs(t *testing.T) {
	// () -> boolean
	sig1 := compute.KernelSignature{
		OutType: compute.NewOutputType(arrow.FixedWidthTypes.Boolean)}

	assert.True(t, sig1.MatchesInputs([]arrow.DataType{}))
	assert.False(t, sig1.MatchesInputs([]arrow.DataType{arrow.PrimitiveTypes.Int8}))

	// (int8, decimal) -> boolean
	sig2 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8),
			compute.NewIDInput(arrow.DECIMAL)},
		OutType: compute.NewOutputType(arrow.FixedWidthTypes.Boolean),
	}
	assert.False(t, sig2.MatchesInputs([]arrow.DataType{}))
	assert.False(t, sig2.MatchesInputs([]arrow.DataType{arrow.PrimitiveTypes.Int8}))
	assert.True(t, sig2.MatchesInputs([]arrow.DataType{
		arrow.PrimitiveTypes.Int8,
		&arrow.Decimal128Type{Precision: 12, Scale: 2}}))

	// (int8, int32) -> boolean
	sig3 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8),
			compute.NewExactInput(arrow.PrimitiveTypes.Int32),
		},
		OutType: compute.NewOutputType(arrow.FixedWidthTypes.Boolean),
	}
	assert.False(t, sig3.MatchesInputs(nil))
	assert.True(t, sig3.MatchesInputs([]arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int32}))
	assert.False(t, sig3.MatchesInputs([]arrow.DataType{arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int16}))
}

func TestKernelSignatureVarArgsMatchesInputs(t *testing.T) {
	{
		sig := compute.KernelSignature{
			InputTypes: []compute.InputType{compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
			OutType:    compute.NewOutputType(arrow.BinaryTypes.String),
			IsVarArgs:  true,
		}

		args := []arrow.DataType{arrow.PrimitiveTypes.Int8}
		assert.True(t, sig.MatchesInputs(args))
		args = append(args, arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int8)
		assert.True(t, sig.MatchesInputs(args))
		args = append(args, arrow.PrimitiveTypes.Int32)
		assert.False(t, sig.MatchesInputs(args))
	}
	{
		sig := compute.KernelSignature{
			InputTypes: []compute.InputType{
				compute.NewExactInput(arrow.PrimitiveTypes.Int8),
				compute.NewExactInput(arrow.BinaryTypes.String),
			},
			OutType:   compute.NewOutputType(arrow.BinaryTypes.String),
			IsVarArgs: true,
		}

		args := []arrow.DataType{arrow.PrimitiveTypes.Int8}
		assert.True(t, sig.MatchesInputs(args))
		args = append(args, arrow.BinaryTypes.String, arrow.BinaryTypes.String)
		assert.True(t, sig.MatchesInputs(args))
		args = append(args, arrow.PrimitiveTypes.Int32)
		assert.False(t, sig.MatchesInputs(args))
	}
}

func TestKernelSignatureToString(t *testing.T) {
	inTypes := []compute.InputType{
		compute.NewExactInput(arrow.PrimitiveTypes.Int8),
		compute.NewIDInput(arrow.DECIMAL),
		compute.NewExactInput(arrow.BinaryTypes.String),
	}

	sig := compute.KernelSignature{
		InputTypes: inTypes, OutType: compute.NewOutputType(arrow.BinaryTypes.String),
	}
	assert.Equal(t, "(int8, Type::DECIMAL128, utf8) -> utf8", sig.String())

	outType := compute.NewComputedOutputType(func(*compute.KernelCtx, []arrow.DataType) (arrow.DataType, error) {
		return nil, arrow.ErrInvalid
	})
	sig2 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8),
			compute.NewIDInput(arrow.DECIMAL)},
		OutType: outType,
	}
	assert.Equal(t, "(int8, Type::DECIMAL128) -> computed", sig2.String())
}

func TestKernelSignatureVarArgsToString(t *testing.T) {
	sig1 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
		OutType:   compute.NewOutputType(arrow.BinaryTypes.String),
		IsVarArgs: true,
	}
	assert.Equal(t, "varargs[int8*] -> utf8", sig1.String())

	sig2 := compute.KernelSignature{
		InputTypes: []compute.InputType{
			compute.NewExactInput(arrow.BinaryTypes.String),
			compute.NewExactInput(arrow.PrimitiveTypes.Int8)},
		OutType:   compute.NewOutputType(arrow.BinaryTypes.String),
		IsVarArgs: true,
	}
	assert.Equal(t, "varargs[utf8, int8*] -> utf8", sig2.String())
}

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
	"math"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/bitutil"
	"github.com/apache/arrow/go/v10/arrow/internal"
	"github.com/apache/arrow/go/v10/arrow/internal/debug"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/arrow/scalar"
)

const DefaultMaxChunkSize = math.MaxInt64

func haveChunkedArray(values []Datum) bool {
	for _, v := range values {
		if v.Kind() == KindChunked {
			return true
		}
	}
	return false
}

type nullGeneralization int8

const (
	nullGenPerhapsNull nullGeneralization = iota
	nullGenAllValid
	nullGenAllNull
)

func getNullGen(val *ExecValue) nullGeneralization {
	dtID := val.Type().ID()
	switch {
	case dtID == arrow.NULL:
		return nullGenAllNull
	case !internal.DefaultHasValidityBitmap(dtID):
		return nullGenAllValid
	case val.IsScalar():
		if val.Scalar.IsValid() {
			return nullGenAllValid
		}
		return nullGenAllNull
	default:
		arr := val.Array
		// do not count if they haven't been counted already
		if arr.Nulls == 0 || arr.Buffers[0].Buf == nil {
			return nullGenAllValid
		}

		if arr.Nulls == arr.Len {
			return nullGenAllNull
		}
	}
	return nullGenPerhapsNull
}

func getNullGenDatum(datum Datum) nullGeneralization {
	var val ExecValue
	switch datum.Kind() {
	case KindArray:
		val.Array.SetMembers(datum.(*ArrayDatum).Value)
	case KindScalar:
		val.Scalar = datum.(*ScalarDatum).Value
	case KindChunked:
		return nullGenPerhapsNull
	default:
		debug.Assert(false, "should be array, scalar, or chunked!")
		return nullGenPerhapsNull
	}
	return getNullGen(&val)
}

func propagateNulls(ctx *KernelCtx, batch *ExecSpan, out *ArraySpan) (err error) {
	if out.Type.ID() == arrow.NULL {
		// null output type is a no-op (rare but it happens)
		return
	}

	// this function is ONLY able to write into output with non-zero offset
	// when the bitmap is preallocated.
	if out.Offset != 0 && out.Buffers[0].Buf == nil {
		return fmt.Errorf("%w: can only propagate nulls into pre-allocated memory when output offset is non-zero", arrow.ErrInvalid)
	}

	var (
		arrsWithNulls = make([]*ArraySpan, 0)
		isAllNull     bool
		prealloc      bool = out.Buffers[0].Buf != nil
	)

	for i := range batch.Values {
		v := &batch.Values[i]
		nullGen := getNullGen(v)
		if nullGen == nullGenAllNull {
			isAllNull = true
		}
		if nullGen != nullGenAllValid && v.IsArray() {
			arrsWithNulls = append(arrsWithNulls, &v.Array)
		}
	}

	outBitmap := out.Buffers[0].Buf
	if isAllNull {
		// an all-null value gives us a short circuit opportunity
		// output should all be null
		out.Nulls = out.Len
		if prealloc {
			bitutil.SetBitsTo(outBitmap, out.Offset, out.Len, false)
			return
		}

		// walk all the values with nulls instead of breaking on the first
		// in case we find a bitmap that can be reused in the non-preallocated case
		for _, arr := range arrsWithNulls {
			if arr.Nulls == arr.Len && arr.Buffers[0].Owner != nil {
				buf := arr.GetBuffer(0)
				buf.Retain()
				out.Buffers[0].Buf = buf.Bytes()
				out.Buffers[0].Owner = buf
				return
			}
		}

		buf := ctx.AllocateBitmap(int64(out.Len))
		out.Buffers[0].Owner = buf
		out.Buffers[0].Buf = buf.Bytes()
		out.Buffers[0].SelfAlloc = true
		bitutil.SetBitsTo(out.Buffers[0].Buf, out.Offset, out.Len, false)
		return
	}

	out.Nulls = array.UnknownNullCount
	switch len(arrsWithNulls) {
	case 0:
		out.Nulls = 0
		if prealloc {
			bitutil.SetBitsTo(outBitmap, out.Offset, out.Len, true)
		}
	case 1:
		arr := arrsWithNulls[0]
		out.Nulls = arr.Nulls
		if prealloc {
			bitutil.CopyBitmap(arr.Buffers[0].Buf, int(arr.Offset), int(arr.Len), outBitmap, int(out.Offset))
			return
		}

		switch {
		case arr.Offset == 0:
			out.Buffers[0] = arr.Buffers[0]
			out.Buffers[0].Owner.Retain()
		case arr.Offset%8 == 0:
			buf := memory.SliceBuffer(arr.GetBuffer(0), int(arr.Offset)/8, int(bitutil.BytesForBits(arr.Len)))
			out.Buffers[0].Buf = buf.Bytes()
			out.Buffers[0].Owner = buf
		default:
			buf := ctx.AllocateBitmap(int64(out.Len))
			out.Buffers[0].Owner = buf
			out.Buffers[0].Buf = buf.Bytes()
			out.Buffers[0].SelfAlloc = true
			bitutil.CopyBitmap(arr.Buffers[0].Buf, int(arr.Offset), int(arr.Len), out.Buffers[0].Buf, 0)
		}
		return

	default:
		if !prealloc {
			buf := ctx.AllocateBitmap(int64(out.Len))
			out.Buffers[0].Owner = buf
			out.Buffers[0].Buf = buf.Bytes()
			out.Buffers[0].SelfAlloc = true
			outBitmap = out.Buffers[0].Buf
		}

		acc := func(left, right *ArraySpan) {
			debug.Assert(left.Buffers[0].Buf != nil, "invalid intersection for null propagation")
			debug.Assert(right.Buffers[0].Buf != nil, "invalid intersection for null propagation")
			bitutil.BitmapAnd(left.Buffers[0].Buf, right.Buffers[0].Buf, left.Offset, right.Offset, outBitmap, out.Offset, out.Len)
		}

		acc(arrsWithNulls[0], arrsWithNulls[1])
		for _, arr := range arrsWithNulls[2:] {
			acc(out, arr)
		}
	}
	return
}

type SelectionVector struct {
}

type ExecBatch struct {
	Values       []Datum
	SelectionVec *SelectionVector
	Guarantee    Expression
	Len          int64
}

func (e *ExecBatch) NumValues() int { return len(e.Values) }

type bufferPrealloc struct {
	bitWidth int
	addLen   int
}

func allocateDataBuffer(ctx *KernelCtx, length, bitWidth int) *memory.Buffer {
	switch bitWidth {
	case 1:
		return ctx.AllocateBitmap(int64(length))
	default:
		bufsiz := int(bitutil.BytesForBits(int64(length * bitWidth)))
		return ctx.Allocate(bufsiz)
	}
}

func computeDataPrealloc(dt arrow.DataType, widths []bufferPrealloc) []bufferPrealloc {
	if typ, ok := dt.(arrow.FixedWidthDataType); ok {
		return append(widths, bufferPrealloc{bitWidth: typ.BitWidth()})
	}

	switch dt.ID() {
	case arrow.BINARY, arrow.STRING, arrow.LIST, arrow.MAP:
		return append(widths, bufferPrealloc{bitWidth: 32, addLen: 1})
	case arrow.LARGE_BINARY, arrow.LARGE_STRING, arrow.LARGE_LIST:
		return append(widths, bufferPrealloc{bitWidth: 64, addLen: 1})
	}
	return widths
}

type kernelExecutor interface {
	Init(*KernelCtx, KernelInitArgs) error
	Execute(context.Context, *ExecBatch, chan<- Datum) error
	WrapResults(ctx context.Context, args []Datum, out <-chan Datum) Datum
	CheckResultType(out Datum, fn string) error
}

type execImpl struct {
	ctx              *KernelCtx
	kernel           KernelNoAgg
	outType          arrow.DataType
	numOutBuf        int
	dataPrealloc     []bufferPrealloc
	preallocValidity bool
}

func (e *execImpl) Init(ctx *KernelCtx, args KernelInitArgs) (err error) {
	e.ctx = ctx
	e.kernel = args.Kernel.(KernelNoAgg)
	e.outType, err = e.kernel.GetSig().OutType.Resolve(ctx, args.Inputs)
	return
}

func (e *execImpl) prepareOutput(length int) *ExecResult {
	var (
		nullCount int = array.UnknownNullCount
	)

	if e.kernel.GetNullHandling() == NullNoOutput {
		nullCount = 0
	}

	output := &ArraySpan{
		Type:  e.outType,
		Len:   int64(length),
		Nulls: int64(nullCount),
	}

	if e.preallocValidity {
		buf := e.ctx.AllocateBitmap(int64(length))
		output.Buffers[0].Owner = buf
		output.Buffers[0].Buf = buf.Bytes()
		output.Buffers[0].SelfAlloc = true
	}

	for i, pre := range e.dataPrealloc {
		if pre.bitWidth >= 0 {
			buf := allocateDataBuffer(e.ctx, length+pre.addLen, pre.bitWidth)
			output.Buffers[i+1].Owner = buf
			output.Buffers[i+1].Buf = buf.Bytes()
			output.Buffers[i+1].SelfAlloc = true
		}
	}

	return output
}

func (e *execImpl) CheckResultType(out Datum, fn string) error {
	typ := out.(ArrayLikeDatum).Type()
	if typ != nil && !arrow.TypeEqual(e.outType, typ) {
		return fmt.Errorf("%w: kernel type result mismatch for function '%s': declared as %s, actual is %s",
			arrow.ErrType, fn, e.outType, typ)
	}
	return nil
}

type spanIterator func() (*ExecSpan, int64, bool)

type scalarExecutor struct {
	execImpl

	elideValidityBitmap bool
	preallocAllBufs     bool
	preallocContiguous  bool
	allScalars          bool
	iter                spanIterator
	iterLen             int64
}

func (s *scalarExecutor) Execute(ctx context.Context, batch *ExecBatch, data chan<- Datum) (err error) {
	s.allScalars, s.iter, err = iterateExecSpans(batch, s.ctx.ExecCtx.ChunkSize, true)
	if err != nil {
		return
	}

	s.iterLen = batch.Len

	if batch.Len == 0 {
		result := array.MakeArrayOfNull(s.ctx.ExecCtx.Alloc, s.outType, 0)
		defer result.Release()
		out := &ArraySpan{}
		out.SetMembers(result.Data())
		return s.emitResult(out, data)
	}

	if err = s.setupPrealloc(batch.Len, batch.Values); err != nil {
		return
	}

	// accommodate distinct output cases:
	//
	// * Fully-preallocated contiguous output
	// * Fully-preallocated, non-contiguous kernel output
	// * not fully-preallocated kernel output: we pass an empty or partially
	//   filled ArrayData to the kernel
	// if s.preallocAllBufs {
	return s.executeSpans(data)
	// }
	// return s.executeNonSpans(data)
}

func (s *scalarExecutor) WrapResults(ctx context.Context, args []Datum, out <-chan Datum) Datum {
	var (
		output Datum
		acc    []arrow.Array
	)

	select {
	case <-ctx.Done():
		return nil
	case output = <-out:
		if haveChunkedArray(args) {
			acc = output.(ArrayLikeDatum).Chunks()
			output.Release()
			for _, c := range acc {
				defer c.Release()
			}
			output = nil
		}
	}

	for {
		select {
		case <-ctx.Done():
			return output
		case o, ok := <-out:
			if !ok {
				if output != nil {
					return output
				}
				chkd := arrow.NewChunked(s.outType, acc)
				defer chkd.Release()
				return NewDatum(chkd)
			}

			if acc == nil {
				acc = output.(ArrayLikeDatum).Chunks()
				output.Release()
				for _, c := range acc {
					defer c.Release()
				}
				output = nil
			}

			defer o.Release()
			if o.Len() == 0 {
				continue
			}

			arr := o.(*ArrayDatum).MakeArray()
			defer arr.Release()
			acc = append(acc, arr)
		}
	}
}

func (s *scalarExecutor) emitResult(resultData *ArraySpan, data chan<- Datum) error {
	var output Datum
	if s.allScalars {
		// we boxed scalar inputs as ArraySpan so now we have to unbox the output
		arr := resultData.MakeArray()
		defer arr.Release()
		sc, err := scalar.GetScalar(arr, 0)
		if err != nil {
			return err
		}
		output = NewDatum(sc)
	} else {
		d := resultData.MakeData()
		defer d.Release()
		output = NewDatum(d)
	}
	data <- output
	return nil
}

func (s *scalarExecutor) setupPrealloc(totalLen int64, args []Datum) error {
	s.numOutBuf = len(s.outType.Layout().Buffers)
	outTypeID := s.outType.ID()
	// default to no validity pre-allocation for the following cases:
	// - Output Array is NullArray
	// - kernel.NullHandling is ComputeNoPrealloc or OutputNotNull
	s.preallocValidity = false

	if outTypeID != arrow.NULL {
		switch s.kernel.GetNullHandling() {
		case NullComputedPrealloc:
			s.preallocValidity = true
		case NullIntersection:
			s.elideValidityBitmap = true
			for _, a := range args {
				nullGen := getNullGenDatum(a) == nullGenAllValid
				s.elideValidityBitmap = s.elideValidityBitmap && nullGen
			}
			s.preallocValidity = !s.elideValidityBitmap
		case NullNoOutput:
			s.elideValidityBitmap = true
		}
	}

	if s.kernel.GetMemAlloc() == MemPrealloc {
		s.dataPrealloc = computeDataPrealloc(s.outType, s.dataPrealloc)
	}

	// validity bitmap either preallocated or elided, and all data buffers allocated
	// this is basically only true for primitive types that are not dict-encoded
	s.preallocAllBufs =
		((s.preallocValidity || s.elideValidityBitmap) && len(s.dataPrealloc) == (s.numOutBuf-1) &&
			!arrow.IsNested(outTypeID) && outTypeID != arrow.DICTIONARY)

	// contiguous prealloc only possible on non-nested types if all
	// buffers are preallocated. otherwise we have to go chunk by chunk
	//
	// some kernels are also unable to write into sliced outputs, so
	// we respect the kernel's attributes
	s.preallocContiguous =
		(s.ctx.ExecCtx.PreallocContiguous && s.kernel.CanWriteIntoSlices() &&
			s.preallocAllBufs)

	return nil
}

func (s *scalarExecutor) executeSingleSpan(input *ExecSpan, out *ExecResult) error {
	switch {
	case out.Type.ID() == arrow.NULL:
		out.Nulls = out.Len
	case s.kernel.GetNullHandling() == NullIntersection:
		if !s.elideValidityBitmap {
			propagateNulls(s.ctx, input, out)
		}
	case s.kernel.GetNullHandling() == NullNoOutput:
		out.Nulls = 0
	}
	return s.kernel.Exec(s.ctx, input, out)
}

func (s *scalarExecutor) executeSpans(data chan<- Datum) (err error) {
	var (
		input  *ExecSpan
		output ExecResult
		next   bool
	)

	if s.preallocContiguous {
		// make one big output alloc
		prealloc := s.prepareOutput(int(s.iterLen))
		output = *prealloc

		output.Offset = 0
		var resultOffset int64
		var nextOffset int64
		for err == nil {
			if input, nextOffset, next = s.iter(); !next {
				break
			}
			output.SetSlice(resultOffset, input.Len)
			err = s.executeSingleSpan(input, &output)
			resultOffset = nextOffset
		}
		if err != nil {
			return
		}

		return s.emitResult(prealloc, data)
	}

	// fully preallocating, but not contiguously
	// we (maybe) preallocate only for the output of processing
	// the current chunk
	for err == nil {
		if input, _, next = s.iter(); !next {
			break
		}

		output = *s.prepareOutput(int(input.Len))
		if err = s.executeSingleSpan(input, &output); err != nil {
			return
		}
		err = s.emitResult(&output, data)
	}

	return
}

func checkAllIsValue(vals []Datum) error {
	for _, v := range vals {
		if !DatumIsValue(v) {
			return fmt.Errorf("%w: tried executing function with non-value type: %s",
				arrow.ErrInvalid, v)
		}
	}
	return nil
}

func checkIfAllScalar(batch *ExecBatch) bool {
	for _, v := range batch.Values {
		if v.Kind() != KindScalar {
			return false
		}
	}
	return batch.NumValues() > 0
}

func execInternal(execCtx *ExecCtx, fn Function, opts FunctionOptions, passedLen int64, args ...Datum) (result Datum, err error) {
	if opts == nil {
		if err = fn.checkOptions(opts); err != nil {
			return
		}
		opts = fn.DefaultOptions()
	}

	// TODO: add opentracing
	if err = checkAllIsValue(args); err != nil {
		return
	}

	inTypes := make([]arrow.DataType, len(args))
	for i, a := range args {
		inTypes[i] = a.(ArrayLikeDatum).Type()
	}

	var (
		k        Kernel
		executor kernelExecutor
	)

	switch fn.Kind() {
	case FuncScalar:
		executor = newScalarExecutor()
	default:
		return nil, fmt.Errorf("%w: direct execution of %s", arrow.ErrNotImplemented, fn.Kind())
	}

	if k, err = fn.DispatchBest(inTypes...); err != nil {
		return
	}

	// Cast Arguments if necessary

	kctx := KernelCtx{ExecCtx: execCtx, Kernel: k}
	init := k.GetInit()
	kinitArgs := KernelInitArgs{k, inTypes, opts}
	if init != nil {
		kctx.State, err = init(&kctx, kinitArgs)
		if err != nil {
			return
		}
	}

	if err = executor.Init(&kctx, kinitArgs); err != nil {
		return
	}

	input := ExecBatch{Values: args, Len: 0}
	if input.NumValues() == 0 {
		if passedLen != -1 {
			input.Len = passedLen
		}
	} else {
		inferred, allSame := inferBatchLength(input.Values)
		input.Len = inferred
		switch fn.Kind() {
		case FuncScalar:
			if passedLen != -1 && passedLen != inferred {
				return nil, fmt.Errorf("%w: passed batch length for execution did not match actual length for scalar fn execution",
					arrow.ErrInvalid)
			}
		case FuncVector:
			vkernel := k.(*VectorKernel)
			if !(allSame || !vkernel.canExecuteChunkwise) {
				return nil, fmt.Errorf("%w: vector kernel arguments must all be the same length", arrow.ErrInvalid)
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan Datum, 10)
	go func() {
		defer close(ch)
		if err = executor.Execute(ctx, &input, ch); err != nil {
			cancel()
		}
	}()

	result = executor.WrapResults(ctx, input.Values, ch)
	debug.Assert(executor.CheckResultType(result, fn.Name()) == nil, "invalid result type")

	if ctx.Err() == context.Canceled {
		result.Release()
	}

	return
}

func newScalarExecutor() *scalarExecutor {
	return &scalarExecutor{}
}

func CallFunction(ctx *ExecCtx, funcName string, opts FunctionOptions, args ...Datum) (Datum, error) {
	if ctx == nil {
		ctx = &defaultCtx
	}

	fn, ok := ctx.Registry.GetFunction(funcName)
	if !ok {
		return nil, fmt.Errorf("%w: function '%s' not found", arrow.ErrKey, funcName)
	}

	return fn.Execute(ctx, opts, args...)
}

package kernels

import (
	"bytes"
	"sort"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/compute/internal/exec"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type SortIndicesOptions struct {
	NullPlacement NullPlacement `compute:"null_placement"`
}

func (s *SortIndicesOptions) TypeName() string {
	return "SortIndicesOptions"
}

type Order int

const (
	Ascending Order = iota
	Descending
)

type NullPlacement int

const (
	AtStart NullPlacement = iota
	AtEnd
)

func GetVectorSortingKernels() []exec.VectorKernel {
	var base exec.VectorKernel
	base.CanExecuteChunkWise = true
	base.OutputChunked = false
	outType := exec.NewOutputType(arrow.ListOf(arrow.PrimitiveTypes.Int64))
	kernels := make([]exec.VectorKernel, 0)
	for _, ty := range primitiveTypes {
		base.Signature = &exec.KernelSignature{
			InputTypes: []exec.InputType{exec.NewExactInput(ty)},
			OutType:    outType,
		}
		base.ExecFn = sortExec
		kernels = append(kernels, base)
	}
	return kernels
}

func sortExec(ctx *exec.KernelCtx, batch *exec.ExecSpan, out *exec.ExecResult) error {
	// Get the input array from the batch
	inExecVal := batch.Values[0]
	inArr := inExecVal.Array

	// Create a slice of indices, initialized to [0, 1, 2, ..., n-1]
	indices := make([]int, inArr.Len)
	for i := range indices {
		indices[i] = i
	}

	sz := inArr.Type.(arrow.FixedWidthDataType).Bytes()

	// Sort the indices slice based on the values in the input array
	sort.Slice(indices, func(i, j int) bool {
		// TODO: not sure what to do here?
		// compare using scalar comparison?
		a := inArr.Buffers[1].Buf[indices[i]*sz : (indices[i]+1)*sz]
		b := inArr.Buffers[1].Buf[indices[j]*sz : (indices[j]+1)*sz]
		return bytes.Compare(a, b) < 0
	})

	// Create a new array builder to build the output array
	builder := array.NewInt64Builder(memory.DefaultAllocator)

	// Add the sorted indices to the output array builder
	for _, index := range indices {
		builder.Append(int64(index))
	}

	// Build the output array and set it in the output ExecResult
	outArr := builder.NewArray()
	out.SetMembers(outArr.Data())

	return nil
}

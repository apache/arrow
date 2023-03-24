package kernels

import (
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/compute/internal/exec"
)

type SortIndicesOptions struct {
	SortKeys      []SortKey     `compute:"sort_keys"`
	NullPlacement NullPlacement `compute:"null_placement"`
}

func (s *SortIndicesOptions) TypeName() string {
	return "SortIndicesOptions"
}

type SortKey struct {
	Name  string `compute:"name"`
	Order Order  `compute:"order"`
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
	if !batch.Values[0].IsArray() {
		return fmt.Errorf("input to sortExec must be an array")
	}
	ar := batch.Values[0].Array
	// sz := int64(ar.Type.(arrow.FixedWidthDataType).Bytes())

	out.SetMembers(ar.MakeData())
	return nil
	//impl, ok := ctx.State.(HashState)
	//if !ok {
	//	return fmt.Errorf("%w: bad initialization of hash state", arrow.ErrInvalid)
	//}
	//
	//if err := impl.Append(ctx, &batch.Values[0].Array); err != nil {
	//	return err
	//}
	//
	//return impl.Flush(out)
}

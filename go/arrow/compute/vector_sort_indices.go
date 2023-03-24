package compute

import (
	"context"

	"github.com/apache/arrow/go/v12/arrow/compute/internal/kernels"
)

var (
	sortIndicesDoc = FunctionDoc{
		Summary:     "Return the indices that would sort an array",
		Description: "This function computes an array of indices that define a stable sort of the input array, record batch or table. By default, nNull values are considered greater than any other value and are therefore sorted at the end of the input. For floating-point types, NaNs are considered greater than any other non-null value, but smaller than null values.",
		ArgNames:    []string{"array"},
	}
)

type SortIndicesOptions = kernels.SortIndicesOptions

// RegisterVectorSorting registers functions related to vector sorting, such as sort_indices.
func RegisterVectorSorting(registry FunctionRegistry) {
	vf := NewVectorFunction("sort_indices", Unary(), sortIndicesDoc)
	vf.defaultOpts = &kernels.SortIndicesOptions{}
	ks := kernels.GetVectorSortingKernels()
	for i := range ks {
		if err := vf.AddKernel(ks[i]); err != nil {
			panic(err)
		}
	}
	registry.AddFunction(vf, false)
}

func SortIndices(ctx context.Context, opts kernels.SortIndicesOptions, input Datum) (Datum, error) {
	return CallFunction(ctx, "sort_indices", &opts, input)
}

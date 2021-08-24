package dataset

// #cgo pkg-config: arrow-dataset
// #include <stdlib.h>
// #include "arrow/dataset/c/api.h"
// #include "arrow/c/helpers.h"
// typedef struct ArrowSchema ArrowSchema;
import "C"

import (
	"runtime"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
)

var (
	InspectAllFragments     = C.kInspectAllFragments
	DisableInspectFragments = C.DISABLE_INSPECT_FRAGMENTS
	DefaultInspectFragments = C.DEFAULT_NUM_FRAGMENTS
)

func arrowSchemaToSchema(out *C.ArrowSchema) (*arrow.Schema, error) {
	ret, err := importSchema(out)
	if err != nil {
		return nil, err
	}

	return arrow.NewSchema(ret.Type.(*arrow.StructType).Fields(), &ret.Metadata), nil
}

type FileFormat int

const (
	PARQUET FileFormat = C.DS_PARQUET_FORMAT
	CSV     FileFormat = C.DS_CSV_FORMAT
	IPC     FileFormat = C.DS_IPC_FORMAT
)

type DatasetFactory uintptr

func (d DatasetFactory) Close() {
	C.release_dataset_factory(C.uintptr_t(d))
}

func (d DatasetFactory) Inspect(numFragments int) (*arrow.Schema, error) {
	sc, err := C.inspect_schema(C.uintptr_t(d), C.int(numFragments))
	if sc.release == nil {
		return nil, err
	}
	defer C.ArrowSchemaRelease(&sc)

	return arrowSchemaToSchema(&sc)
}

func (d DatasetFactory) CreateDataset() (*Dataset, error) {
	ds, err := C.create_dataset(C.uintptr_t(d))
	if ds == 0 {
		return nil, err
	}

	ret := &Dataset{addr: ds}
	runtime.SetFinalizer(ret, (*Dataset).Close)
	return ret, err
}

func CreateDatasetFactory(uri string) (DatasetFactory, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	ds, err := C.factory_from_path(curi, C.DS_PARQUET_FORMAT)
	if ds == 0 {
		return 0, err
	}
	return DatasetFactory(ds), nil
}

type Dataset struct {
	addr   C.uintptr_t
	schema *arrow.Schema

	dstype string
}

func (d *Dataset) Close() {
	if d.addr != 0 {
		C.close_dataset(d.addr)
		d.addr = 0
	}
}

func (d *Dataset) Schema() (*arrow.Schema, error) {
	if d.schema != nil {
		return d.schema, nil
	}

	sc, err := C.get_dataset_schema(d.addr)
	defer C.ArrowSchemaRelease(&sc)
	if err != nil {
		return nil, err
	}

	s, err := arrowSchemaToSchema(&sc)
	if err != nil {
		return nil, err
	}

	d.schema = s
	return d.schema, nil
}

func (d *Dataset) Type() string {
	if d.dstype == "" {
		d.dstype = C.GoString(C.dataset_type_name(d.addr))
	}
	return d.dstype
}

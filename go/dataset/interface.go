package dataset

// #include "arrow/c/abi.h"
// typedef struct ArrowSchema ArrowSchema;
import "C"

import (
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	"golang.org/x/xerrors"
)

var formatToSimpleType = map[string]arrow.DataType{
	"n":   arrow.Null,
	"b":   arrow.FixedWidthTypes.Boolean,
	"c":   arrow.PrimitiveTypes.Int8,
	"C":   arrow.PrimitiveTypes.Uint8,
	"s":   arrow.PrimitiveTypes.Int16,
	"S":   arrow.PrimitiveTypes.Uint16,
	"i":   arrow.PrimitiveTypes.Int32,
	"I":   arrow.PrimitiveTypes.Uint32,
	"l":   arrow.PrimitiveTypes.Int64,
	"L":   arrow.PrimitiveTypes.Uint64,
	"e":   arrow.FixedWidthTypes.Float16,
	"f":   arrow.PrimitiveTypes.Float32,
	"g":   arrow.PrimitiveTypes.Float64,
	"z":   arrow.BinaryTypes.Binary,
	"u":   arrow.BinaryTypes.String,
	"tdD": arrow.FixedWidthTypes.Date32,
	"tdm": arrow.FixedWidthTypes.Date64,
	"tts": arrow.FixedWidthTypes.Time32s,
	"ttm": arrow.FixedWidthTypes.Time32ms,
	"ttu": arrow.FixedWidthTypes.Time64us,
	"ttn": arrow.FixedWidthTypes.Time64ns,
	"tDs": arrow.FixedWidthTypes.Duration_s,
	"tDm": arrow.FixedWidthTypes.Duration_ms,
	"tDu": arrow.FixedWidthTypes.Duration_us,
	"tDn": arrow.FixedWidthTypes.Duration_ns,
	"tiM": arrow.FixedWidthTypes.MonthInterval,
	"tiD": arrow.FixedWidthTypes.DayTimeInterval,
}

func decodeCMetadata(md *C.char) arrow.Metadata {
	if md == nil {
		return arrow.Metadata{}
	}

	const maxlen = 0x7fffffff
	data := (*[maxlen]byte)(unsafe.Pointer(md))[:]

	readint32 := func() int32 {
		v := *(*int32)(unsafe.Pointer(&data[0]))
		data = data[arrow.Int32SizeBytes:]
		return v
	}

	readstr := func() string {
		l := readint32()
		s := string(data[:l])
		data = data[l:]
		return s
	}

	npairs := readint32()
	if npairs == 0 {
		return arrow.Metadata{}
	}

	keys := make([]string, npairs)
	vals := make([]string, npairs)

	for i := int32(0); i < npairs; i++ {
		keys[i] = readstr()
		vals[i] = readstr()
	}

	return arrow.NewMetadata(keys, vals)
}

func importSchema(schema *C.ArrowSchema) (ret arrow.Field, err error) {
	var childFields []arrow.Field
	if schema.n_children > 0 {
		var schemaChildren []*C.ArrowSchema
		s := (*reflect.SliceHeader)(unsafe.Pointer(&schemaChildren))
		s.Data = uintptr(unsafe.Pointer(schema.children))
		s.Len = int(schema.n_children)
		s.Cap = int(schema.n_children)

		childFields = make([]arrow.Field, schema.n_children)
		for i, c := range schemaChildren {
			childFields[i], err = importSchema(c)
			if err != nil {
				return
			}
		}
	}

	ret.Name = C.GoString(schema.name)
	ret.Nullable = (schema.flags & C.ARROW_FLAG_NULLABLE) != 0
	ret.Metadata = decodeCMetadata(schema.metadata)

	f := C.GoString(schema.format)
	dt, ok := formatToSimpleType[f]
	if ok {
		ret.Type = dt
		return
	}

	switch f[0] {
	case 'w':
		byteWidth, err := strconv.Atoi(strings.Split(f, ":")[1])
		if err != nil {
			return ret, err
		}
		dt = &arrow.FixedSizeBinaryType{ByteWidth: byteWidth}
	case 'd':
		props := strings.Split(f, ":")[1]
		propList := strings.Split(props, ",")
		if len(propList) == 3 {
			err = xerrors.New("only decimal128 is supported")
			return
		}

		precision, _ := strconv.Atoi(propList[0])
		scale, _ := strconv.Atoi(propList[1])
		dt = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
	case '+':
		switch f[1] {
		case 'l':
			dt = arrow.ListOf(childFields[0].Type)
		case 'w':
			listSize, err := strconv.Atoi(strings.Split(f, ":")[1])
			if err != nil {
				return ret, err
			}

			dt = arrow.FixedSizeListOf(int32(listSize), childFields[0].Type)
		case 's':
			dt = arrow.StructOf(childFields...)
		case 'm':
			st := childFields[0].Type.(*arrow.StructType)
			dt = arrow.MapOf(st.Field(0).Type, st.Field(1).Type)
			dt.(*arrow.MapType).KeysSorted = (schema.flags & C.ARROW_FLAG_MAP_KEYS_SORTED) != 0
		}
	}

	if dt == nil {
		err = xerrors.New("unimplemented type")
	} else {
		ret.Type = dt
	}
	return
}

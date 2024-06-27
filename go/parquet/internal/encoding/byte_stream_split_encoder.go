package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/apache/arrow/go/v17/internal/bitutils"
	"github.com/apache/arrow/go/v17/internal/utils"
	"github.com/apache/arrow/go/v17/parquet"
	"golang.org/x/xerrors"
)

type NumericByteStreamSplitType interface {
	float32 | float64 | int32 | int64
}

type ByteStreamSplitType interface {
	NumericByteStreamSplitType | parquet.FixedLenByteArray
}

// putByteStreamSplit is a generic implementation of the BYTE_STREAM_SPLIT encoding, operating on the byte-representation of values.
func putByteStreamSplit[T ~[]byte](in []T, sink *PooledBufferWriter, typeLen int) {
	for _, val := range in {
		if len(val) != typeLen {
			panic(fmt.Sprintf("invalid element of size %d, expected all elements to be of size %d", len(val), typeLen))
		}
	}

	bytesNeeded := len(in) * typeLen
	sink.Reserve(bytesNeeded)
	for offset := 0; offset < typeLen; offset++ {
		for _, val := range in {
			sink.UnsafeWrite([]byte{byte(val[offset])}) // Single-element slice
			// sink.UnsafeWrite(val[offset : offset+1]) // Single-element slice
		}
	}
}

// putByteStreamSplitNumeric is a helper that handles conversion of fixed-width numeric types to their byte-representations
func putByteStreamSplitNumeric[T NumericByteStreamSplitType](in []T, enc TypedEncoder, sink *PooledBufferWriter) {
	data := castToBytes(in)
	putByteStreamSplit(data, sink, enc.Type().ByteSize())
}

// putByteStreamSplitSpaced encodes data that has space for nulls using the BYTE_STREAM_SPLIT encoding, calling to the provided putFn to encode runs of non-null values.
func putByteStreamSplitSpaced[T ByteStreamSplitType](in []T, validBits []byte, validBitsOffset int64, bitSetReader bitutils.SetBitRunReader, putFn func([]T)) {
	if validBits != nil {
		if bitSetReader == nil {
			bitSetReader = bitutils.NewSetBitRunReader(validBits, validBitsOffset, int64(len(in)))
		} else {
			bitSetReader.Reset(validBits, validBitsOffset, int64(len(in)))
		}

		for {
			run := bitSetReader.NextRun()
			if run.Length == 0 {
				break
			}
			putFn(in[int(run.Pos):int(run.Pos+run.Length)])
		}
	} else {
		putFn(in)
	}
}

// decodeByteStreamSplit is a generic implementation of the BYTE_STREAM_SPLIT decoder, operating on the byte-representation of values.
func decodeByteStreamSplit[T ~[]byte](out []T, dec TypedDecoder, data []byte, typeLen int) (int, error) {
	max := utils.Min(len(out), dec.ValuesLeft())
	numBytesNeeded := max * typeLen
	if numBytesNeeded > len(data) || numBytesNeeded > math.MaxInt32 {
		return 0, xerrors.New("parquet: eof exception")
	}

	for idx := range out[:max] {
		if len(out[idx]) != typeLen {
			out[idx] = make([]byte, typeLen)
		}
	}

	for offset := 0; offset < typeLen; offset++ {
		for idx := range out[:max] {
			out[idx][offset] = data[0]
			data = data[1:]
		}
	}

	dec.SetData(dec.ValuesLeft()-max, data)

	return max, nil
}

// decodeByteStreamSplitNumeric is a helper that handles conversion of bytes to the correct fixed-width numeric type.
func decodeByteStreamSplitNumeric[T NumericByteStreamSplitType](out []T, dec TypedDecoder, data []byte) (int, error) {
	res := make([][]byte, len(out))
	n, err := decodeByteStreamSplit(res, dec, data, dec.Type().ByteSize())
	if err != nil {
		return n, err
	}

	castFromBytes(res, out)

	return n, nil
}

// decodeByteStreamSplitSpaced decodes BYTE_STREAM_SPLIT-encoded data with space for nulls, calling to the provided decodeFn to decode runs of non-null values.
func decodeByteStreamSplitSpaced[T ByteStreamSplitType](out []T, nullCount int, validBits []byte, validBitsOffset int64, decodeFn func([]T) (int, error)) (int, error) {
	toRead := len(out) - nullCount
	valuesRead, err := decodeFn(out[:toRead])
	if err != nil {
		return valuesRead, err
	}
	if valuesRead != toRead {
		return valuesRead, xerrors.New("parquet: number of values / definitions levels read did not match")
	}

	return spacedExpand(out, nullCount, validBits, validBitsOffset), nil
}

// ByteStreamSplitFloat32Encoder writes the underlying bytes of the Float32
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitFloat32Encoder struct {
	encoder
	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *ByteStreamSplitFloat32Encoder) Put(in []float32) {
	putByteStreamSplitNumeric(in, enc, enc.sink)
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *ByteStreamSplitFloat32Encoder) PutSpaced(in []float32, validBits []byte, validBitsOffset int64) {
	putByteStreamSplitSpaced(in, validBits, validBitsOffset, enc.bitSetReader, enc.Put)
}

// Type returns the underlying physical type this encoder works with, Float32.
func (enc ByteStreamSplitFloat32Encoder) Type() parquet.Type {
	return parquet.Types.Float
}

// ByteStreamSplitFloat64Encoder writes the underlying bytes of the Float64
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitFloat64Encoder struct {
	encoder
	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *ByteStreamSplitFloat64Encoder) Put(in []float64) {
	putByteStreamSplitNumeric(in, enc, enc.sink)
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *ByteStreamSplitFloat64Encoder) PutSpaced(in []float64, validBits []byte, validBitsOffset int64) {
	putByteStreamSplitSpaced(in, validBits, validBitsOffset, enc.bitSetReader, enc.Put)
}

// Type returns the underlying physical type this encoder works with, Float64.
func (enc ByteStreamSplitFloat64Encoder) Type() parquet.Type {
	return parquet.Types.Double
}

// ByteStreamSplitInt32Encoder writes the underlying bytes of the Int32
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitInt32Encoder struct {
	encoder
	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *ByteStreamSplitInt32Encoder) Put(in []int32) {
	putByteStreamSplitNumeric(in, enc, enc.sink)
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *ByteStreamSplitInt32Encoder) PutSpaced(in []int32, validBits []byte, validBitsOffset int64) {
	putByteStreamSplitSpaced(in, validBits, validBitsOffset, enc.bitSetReader, enc.Put)
}

// Type returns the underlying physical type this encoder works with, Int32.
func (enc ByteStreamSplitInt32Encoder) Type() parquet.Type {
	return parquet.Types.Int32
}

// ByteStreamSplitInt64Encoder writes the underlying bytes of the Int64
// into interlaced streams as defined by the BYTE_STREAM_SPLIT encoding
type ByteStreamSplitInt64Encoder struct {
	encoder
	bitSetReader bitutils.SetBitRunReader
}

// Put writes the provided values to the encoder
func (enc *ByteStreamSplitInt64Encoder) Put(in []int64) {
	putByteStreamSplitNumeric(in, enc, enc.sink)
}

// PutSpaced is like Put but works with data that is spaced out according to the passed in bitmap
func (enc *ByteStreamSplitInt64Encoder) PutSpaced(in []int64, validBits []byte, validBitsOffset int64) {
	putByteStreamSplitSpaced(in, validBits, validBitsOffset, enc.bitSetReader, enc.Put)
}

// Type returns the underlying physical type this encoder works with, Int64.
func (enc ByteStreamSplitInt64Encoder) Type() parquet.Type {
	return parquet.Types.Int64
}

// ByteStreamSplitFloat32Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Float32 values
type ByteStreamSplitFloat32Decoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, Float32
func (ByteStreamSplitFloat32Decoder) Type() parquet.Type {
	return parquet.Types.Float
}

// Decode populates out with float32 values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (dec *ByteStreamSplitFloat32Decoder) Decode(out []float32) (int, error) {
	return decodeByteStreamSplitNumeric(out, dec, dec.data)
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (dec *ByteStreamSplitFloat32Decoder) DecodeSpaced(out []float32, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	return decodeByteStreamSplitSpaced(out, nullCount, validBits, validBitsOffset, dec.Decode)
}

// ByteStreamSplitFloat64Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Float64 values
type ByteStreamSplitFloat64Decoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, Float64
func (ByteStreamSplitFloat64Decoder) Type() parquet.Type {
	return parquet.Types.Double
}

// Decode populates out with float64 values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (dec *ByteStreamSplitFloat64Decoder) Decode(out []float64) (int, error) {
	return decodeByteStreamSplitNumeric(out, dec, dec.data)
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (dec *ByteStreamSplitFloat64Decoder) DecodeSpaced(out []float64, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	return decodeByteStreamSplitSpaced(out, nullCount, validBits, validBitsOffset, dec.Decode)
}

// ByteStreamSplitInt32Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Int32 values
type ByteStreamSplitInt32Decoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, Int32
func (ByteStreamSplitInt32Decoder) Type() parquet.Type {
	return parquet.Types.Int32
}

// Decode populates out with int32 values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (dec *ByteStreamSplitInt32Decoder) Decode(out []int32) (int, error) {
	return decodeByteStreamSplitNumeric(out, dec, dec.data)
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (dec *ByteStreamSplitInt32Decoder) DecodeSpaced(out []int32, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	return decodeByteStreamSplitSpaced(out, nullCount, validBits, validBitsOffset, dec.Decode)
}

// ByteStreamSplitInt64Decoder is a decoder for BYTE_STREAM_SPLIT-encoded
// bytes representing Int64 values
type ByteStreamSplitInt64Decoder struct {
	decoder
}

// Type returns the physical type this decoder operates on, Int64
func (ByteStreamSplitInt64Decoder) Type() parquet.Type {
	return parquet.Types.Int64
}

// Decode populates out with int64 values until either there are no more
// values to decode or the length of out has been filled. Then returns the total number of values
// that were decoded.
func (dec *ByteStreamSplitInt64Decoder) Decode(out []int64) (int, error) {
	return decodeByteStreamSplitNumeric(out, dec, dec.data)
}

// DecodeSpaced does the same as Decode but spaces out the resulting slice according to the bitmap leaving space for null values
func (dec *ByteStreamSplitInt64Decoder) DecodeSpaced(out []int64, nullCount int, validBits []byte, validBitsOffset int64) (int, error) {
	return decodeByteStreamSplitSpaced(out, nullCount, validBits, validBitsOffset, dec.Decode)
}

// castToBytes copies a slice of fixed-width numeric values to a slice of byte-slices with their binary encodings
func castToBytes[T NumericByteStreamSplitType](s []T) [][]byte {
	wr := newTempWriter(len(s))
	for i := range s {
		if err := binary.Write(wr, binary.LittleEndian, s[i]); err != nil {
			panic(err)
		}
	}
	return wr.data
}

// castFromBytes copies a slice of byte-slices to the provided slice in its target fixed-width numeric encoding
func castFromBytes[T NumericByteStreamSplitType](s [][]byte, out []T) {
	rdr := newTempReader(s)
	for idx := range out {
		if err := binary.Read(rdr, binary.LittleEndian, &out[idx]); err != nil {
			panic(err)
		}
	}
}

type tempWriter struct {
	data [][]byte
	idx  int
}

func newTempWriter(size int) *tempWriter {
	return &tempWriter{data: make([][]byte, size)}
}

// Write implements io.Writer.
func (t *tempWriter) Write(p []byte) (int, error) {
	if t.idx >= len(t.data) {
		return 0, fmt.Errorf("writer full")
	}

	t.data[t.idx] = p
	t.idx++

	return len(p), nil
}

func newTempReader(data [][]byte) *tempReader {
	return &tempReader{data: data}
}

type tempReader struct {
	data [][]byte
	idx  int
}

// Read implements io.Reader.
func (t *tempReader) Read(p []byte) (int, error) {
	if t.idx >= len(t.data) {
		return 0, io.EOF
	}

	n := copy(p, t.data[t.idx])
	t.idx++
	return n, nil
}

var _ io.Writer = (*tempWriter)(nil)
var _ io.Reader = (*tempReader)(nil)

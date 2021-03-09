package compress

import (
	"compress/flate"
	"io"
	"io/ioutil"

	"github.com/apache/arrow/go/parquet/internal/gen-go/parquet"
)

// Compression is an alias to the thrift compression codec enum type for easy use
type Compression parquet.CompressionCodec

func (c Compression) String() string {
	return parquet.CompressionCodec(c).String()
}

// DefaultCompressionLevel will use flate.DefaultCompression since many of the compression libraries
// use that to denote "use the default".
const DefaultCompressionLevel = flate.DefaultCompression

// Codecs is a useful struct to provide namespaced enum values to use for specifying the compression type to use
// which make for easy internal swapping between them and the thrift enum since they are initialized to the same
// constant values.
var Codecs = struct {
	Uncompressed Compression
	Snappy       Compression
	Gzip         Compression
	Lzo          Compression
	Brotli       Compression
	Lz4          Compression
	Zstd         Compression
}{
	Uncompressed: Compression(parquet.CompressionCodec_UNCOMPRESSED),
	Snappy:       Compression(parquet.CompressionCodec_SNAPPY),
	Gzip:         Compression(parquet.CompressionCodec_GZIP),
	Lzo:          Compression(parquet.CompressionCodec_LZO),
	Brotli:       Compression(parquet.CompressionCodec_BROTLI),
	Lz4:          Compression(parquet.CompressionCodec_LZ4),
	Zstd:         Compression(parquet.CompressionCodec_ZSTD),
}

// Codec is an interface which is implemented for each compression type in order to make the interactions easy to
// implement. Most consumers won't be calling GetCodec directly.
type Codec interface {
	// NewReader provides a reader that wraps a stream with compressed data to stream the uncompressed data
	NewReader(io.Reader) io.ReadCloser
	// NewWriter provides a wrapper around a write stream to compress data before writing it.
	NewWriter(io.Writer) io.WriteCloser
	// NewWriterLevel is like NewWrapper but allows specifying the compression level
	NewWriterLevel(io.Writer, int) (io.WriteCloser, error)
	// Encode encodes a block of data given by src and returns the compressed block. dst needs to be either nil
	// or sized large enough to fit the compressed block (use CompressBound to allocate). dst and src should not
	// overlap since some of the compression types don't allow it.
	//
	// The returned slice *might* be a slice of dst if it was able to fit the whole compressed data in it.
	Encode(dst, src []byte) []byte
	// EncodeLevel is like Encode, but specifies a particular encoding level instead of the default.
	EncodeLevel(dst, src []byte, level int) []byte
	// CompressBound returns the boundary of maximum size of compressed data under the chosen codec.
	CompressBound(int64) int64
	// Decode is for decoding a single block rather than a stream, like with Encode, dst must be either nil or
	// sized large enough to accommodate the uncompressed data and should not overlap with src.
	//
	// the returned slice *might* be a slice of dst.
	Decode(dst, src []byte) []byte
}

var codecs = map[Compression]Codec{}

type nocodec struct{}

func (nocodec) NewReader(r io.Reader) io.ReadCloser {
	ret, ok := r.(io.ReadCloser)
	if !ok {
		return ioutil.NopCloser(r)
	}
	return ret
}

func (nocodec) Decode(dst, src []byte) []byte {
	if dst != nil {
		copy(dst, src)
	}
	return dst
}

type writerNopCloser struct {
	io.Writer
}

func (writerNopCloser) Close() error {
	return nil
}

func (nocodec) Encode(dst, src []byte) []byte {
	copy(dst, src)
	return dst
}

func (nocodec) EncodeLevel(dst, src []byte, _ int) []byte {
	copy(dst, src)
	return dst
}

func (nocodec) NewWriter(w io.Writer) io.WriteCloser {
	ret, ok := w.(io.WriteCloser)
	if !ok {
		return writerNopCloser{w}
	}
	return ret
}

func (n nocodec) NewWriterLevel(w io.Writer, _ int) (io.WriteCloser, error) {
	return n.NewWriter(w), nil
}

func (nocodec) CompressBound(len int64) int64 { return len }

func init() {
	codecs[Codecs.Uncompressed] = nocodec{}
}

// GetCodec returns a Codec interface for the requested Compression type
func GetCodec(typ Compression) Codec {
	ret, ok := codecs[typ]
	if !ok {
		// return codecs[Codecs.Uncompressed]
		panic("compression for " + typ.String() + " unimplemented")
	}
	return ret
}

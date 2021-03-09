package compress

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/pierrec/lz4/v4"
)

type lz4Codec struct{}

func (lz4Codec) NewReader(r io.Reader) io.ReadCloser {
	rdr := lz4.NewReader(r)
	return ioutil.NopCloser(rdr)
}

func (lz4Codec) EncodeLevel(dst, src []byte, level int) []byte {
	var c lz4.CompressorHC
	if level == DefaultCompressionLevel {
		level = int(lz4.Level1)
	}
	n, err := c.CompressBlock(src, dst[:cap(dst)])
	if err != nil {
		panic(err)
	}
	return dst[:n]
}

func (lz4Codec) Encode(dst, src []byte) []byte {
	var c lz4.Compressor
	n, err := c.CompressBlock(src, dst[:cap(dst)])
	if err != nil {
		panic(err)
	}
	return dst[:n]
}

func (lz4Codec) Decode(dst, src []byte) []byte {
	if dst != nil {
		n, err := lz4.UncompressBlock(src, dst)
		if err != nil {
			panic(err)
		}
		return dst[:n]
	}

	rdr := lz4.NewReader(bytes.NewReader(src))
	dst, err := ioutil.ReadAll(rdr)
	if err != nil {
		panic(err)
	}
	return dst
}

func (lz4Codec) CompressBound(len int64) int64 {
	return int64(lz4.CompressBlockBound(int(len)))
}

func (lz4Codec) NewWriter(w io.Writer) io.WriteCloser {
	return lz4.NewWriter(w)
}

func (lz4Codec) NewWriterLevel(w io.Writer, level int) (io.WriteCloser, error) {
	out := lz4.NewWriter(w)
	if level == DefaultCompressionLevel {
		out.Apply(lz4.CompressionLevelOption(lz4.Level1), lz4.ChecksumOption(false))
		return out, nil
	}
	err := out.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(level)))
	return out, err
}

func init() {
	codecs[Codecs.Lz4] = lz4Codec{}
}

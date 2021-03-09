package compress

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/klauspost/compress/gzip"
	"golang.org/x/xerrors"
)

type gzipCodec struct{}

func (gzipCodec) NewReader(r io.Reader) io.ReadCloser {
	ret, err := gzip.NewReader(r)
	if err != nil {
		panic(xerrors.Errorf("codec: gzip: %w", err))
	}
	return ret
}

func (gzipCodec) Decode(dst, src []byte) []byte {
	rdr, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		panic(err)
	}

	if dst != nil {
		n, err := io.ReadFull(rdr, dst)
		if err != nil {
			panic(err)
		}
		return dst[:n]
	}

	dst, err = ioutil.ReadAll(rdr)
	if err != nil {
		panic(err)
	}

	return dst
}

func (g gzipCodec) EncodeLevel(dst, src []byte, level int) []byte {
	maxlen := int(g.CompressBound(int64(len(src))))
	if dst == nil || cap(dst) < maxlen {
		dst = make([]byte, 0, maxlen)
	}
	buf := bytes.NewBuffer(dst[:0])
	w, err := gzip.NewWriterLevel(buf, level)
	if err != nil {
		panic(err)
	}
	_, err = w.Write(src)
	if err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (g gzipCodec) Encode(dst, src []byte) []byte {
	return g.EncodeLevel(dst, src, DefaultCompressionLevel)
}

func (gzipCodec) CompressBound(len int64) int64 {
	return len + ((len + 7) >> 3) + ((len + 63) >> 6) + 5
}

func (gzipCodec) NewWriter(w io.Writer) io.WriteCloser {
	return gzip.NewWriter(w)
}

func (gzipCodec) NewWriterLevel(w io.Writer, level int) (io.WriteCloser, error) {
	return gzip.NewWriterLevel(w, level)
}

func init() {
	codecs[Codecs.Gzip] = gzipCodec{}
}

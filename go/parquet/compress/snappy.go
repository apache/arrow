package compress

import (
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
)

type snappyCodec struct{}

func (snappyCodec) Encode(dst, src []byte) []byte {
	return snappy.Encode(dst, src)
}

func (snappyCodec) EncodeLevel(dst, src []byte, _ int) []byte {
	return snappy.Encode(dst, src)
}

func (snappyCodec) Decode(dst, src []byte) []byte {
	dst, err := snappy.Decode(dst, src)
	if err != nil {
		panic(err)
	}
	return dst
}

func (snappyCodec) NewReader(r io.Reader) io.ReadCloser {
	return ioutil.NopCloser(snappy.NewReader(r))
}

func (snappyCodec) CompressBound(len int64) int64 {
	return int64(snappy.MaxEncodedLen(int(len)))
}

func (snappyCodec) NewWriter(w io.Writer) io.WriteCloser {
	return snappy.NewBufferedWriter(w)
}

func (s snappyCodec) NewWriterLevel(w io.Writer, _ int) (io.WriteCloser, error) {
	return s.NewWriter(w), nil
}

func init() {
	codecs[Codecs.Snappy] = snappyCodec{}
}

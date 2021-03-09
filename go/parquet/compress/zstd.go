package compress

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type zstdCodec struct{}

type zstdcloser struct {
	*zstd.Decoder
}

var (
	enc         *zstd.Encoder
	dec         *zstd.Decoder
	initEncoder sync.Once
	initDecoder sync.Once
)

func getencoder() *zstd.Encoder {
	initEncoder.Do(func() {
		enc, _ = zstd.NewWriter(nil, zstd.WithZeroFrames(true))
	})
	return enc
}

func getdecoder() *zstd.Decoder {
	initDecoder.Do(func() {
		dec, _ = zstd.NewReader(nil)
	})
	return dec
}

func (zstdCodec) Decode(dst, src []byte) []byte {
	dst, err := getdecoder().DecodeAll(src, dst[:0])
	if err != nil {
		panic(err)
	}
	return dst
}

func (z *zstdcloser) Close() error {
	z.Decoder.Close()
	return nil
}

func (zstdCodec) NewReader(r io.Reader) io.ReadCloser {
	ret, _ := zstd.NewReader(r)
	return &zstdcloser{ret}
}

func (zstdCodec) NewWriter(w io.Writer) io.WriteCloser {
	ret, _ := zstd.NewWriter(w)
	return ret
}

func (zstdCodec) NewWriterLevel(w io.Writer, level int) (io.WriteCloser, error) {
	var compressLevel zstd.EncoderLevel
	if level == DefaultCompressionLevel {
		compressLevel = zstd.SpeedDefault
	} else {
		compressLevel = zstd.EncoderLevelFromZstd(level)
	}
	return zstd.NewWriter(w, zstd.WithEncoderLevel(compressLevel))
}

func (z zstdCodec) Encode(dst, src []byte) []byte {
	return getencoder().EncodeAll(src, dst[:0])
}

func (z zstdCodec) EncodeLevel(dst, src []byte, level int) []byte {
	compressLevel := zstd.EncoderLevelFromZstd(level)
	if level == DefaultCompressionLevel {
		compressLevel = zstd.SpeedDefault
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true), zstd.WithEncoderLevel(compressLevel))
	return enc.EncodeAll(src, dst[:0])
}

// from zstd.h
func (zstdCodec) CompressBound(len int64) int64 {
	extra := ((128 << 10) - len) >> 11
	if len >= (128 << 10) {
		extra = 0
	}
	return len + (len >> 8) + extra
}

func init() {
	codecs[Codecs.Zstd] = zstdCodec{}
}

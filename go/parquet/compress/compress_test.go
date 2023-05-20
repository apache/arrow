// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compress_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/stretchr/testify/assert"
)

const (
	RandomDataSize       = 3 * 1024 * 1024
	CompressibleDataSize = 8 * 1024 * 1024
)

func makeRandomData(size int) []byte {
	ret := make([]byte, size)
	r := rand.New(rand.NewSource(1234))
	r.Read(ret)
	return ret
}

func makeCompressibleData(size int) []byte {
	const base = "Apache Arrow is a cross-language development platform for in-memory data"

	data := make([]byte, size)
	n := copy(data, base)
	for i := n; i < len(data); i *= 2 {
		copy(data[i:], data[:i])
	}
	return data
}

func TestErrorForUnimplemented(t *testing.T) {
	_, err := compress.GetCodec(compress.Codecs.Lzo)
	assert.Error(t, err)

	_, err = compress.GetCodec(compress.Codecs.Lz4)
	assert.Error(t, err)
}

func TestCompressDataOneShot(t *testing.T) {
	tests := []struct {
		c compress.Compression
	}{
		{compress.Codecs.Uncompressed},
		{compress.Codecs.Snappy},
		{compress.Codecs.Gzip},
		{compress.Codecs.Brotli},
		{compress.Codecs.Zstd},
		// {compress.Codecs.Lzo},
		// {compress.Codecs.Lz4},
	}

	for _, tt := range tests {
		t.Run(tt.c.String(), func(t *testing.T) {
			codec, err := compress.GetCodec(tt.c)
			assert.NoError(t, err)
			data := makeCompressibleData(CompressibleDataSize)

			buf := make([]byte, codec.CompressBound(int64(len(data))))
			compressed := codec.Encode(buf, data)
			assert.Same(t, &buf[0], &compressed[0])

			out := make([]byte, len(data))
			uncompressed := codec.Decode(out, compressed)
			assert.Same(t, &out[0], &uncompressed[0])

			assert.Exactly(t, data, uncompressed)
		})
	}
}

func TestCompressReaderWriter(t *testing.T) {
	tests := []struct {
		c compress.Compression
	}{
		{compress.Codecs.Uncompressed},
		{compress.Codecs.Snappy},
		{compress.Codecs.Gzip},
		{compress.Codecs.Brotli},
		{compress.Codecs.Zstd},
		// {compress.Codecs.Lzo},
		// {compress.Codecs.Lz4},
	}

	for _, tt := range tests {
		t.Run(tt.c.String(), func(t *testing.T) {
			var buf bytes.Buffer
			codec, err := compress.GetCodec(tt.c)
			assert.NoError(t, err)
			data := makeRandomData(RandomDataSize)

			wr := codec.NewWriter(&buf)

			const chunkSize = 1111
			input := data
			for len(input) > 0 {
				var (
					n   int
					err error
				)
				if len(input) > chunkSize {
					n, err = wr.Write(input[:chunkSize])
				} else {
					n, err = wr.Write(input)
				}

				assert.NoError(t, err)
				input = input[n:]
			}
			wr.Close()

			rdr := codec.NewReader(&buf)
			out, err := io.ReadAll(rdr)
			assert.NoError(t, err)
			assert.Exactly(t, data, out)
		})
	}
}

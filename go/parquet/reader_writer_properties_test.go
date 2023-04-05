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

package parquet_test

import (
	"bytes"
	"testing"

	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/compress"
	"github.com/stretchr/testify/assert"
)

func TestReaderPropBasics(t *testing.T) {
	props := parquet.NewReaderProperties(nil)
	assert.Equal(t, parquet.DefaultBufSize, props.BufferSize)
	assert.False(t, props.BufferedStreamEnabled)
}

func TestWriterPropBasics(t *testing.T) {
	props := parquet.NewWriterProperties()

	assert.Equal(t, parquet.DefaultDataPageSize, props.DataPageSize())
	assert.Equal(t, parquet.DefaultDictionaryPageSizeLimit, props.DictionaryPageSizeLimit())
	assert.Equal(t, parquet.V2_LATEST, props.Version())
	assert.Equal(t, parquet.DataPageV1, props.DataPageVersion())
}

func TestWriterPropAdvanced(t *testing.T) {
	props := parquet.NewWriterProperties(
		parquet.WithCompressionFor("gzip", compress.Codecs.Gzip),
		parquet.WithCompressionFor("zstd", compress.Codecs.Zstd),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithEncoding(parquet.Encodings.DeltaBinaryPacked),
		parquet.WithEncodingFor("delta-length", parquet.Encodings.DeltaLengthByteArray),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithRootName("test2"),
		parquet.WithRootRepetition(parquet.Repetitions.Required))

	assert.Equal(t, compress.Codecs.Gzip, props.CompressionPath(parquet.ColumnPathFromString("gzip")))
	assert.Equal(t, compress.Codecs.Zstd, props.CompressionFor("zstd"))
	assert.Equal(t, compress.Codecs.Snappy, props.CompressionPath(parquet.ColumnPathFromString("delta-length")))
	assert.Equal(t, parquet.Encodings.DeltaBinaryPacked, props.EncodingFor("gzip"))
	assert.Equal(t, parquet.Encodings.DeltaLengthByteArray, props.EncodingPath(parquet.ColumnPathFromString("delta-length")))
	assert.Equal(t, parquet.DataPageV2, props.DataPageVersion())
	assert.Equal(t, "test2", props.RootName())
	assert.Equal(t, parquet.Repetitions.Required, props.RootRepetition())
}

func TestReaderPropsGetStreamInsufficient(t *testing.T) {
	data := "shorter than expected"
	buf := memory.NewBufferBytes([]byte(data))
	rdr := bytes.NewReader(buf.Bytes())

	props := parquet.NewReaderProperties(nil)
	_, err := props.GetStream(rdr, 12, 15)
	assert.Error(t, err)
}

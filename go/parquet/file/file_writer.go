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

package file

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/internal/encryption"
	"github.com/apache/arrow/go/v14/parquet/internal/utils"
	"github.com/apache/arrow/go/v14/parquet/metadata"
	"github.com/apache/arrow/go/v14/parquet/schema"
)

// Writer is the primary interface for writing a parquet file
type Writer struct {
	sink           utils.WriteCloserTell
	open           bool
	props          *parquet.WriterProperties
	rowGroups      int
	nrows          int
	metadata       metadata.FileMetaDataBuilder
	fileEncryptor  encryption.FileEncryptor
	rowGroupWriter *rowGroupWriter

	// The Schema of this writer
	Schema *schema.Schema
}

type writerConfig struct {
	props            *parquet.WriterProperties
	keyValueMetadata metadata.KeyValueMetadata
}

type WriteOption func(*writerConfig)

func WithWriterProps(props *parquet.WriterProperties) WriteOption {
	return func(c *writerConfig) {
		c.props = props
	}
}

func WithWriteMetadata(meta metadata.KeyValueMetadata) WriteOption {
	return func(c *writerConfig) {
		c.keyValueMetadata = meta
	}
}

// NewParquetWriter returns a Writer that writes to the provided WriteSeeker with the given schema.
//
// If props is nil, then the default Writer Properties will be used. If the key value metadata is not nil,
// it will be added to the file.
func NewParquetWriter(w io.Writer, sc *schema.GroupNode, opts ...WriteOption) *Writer {
	config := &writerConfig{}
	for _, o := range opts {
		o(config)
	}
	if config.props == nil {
		config.props = parquet.NewWriterProperties()
	}

	fileSchema := schema.NewSchema(sc)
	fw := &Writer{
		props:  config.props,
		sink:   &utils.TellWrapper{Writer: w},
		open:   true,
		Schema: fileSchema,
	}

	fw.metadata = *metadata.NewFileMetadataBuilder(fw.Schema, fw.props, config.keyValueMetadata)
	fw.startFile()
	return fw
}

// NumColumns returns the number of columns to write as defined by the schema.
func (fw *Writer) NumColumns() int { return fw.Schema.NumColumns() }

// NumRowGroups returns the current number of row groups that will be written for this file.
func (fw *Writer) NumRowGroups() int { return fw.rowGroups }

// NumRows returns the current number of rows that have be written
func (fw *Writer) NumRows() int { return fw.nrows }

// Properties returns the writer properties that are in use for this file.
func (fw *Writer) Properties() *parquet.WriterProperties { return fw.props }

// AppendBufferedRowGroup appends a rowgroup to the file and returns a writer
// that buffers the row group in memory allowing writing multiple columns
// at once to the row group. Data is not flushed out until the row group
// is closed.
//
// When calling Close, all columns must have the same number of rows written.
func (fw *Writer) AppendBufferedRowGroup() BufferedRowGroupWriter {
	return fw.appendRowGroup(true)
}

// AppendRowGroup appends a row group to the file and returns a writer
// that writes columns to the row group in serial via calling NextColumn.
//
// When calling NextColumn, the same number of rows need to have been written
// to each column before moving on. Otherwise the rowgroup writer will panic.
func (fw *Writer) AppendRowGroup() SerialRowGroupWriter {
	return fw.appendRowGroup(false)
}

func (fw *Writer) appendRowGroup(buffered bool) *rowGroupWriter {
	if fw.rowGroupWriter != nil {
		fw.rowGroupWriter.Close()
	}
	fw.rowGroups++
	rgMeta := fw.metadata.AppendRowGroup()
	fw.rowGroupWriter = newRowGroupWriter(fw.sink, rgMeta, int16(fw.rowGroups)-1, fw.props, buffered, fw.fileEncryptor)
	return fw.rowGroupWriter
}

func (fw *Writer) startFile() {
	encryptionProps := fw.props.FileEncryptionProperties()
	magic := magicBytes
	if encryptionProps != nil {
		// check that all columns in columnEncryptionProperties exist in the schema
		encryptedCols := encryptionProps.EncryptedColumns()
		// if columnEncryptionProperties is empty, every column in the file schema will be encrypted with the footer key
		if len(encryptedCols) != 0 {
			colPaths := make(map[string]bool)
			for i := 0; i < fw.Schema.NumColumns(); i++ {
				colPaths[fw.Schema.Column(i).Path()] = true
			}
			for k := range encryptedCols {
				if _, ok := colPaths[k]; !ok {
					panic("encrypted column " + k + " not found in file schema")
				}
			}
		}

		fw.fileEncryptor = encryption.NewFileEncryptor(encryptionProps, fw.props.Allocator())
		if encryptionProps.EncryptedFooter() {
			magic = magicEBytes
		}
	}
	n, err := fw.sink.Write(magic)
	if n != 4 || err != nil {
		panic("failed to write magic number")
	}
}

// AppendKeyValueMetadata appends a key/value pair to the existing key/value metadata
func (fw *Writer) AppendKeyValueMetadata(key string, value string) error {
	return fw.metadata.AppendKeyValueMetadata(key, value)
}

// Close closes any open row group writer and writes the file footer. Subsequent
// calls to close will have no effect.
func (fw *Writer) Close() (err error) {
	if fw.open {
		// if any functions here panic, we set open to be false so
		// that this doesn't get called again
		fw.open = false
		if fw.rowGroupWriter != nil {
			fw.nrows += fw.rowGroupWriter.nrows
			fw.rowGroupWriter.Close()
		}
		fw.rowGroupWriter = nil
		defer func() {
			ierr := fw.sink.Close()
			if err != nil {
				if ierr != nil {
					err = fmt.Errorf("error on close:%w, %s", err, ierr)
				}
				return
			}

			err = ierr
		}()

		fileEncryptProps := fw.props.FileEncryptionProperties()
		if fileEncryptProps == nil { // non encrypted file
			fileMetadata, err := fw.metadata.Finish()
			if err != nil {
				return err
			}

			_, err = writeFileMetadata(fileMetadata, fw.sink)
			return err
		}

		return fw.closeEncryptedFile(fileEncryptProps)
	}
	return nil
}

func (fw *Writer) closeEncryptedFile(props *parquet.FileEncryptionProperties) error {
	// encrypted file with encrypted footer
	if props.EncryptedFooter() {
		fileMetadata, err := fw.metadata.Finish()
		if err != nil {
			return err
		}

		footerLen := int64(0)

		cryptoMetadata := fw.metadata.GetFileCryptoMetaData()
		n, err := writeFileCryptoMetadata(cryptoMetadata, fw.sink)
		if err != nil {
			return err
		}

		footerLen += n
		footerEncryptor := fw.fileEncryptor.GetFooterEncryptor()
		n, err = writeEncryptedFileMetadata(fileMetadata, fw.sink, footerEncryptor, true)
		if err != nil {
			return err
		}
		footerLen += n

		if err = binary.Write(fw.sink, binary.LittleEndian, uint32(footerLen)); err != nil {
			return err
		}
		if _, err = fw.sink.Write(magicEBytes); err != nil {
			return err
		}
	} else {
		fileMetadata, err := fw.metadata.Finish()
		if err != nil {
			return err
		}
		footerSigningEncryptor := fw.fileEncryptor.GetFooterSigningEncryptor()
		if _, err = writeEncryptedFileMetadata(fileMetadata, fw.sink, footerSigningEncryptor, false); err != nil {
			return err
		}
	}
	if fw.fileEncryptor != nil {
		fw.fileEncryptor.WipeOutEncryptionKeys()
	}
	return nil
}

func writeFileMetadata(fileMetadata *metadata.FileMetaData, w io.Writer) (n int64, err error) {
	n, err = fileMetadata.WriteTo(w, nil)
	if err != nil {
		return
	}

	if err = binary.Write(w, binary.LittleEndian, uint32(n)); err != nil {
		return
	}
	if _, err = w.Write(magicBytes); err != nil {
		return
	}
	return n + int64(4+len(magicBytes)), nil
}

func writeEncryptedFileMetadata(fileMetadata *metadata.FileMetaData, w io.Writer, encryptor encryption.Encryptor, encryptFooter bool) (n int64, err error) {
	n, err = fileMetadata.WriteTo(w, encryptor)
	if encryptFooter {
		return
	}
	if err != nil {
		return
	}
	if err = binary.Write(w, binary.LittleEndian, uint32(n)); err != nil {
		return
	}
	if _, err = w.Write(magicBytes); err != nil {
		return
	}
	return n + int64(4+len(magicBytes)), nil
}

func writeFileCryptoMetadata(crypto *metadata.FileCryptoMetadata, w io.Writer) (int64, error) {
	return crypto.WriteTo(w)
}

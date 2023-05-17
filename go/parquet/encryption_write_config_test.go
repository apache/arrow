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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/schema"
	"github.com/stretchr/testify/suite"
)

/*
 * This file contains unit-tests for writing encrypted Parquet files with
 * different encryption configurations.
 * The files are saved in temporary folder and will be deleted after reading
 * them in encryption_read_config_test.go test.
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * Each unit-test creates a single parquet file with eight columns using one of the
 * following encryption configurations:
 *
 *  - Encryption configuration 1:   Encrypt all columns and the footer with the same key.
 *                                  (uniform encryption)
 *  - Encryption configuration 2:   Encrypt two columns and the footer, with different
 *                                  keys.
 *  - Encryption configuration 3:   Encrypt two columns, with different keys.
 *                                  Don’t encrypt footer (to enable legacy readers)
 *                                  - plaintext footer mode.
 *  - Encryption configuration 4:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix for file identity
 *                                  verification.
 *  - Encryption configuration 5:   Encrypt two columns and the footer, with different
 *                                  keys. Supply aad_prefix, and call
 *                                  disable_aad_prefix_storage to prevent file
 *                                  identity storage in file metadata.
 *  - Encryption configuration 6:   Encrypt two columns and the footer, with different
 *                                  keys. Use the alternative (AES_GCM_CTR_V1) algorithm.
 */

var (
	tempdir string
)

type EncryptionConfigTestSuite struct {
	suite.Suite

	pathToDoubleField    string
	pathToFloatField     string
	fileName             string
	numRgs               int
	rowsPerRG            int
	schema               *schema.GroupNode
	footerEncryptionKey  string
	columnEncryptionKey1 string
	columnEncryptionKey2 string
}

func (en *EncryptionConfigTestSuite) encryptFile(configs *parquet.FileEncryptionProperties, filename string) {
	filename = filepath.Join(tempdir, filename)

	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy), parquet.WithEncryptionProperties(configs))
	outFile, err := os.Create(filename)
	en.Require().NoError(err)
	en.Require().NotNil(outFile)

	writer := file.NewParquetWriter(outFile, en.schema, file.WithWriterProps(props))
	defer writer.Close()

	for r := 0; r < en.numRgs; r++ {
		var (
			bufferedMode = r%2 == 0
			rgr          file.RowGroupWriter
			colIndex     = 0
		)

		if bufferedMode {
			rgr = writer.AppendBufferedRowGroup()
		} else {
			rgr = writer.AppendRowGroup()
		}

		nextColumn := func() file.ColumnChunkWriter {
			defer func() { colIndex++ }()
			if bufferedMode {
				cw, _ := rgr.(file.BufferedRowGroupWriter).Column(colIndex)
				return cw
			}
			cw, _ := rgr.(file.SerialRowGroupWriter).NextColumn()
			return cw
		}

		// write the bool col
		boolWriter := nextColumn().(*file.BooleanColumnChunkWriter)
		for i := 0; i < en.rowsPerRG; i++ {
			value := (i % 2) == 0
			n, err := boolWriter.WriteBatch([]bool{value}, nil, nil)
			en.EqualValues(1, n)
			en.Require().NoError(err)
		}

		// write the int32 col
		int32Writer := nextColumn().(*file.Int32ColumnChunkWriter)
		for i := int32(0); i < int32(en.rowsPerRG); i++ {
			n, err := int32Writer.WriteBatch([]int32{i}, nil, nil)
			en.EqualValues(1, n)
			en.Require().NoError(err)
		}

		// write the int64 column, each row repeats twice
		int64Writer := nextColumn().(*file.Int64ColumnChunkWriter)
		for i := 0; i < 2*en.rowsPerRG; i++ {
			var (
				defLevel       = [1]int16{1}
				repLevel       = [1]int16{0}
				value    int64 = int64(i) * 1000 * 1000 * 1000 * 1000
			)
			if i%2 == 0 {
				repLevel[0] = 1
			}

			n, err := int64Writer.WriteBatch([]int64{value}, defLevel[:], repLevel[:])
			en.EqualValues(1, n)
			en.Require().NoError(err)
		}

		// write the int96 col
		int96Writer := nextColumn().(*file.Int96ColumnChunkWriter)
		for i := 0; i < en.rowsPerRG; i++ {
			val := parquet.Int96{}
			binary.LittleEndian.PutUint32(val[:], uint32(i))
			binary.LittleEndian.PutUint32(val[4:], uint32(i+1))
			binary.LittleEndian.PutUint32(val[8:], uint32(i+2))
			n, err := int96Writer.WriteBatch([]parquet.Int96{val}, nil, nil)
			en.EqualValues(1, n)
			en.Require().NoError(err)
		}

		// write the float column
		floatWriter := nextColumn().(*file.Float32ColumnChunkWriter)
		for i := 0; i < en.rowsPerRG; i++ {
			val := float32(i) * 1.1
			n, err := floatWriter.WriteBatch([]float32{val}, nil, nil)
			en.EqualValues(1, n)
			en.Require().NoError(err)
		}

		// write the double column
		doubleWriter := nextColumn().(*file.Float64ColumnChunkWriter)
		for i := 0; i < en.rowsPerRG; i++ {
			value := float64(i) * 1.1111111
			n, err := doubleWriter.WriteBatch([]float64{value}, nil, nil)
			en.EqualValues(1, n)
			en.Require().NoError(err)
		}

		// write the bytearray column. make every alternate value NULL
		baWriter := nextColumn().(*file.ByteArrayColumnChunkWriter)
		for i := 0; i < en.rowsPerRG; i++ {
			var (
				n     int64
				err   error
				hello = []byte{'p', 'a', 'r', 'q', 'u', 'e', 't', 0, 0, 0}
			)
			hello[7] = byte(int('0') + i/100)
			hello[8] = byte(int('0') + (i/10)%10)
			hello[9] = byte(int('0') + i%10)
			if i%2 == 0 {
				n, err = baWriter.WriteBatch([]parquet.ByteArray{hello}, []int16{1}, nil)
				en.EqualValues(1, n)
			} else {
				n, err = baWriter.WriteBatch([]parquet.ByteArray{nil}, []int16{0}, nil)
				en.Zero(n)
			}

			en.Require().NoError(err)
		}

		// write fixedlength byte array column
		flbaWriter := nextColumn().(*file.FixedLenByteArrayColumnChunkWriter)
		for i := 0; i < en.rowsPerRG; i++ {
			v := byte(i)
			value := parquet.FixedLenByteArray{v, v, v, v, v, v, v, v, v, v}
			n, err := flbaWriter.WriteBatch([]parquet.FixedLenByteArray{value}, nil, nil)
			en.EqualValues(1, n)
			en.Require().NoError(err)
		}
	}
}

func (en *EncryptionConfigTestSuite) SetupSuite() {
	var err error
	tempdir, err = os.MkdirTemp("", "parquet-encryption-test-*")
	en.Require().NoError(err)
	fmt.Println(tempdir)

	en.fileName = FileName
	en.rowsPerRG = 50
	en.numRgs = 5
	en.pathToDoubleField = "double_field"
	en.pathToFloatField = "float_field"
	en.footerEncryptionKey = FooterEncryptionKey
	en.columnEncryptionKey1 = ColumnEncryptionKey1
	en.columnEncryptionKey2 = ColumnEncryptionKey2

	fields := make(schema.FieldList, 0)
	// create a primitive node named "boolean_field" with type BOOLEAN
	// repetition:REQUIRED
	fields = append(fields, schema.NewBooleanNode("boolean_field", parquet.Repetitions.Required, -1))
	// create a primitive node named "int32_field" with type INT32 repetition REQUIRED
	// and logical type: TIME_MILLIS
	f, _ := schema.NewPrimitiveNodeLogical("int32_field", parquet.Repetitions.Required,
		schema.NewTimeLogicalType(true, schema.TimeUnitMillis), parquet.Types.Int32, 0, -1)
	fields = append(fields, f)

	// create a primitive node named "int64_field" with type int64, repetition:REPEATED
	fields = append(fields, schema.NewInt64Node("int64_field", parquet.Repetitions.Repeated, -1))

	fields = append(fields,
		schema.NewInt96Node("int96_field", parquet.Repetitions.Required, -1),
		schema.NewFloat32Node("float_field", parquet.Repetitions.Required, -1),
		schema.NewFloat64Node("double_field", parquet.Repetitions.Required, -1))

	// create a primitive node named ba_field with type:BYTE_ARRAY repetition:OPTIONAL
	fields = append(fields, schema.NewByteArrayNode("ba_field", parquet.Repetitions.Optional, -1))

	// create a primitive node for flba_field
	fields = append(fields, schema.NewFixedLenByteArrayNode("flba_field", parquet.Repetitions.Required, 10, -1))

	// flba_field fixedlenbytearray
	en.schema, _ = schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
}

// Encryption Config 1: Encrypt All columns and the footer with the same key
// (uniform encryption)
func (en *EncryptionConfigTestSuite) TestUniformEncryption() {
	props := parquet.NewFileEncryptionProperties(en.footerEncryptionKey, parquet.WithFooterKeyMetadata("kf"))
	en.encryptFile(props, "tmp_uniform_encryption.parquet.encrypted")
}

// Encryption config 2: Encrypt Two Columns and the Footer, with different keys
func (en *EncryptionConfigTestSuite) TestEncryptTwoColumnsAndFooter() {
	encryptCols := make(parquet.ColumnPathToEncryptionPropsMap)
	encryptCols[en.pathToDoubleField] = parquet.NewColumnEncryptionProperties(en.pathToDoubleField, parquet.WithKey(en.columnEncryptionKey1), parquet.WithKeyID("kc1"))
	encryptCols[en.pathToFloatField] = parquet.NewColumnEncryptionProperties(en.pathToFloatField, parquet.WithKey(en.columnEncryptionKey2), parquet.WithKeyID("kc2"))

	props := parquet.NewFileEncryptionProperties(en.footerEncryptionKey, parquet.WithFooterKeyMetadata("kf"), parquet.WithEncryptedColumns(encryptCols))
	en.encryptFile(props, "tmp_encrypt_columns_and_footer.parquet.encrypted")
}

// Encryption Config 3: encrypt two columns, with different keys.
// plaintext footer
// (plaintext footer mode, readable by legacy readers)
func (en *EncryptionConfigTestSuite) TestEncryptTwoColumnsPlaintextFooter() {
	encryptCols := make(parquet.ColumnPathToEncryptionPropsMap)
	encryptCols[en.pathToDoubleField] = parquet.NewColumnEncryptionProperties(en.pathToDoubleField, parquet.WithKey(en.columnEncryptionKey1), parquet.WithKeyID("kc1"))
	encryptCols[en.pathToFloatField] = parquet.NewColumnEncryptionProperties(en.pathToFloatField, parquet.WithKey(en.columnEncryptionKey2), parquet.WithKeyID("kc2"))

	props := parquet.NewFileEncryptionProperties(en.footerEncryptionKey, parquet.WithFooterKeyMetadata("kf"), parquet.WithEncryptedColumns(encryptCols), parquet.WithPlaintextFooter())
	en.encryptFile(props, "tmp_encrypt_columns_plaintext_footer.parquet.encrypted")
}

// Encryption Config 4: Encrypt two columns and the footer, with different keys
// use aad_prefix
func (en *EncryptionConfigTestSuite) TestEncryptTwoColumnsAndFooterWithAadPrefix() {
	encryptCols := make(parquet.ColumnPathToEncryptionPropsMap)
	encryptCols[en.pathToDoubleField] = parquet.NewColumnEncryptionProperties(en.pathToDoubleField, parquet.WithKey(en.columnEncryptionKey1), parquet.WithKeyID("kc1"))
	encryptCols[en.pathToFloatField] = parquet.NewColumnEncryptionProperties(en.pathToFloatField, parquet.WithKey(en.columnEncryptionKey2), parquet.WithKeyID("kc2"))

	props := parquet.NewFileEncryptionProperties(en.footerEncryptionKey, parquet.WithFooterKeyMetadata("kf"), parquet.WithEncryptedColumns(encryptCols), parquet.WithAadPrefix(en.fileName))
	en.encryptFile(props, "tmp_encrypt_columns_and_footer_aad.parquet.encrypted")
}

// Encryption Config 5: Encrypt Two columns and the footer, with different keys
// use aad_prefix and disable_aad_prefix_storage
func (en *EncryptionConfigTestSuite) TestEncryptTwoColumnsAndFooterWithAadPrefixDisableAadStorage() {
	encryptCols := make(parquet.ColumnPathToEncryptionPropsMap)
	encryptCols[en.pathToDoubleField] = parquet.NewColumnEncryptionProperties(en.pathToDoubleField, parquet.WithKey(en.columnEncryptionKey1), parquet.WithKeyID("kc1"))
	encryptCols[en.pathToFloatField] = parquet.NewColumnEncryptionProperties(en.pathToFloatField, parquet.WithKey(en.columnEncryptionKey2), parquet.WithKeyID("kc2"))

	props := parquet.NewFileEncryptionProperties(en.footerEncryptionKey, parquet.WithFooterKeyMetadata("kf"), parquet.WithAadPrefix(en.fileName), parquet.DisableAadPrefixStorage())
	en.encryptFile(props, "tmp_encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted")
}

// Encryption Config 6: Encrypt two columns and the footer, with different keys.
// Use AES_GCM_CTR_V1
func (en *EncryptionConfigTestSuite) TestEncryptTwoColumnsAndFooterAesGcmCtr() {
	encryptCols := make(parquet.ColumnPathToEncryptionPropsMap)
	encryptCols[en.pathToDoubleField] = parquet.NewColumnEncryptionProperties(en.pathToDoubleField, parquet.WithKey(en.columnEncryptionKey1), parquet.WithKeyID("kc1"))
	encryptCols[en.pathToFloatField] = parquet.NewColumnEncryptionProperties(en.pathToFloatField, parquet.WithKey(en.columnEncryptionKey2), parquet.WithKeyID("kc2"))

	props := parquet.NewFileEncryptionProperties(en.footerEncryptionKey, parquet.WithFooterKeyMetadata("kf"), parquet.WithEncryptedColumns(encryptCols), parquet.WithAlg(parquet.AesCtr))
	en.encryptFile(props, "tmp_encrypt_columns_and_footer_ctr.parquet.encrypted")
}

func TestFileEncryption(t *testing.T) {
	suite.Run(t, new(EncryptionConfigTestSuite))
}

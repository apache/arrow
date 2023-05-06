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
	"path"
	"testing"

	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/internal/encryption"
	"github.com/stretchr/testify/suite"
)

/*
 * This file contains a unit-test for reading encrypted Parquet files with
 * different decryption configurations.
 *
 * The unit-test is called multiple times, each time to decrypt parquet files using
 * different decryption configuration as described below.
 * In each call two encrypted files are read: one temporary file that was generated using
 * encryption_write_config_test.go test and will be deleted upon
 * reading it, while the second resides in
 * parquet-testing/data repository. Those two encrypted files were encrypted using the
 * same encryption configuration.
 * The encrypted parquet file names are passed as parameter to the unit-test.
 *
 * A detailed description of the Parquet Modular Encryption specification can be found
 * here:
 * https://github.com/apache/parquet-format/blob/encryption/Encryption.md
 *
 * The following decryption configurations are used to decrypt each parquet file:
 *
 *  - Decryption configuration 1:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key.
 *  - Decryption configuration 2:   Decrypt using key retriever that holds the keys of
 *                                  two encrypted columns and the footer key. Supplies
 *                                  aad_prefix to verify file identity.
 *  - Decryption configuration 3:   Decrypt using explicit column and footer keys
 *                                  (instead of key retrieval callback).
 *  - Decryption Configuration 4:   PlainText Footer mode - test legacy reads,
 *                                  read the footer + all non-encrypted columns.
 *                                  (pairs with encryption configuration 3)
 *
 * The encrypted parquet files that is read was encrypted using one of the configurations
 * below:
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

func getDataDir() string {
	datadir := os.Getenv("PARQUET_TEST_DATA")
	if datadir == "" {
		panic("please point the PARQUET_TEST_DATA environment variable to the test data dir")
	}
	return datadir
}

type TestDecryptionSuite struct {
	suite.Suite

	pathToDouble        string
	pathToFloat         string
	decryptionConfigs   []*parquet.FileDecryptionProperties
	footerEncryptionKey string
	colEncryptionKey1   string
	colEncryptionKey2   string
	fileName            string
	rowsPerRG           int
}

func (d *TestDecryptionSuite) TearDownSuite() {
	os.Remove(tempdir)
}

func TestFileEncryptionDecryption(t *testing.T) {
	suite.Run(t, new(EncryptionConfigTestSuite))
	suite.Run(t, new(TestDecryptionSuite))
}

func (d *TestDecryptionSuite) SetupSuite() {
	d.pathToDouble = "double_field"
	d.pathToFloat = "float_field"
	d.footerEncryptionKey = FooterEncryptionKey
	d.colEncryptionKey1 = ColumnEncryptionKey1
	d.colEncryptionKey2 = ColumnEncryptionKey2
	d.fileName = FileName
	d.rowsPerRG = 50 // same as write encryption test

	d.createDecryptionConfigs()
}

func (d *TestDecryptionSuite) createDecryptionConfigs() {
	// Decryption configuration 1: Decrypt using key retriever callback that holds the
	// keys of two encrypted columns and the footer key.
	stringKr1 := make(encryption.StringKeyIDRetriever)
	stringKr1.PutKey("kf", d.footerEncryptionKey)
	stringKr1.PutKey("kc1", d.colEncryptionKey1)
	stringKr1.PutKey("kc2", d.colEncryptionKey2)

	d.decryptionConfigs = append(d.decryptionConfigs,
		parquet.NewFileDecryptionProperties(parquet.WithKeyRetriever(stringKr1)))

	// Decryption configuration 2: Decrypt using key retriever callback that holds the
	// keys of two encrypted columns and the footer key. Supply aad_prefix.
	stringKr2 := make(encryption.StringKeyIDRetriever)
	stringKr2.PutKey("kf", d.footerEncryptionKey)
	stringKr2.PutKey("kc1", d.colEncryptionKey1)
	stringKr2.PutKey("kc2", d.colEncryptionKey2)
	d.decryptionConfigs = append(d.decryptionConfigs,
		parquet.NewFileDecryptionProperties(parquet.WithKeyRetriever(stringKr2), parquet.WithDecryptAadPrefix(d.fileName)))

	// Decryption configuration 3: Decrypt using explicit column and footer keys. Supply
	// aad_prefix.
	decryptCols := make(parquet.ColumnPathToDecryptionPropsMap)
	decryptCols[d.pathToFloat] = parquet.NewColumnDecryptionProperties(d.pathToFloat, parquet.WithDecryptKey(d.colEncryptionKey2))
	decryptCols[d.pathToDouble] = parquet.NewColumnDecryptionProperties(d.pathToDouble, parquet.WithDecryptKey(d.colEncryptionKey1))
	d.decryptionConfigs = append(d.decryptionConfigs,
		parquet.NewFileDecryptionProperties(parquet.WithFooterKey(d.footerEncryptionKey), parquet.WithColumnKeys(decryptCols)))

	// Decryption Configuration 4: use plaintext footer mode, read only footer + plaintext
	// columns.
	d.decryptionConfigs = append(d.decryptionConfigs, nil)
}

func (d *TestDecryptionSuite) decryptFile(filename string, decryptConfigNum int) {
	// if we get decryption_config_num = x then it means the actual number is x+1
	// and since we want decryption_config_num=4 we set the condition to 3
	props := parquet.NewReaderProperties(memory.DefaultAllocator)
	if decryptConfigNum != 3 {
		props.FileDecryptProps = d.decryptionConfigs[decryptConfigNum].Clone("")
	}

	fileReader, err := file.OpenParquetFile(filename, false, file.WithReadProps(props))
	if err != nil {
		panic(err)
	}
	defer fileReader.Close()
	// get metadata
	fileMetadata := fileReader.MetaData()
	// get number of rowgroups
	numRowGroups := len(fileMetadata.RowGroups)
	// number of columns
	numColumns := fileMetadata.Schema.NumColumns()
	d.Equal(8, numColumns)

	for r := 0; r < numRowGroups; r++ {
		rowGroupReader := fileReader.RowGroup(r)

		// get rowgroup meta
		rgMeta := fileMetadata.RowGroup(r)
		d.EqualValues(d.rowsPerRG, rgMeta.NumRows())

		valuesRead := 0
		rowsRead := int64(0)

		// get col reader for boolean column
		colReader, err := rowGroupReader.Column(0)
		if err != nil {
			panic(err)
		}
		boolReader := colReader.(*file.BooleanColumnChunkReader)

		// get column chunk metadata for boolean column
		boolMd, _ := rgMeta.ColumnChunk(0)
		d.EqualValues(d.rowsPerRG, boolMd.NumValues())

		// Read all rows in column
		i := 0
		for boolReader.HasNext() {
			var val [1]bool
			// read one value at a time. the number of rows read is returned. values
			// read contains the number of non-null rows
			rowsRead, valuesRead, _ = boolReader.ReadBatch(1, val[:], nil, nil)
			// ensure only 1 value is read
			d.EqualValues(1, rowsRead)
			// there are no null values
			d.EqualValues(1, valuesRead)
			// verify the value
			expected := i%2 == 0
			d.Equal(expected, val[0], "i: ", i)
			i++
		}
		d.EqualValues(i, boolMd.NumValues())

		// Get column reader for int32 column
		colReader, err = rowGroupReader.Column(1)
		if err != nil {
			panic(err)
		}
		int32reader := colReader.(*file.Int32ColumnChunkReader)

		int32md, _ := rgMeta.ColumnChunk(1)
		d.EqualValues(d.rowsPerRG, int32md.NumValues())
		// Read all rows in column
		i = 0
		for int32reader.HasNext() {
			var val [1]int32
			// read one value at a time. the number of rows read is returned. values
			// read contains the number of non-null rows
			rowsRead, valuesRead, _ = int32reader.ReadBatch(1, val[:], nil, nil)
			// ensure only 1 value is read
			d.EqualValues(1, rowsRead)
			// there are no null values
			d.EqualValues(1, valuesRead)
			// verify the value
			d.EqualValues(i, val[0])
			i++
		}
		d.EqualValues(i, int32md.NumValues())

		// Get column reader for int64 column
		colReader, err = rowGroupReader.Column(2)
		if err != nil {
			panic(err)
		}
		int64reader := colReader.(*file.Int64ColumnChunkReader)

		int64md, _ := rgMeta.ColumnChunk(2)
		// repeated column, we should have 2*d.rowsPerRG values
		d.EqualValues(2*d.rowsPerRG, int64md.NumValues())
		// Read all rows in column
		i = 0
		for int64reader.HasNext() {
			var (
				val [1]int64
				def [1]int16
				rep [1]int16
			)

			// read one value at a time. the number of rows read is returned. values
			// read contains the number of non-null rows
			rowsRead, valuesRead, _ = int64reader.ReadBatch(1, val[:], def[:], rep[:])
			// ensure only 1 value is read
			d.EqualValues(1, rowsRead)
			// there are no null values
			d.EqualValues(1, valuesRead)
			// verify the value
			expectedValue := int64(i) * 1000 * 1000 * 1000 * 1000
			d.Equal(expectedValue, val[0])
			if i%2 == 0 {
				d.EqualValues(1, rep[0])
			} else {
				d.Zero(rep[0])
			}
			i++
		}
		d.EqualValues(i, int64md.NumValues())

		// Get column reader for int96 column
		colReader, err = rowGroupReader.Column(3)
		if err != nil {
			panic(err)
		}
		int96reader := colReader.(*file.Int96ColumnChunkReader)

		int96md, _ := rgMeta.ColumnChunk(3)
		// Read all rows in column
		i = 0
		for int96reader.HasNext() {
			var (
				val [1]parquet.Int96
			)

			// read one value at a time. the number of rows read is returned. values
			// read contains the number of non-null rows
			rowsRead, valuesRead, _ = int96reader.ReadBatch(1, val[:], nil, nil)
			// ensure only 1 value is read
			d.EqualValues(1, rowsRead)
			// there are no null values
			d.EqualValues(1, valuesRead)
			// verify the value
			var expectedValue parquet.Int96
			binary.LittleEndian.PutUint32(expectedValue[:4], uint32(i))
			binary.LittleEndian.PutUint32(expectedValue[4:], uint32(i+1))
			binary.LittleEndian.PutUint32(expectedValue[8:], uint32(i+2))
			d.Equal(expectedValue, val[0])
			i++
		}
		d.EqualValues(i, int96md.NumValues())

		// these two columns are always encrypted when we write them, so don't
		// try to read them during the plaintext test.
		if props.FileDecryptProps != nil {
			// Get column reader for the float column
			colReader, err = rowGroupReader.Column(4)
			if err != nil {
				panic(err)
			}
			floatReader := colReader.(*file.Float32ColumnChunkReader)

			floatmd, _ := rgMeta.ColumnChunk(4)

			i = 0
			for floatReader.HasNext() {
				var value [1]float32
				// read one value at a time. the number of rows read is returned. values
				// read contains the number of non-null rows
				rowsRead, valuesRead, _ = floatReader.ReadBatch(1, value[:], nil, nil)
				// ensure only 1 value is read
				d.EqualValues(1, rowsRead)
				// there are no null values
				d.EqualValues(1, valuesRead)
				// verify the value
				expectedValue := float32(i) * 1.1
				d.Equal(expectedValue, value[0])
				i++
			}
			d.EqualValues(i, floatmd.NumValues())

			// Get column reader for the double column
			colReader, err = rowGroupReader.Column(5)
			if err != nil {
				panic(err)
			}
			dblReader := colReader.(*file.Float64ColumnChunkReader)

			dblmd, _ := rgMeta.ColumnChunk(5)

			i = 0
			for dblReader.HasNext() {
				var value [1]float64
				// read one value at a time. the number of rows read is returned. values
				// read contains the number of non-null rows
				rowsRead, valuesRead, _ = dblReader.ReadBatch(1, value[:], nil, nil)
				// ensure only 1 value is read
				d.EqualValues(1, rowsRead)
				// there are no null values
				d.EqualValues(1, valuesRead)
				// verify the value
				expectedValue := float64(i) * 1.1111111
				d.Equal(expectedValue, value[0])
				i++
			}
			d.EqualValues(i, dblmd.NumValues())
		}

		colReader, err = rowGroupReader.Column(6)
		if err != nil {
			panic(err)
		}
		bareader := colReader.(*file.ByteArrayColumnChunkReader)

		bamd, _ := rgMeta.ColumnChunk(6)

		i = 0
		for bareader.HasNext() {
			var value [1]parquet.ByteArray
			var def [1]int16

			rowsRead, valuesRead, _ := bareader.ReadBatch(1, value[:], def[:], nil)
			d.EqualValues(1, rowsRead)
			expected := [10]byte{'p', 'a', 'r', 'q', 'u', 'e', 't', 0, 0, 0}
			expected[7] = byte('0') + byte(i/100)
			expected[8] = byte('0') + byte(i/10)%10
			expected[9] = byte('0') + byte(i%10)
			if i%2 == 0 {
				d.Equal(1, valuesRead)
				d.Len(value[0], 10)
				d.EqualValues(expected[:], value[0])
				d.EqualValues(1, def[0])
			} else {
				d.Zero(valuesRead)
				d.Zero(def[0])
			}
			i++
		}
		d.EqualValues(i, bamd.NumValues())
	}
}

func (d *TestDecryptionSuite) checkResults(fileName string, decryptionConfig, encryptionConfig uint) {
	decFn := func() { d.decryptFile(fileName, int(decryptionConfig-1)) }

	// Encryption configuration number 5 contains aad_prefix and disable_aad_prefix_storage
	// an exception is expected to be thrown if the file is not decrypted with aad_prefix
	if encryptionConfig == 5 {
		if decryptionConfig == 1 || decryptionConfig == 3 {
			d.Panics(decFn)
			return
		}
	}

	// decryption config number two contains aad_prefix. an exception
	// is expected to be thrown if the file was not encrypted with the same aad_prefix
	if decryptionConfig == 2 {
		if encryptionConfig != 5 && encryptionConfig != 4 {
			d.Panics(decFn)
			return
		}
	}

	// decryption config 4 can only work when the encryption config is 3
	if decryptionConfig == 4 && encryptionConfig != 3 {
		return
	}
	d.NotPanics(decFn)
}

// Read encrypted parquet file.
// the test reads two parquet files that were encrypted using the same encryption config
// one was generated in encryption_write_configurations_test.go tests and is deleted
// once the file is read and the second exists in parquet-testing/data folder
func (d *TestDecryptionSuite) TestDecryption() {
	tests := []struct {
		file   string
		config uint
	}{
		{"uniform_encryption.parquet.encrypted", 1},
		{"encrypt_columns_and_footer.parquet.encrypted", 2},
		{"encrypt_columns_plaintext_footer.parquet.encrypted", 3},
		{"encrypt_columns_and_footer_aad.parquet.encrypted", 4},
		{"encrypt_columns_and_footer_disable_aad_storage.parquet.encrypted", 5},
		{"encrypt_columns_and_footer_ctr.parquet.encrypted", 6},
	}
	for _, tt := range tests {
		d.Run(tt.file, func() {
			// decrypt file that was generated in encryption-write-tests
			tmpFile := path.Join(tempdir, "tmp_"+tt.file)
			d.Require().FileExists(tmpFile)

			// iterate over decryption configs and use each one to read the encrypted file
			for idx := range d.decryptionConfigs {
				decConfig := idx + 1
				d.checkResults(tmpFile, uint(decConfig), tt.config)
			}
			os.Remove(tmpFile)

			file := path.Join(getDataDir(), tt.file)
			d.Require().FileExists(file)

			for idx := range d.decryptionConfigs {
				decConfig := idx + 1
				d.Run(fmt.Sprintf("config %d", decConfig), func() {
					d.checkResults(file, uint(decConfig), tt.config)
				})
			}
		})
	}
}

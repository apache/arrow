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
	"testing"

	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/internal/encryption"
	"github.com/stretchr/testify/assert"
)

const (
	FooterEncryptionKey  = "0123456789012345"
	ColumnEncryptionKey1 = "1234567890123450"
	ColumnEncryptionKey2 = "1234567890123451"
	FileName             = "tester"
)

func TestColumnEncryptedWithOwnKey(t *testing.T) {
	t.Parallel()

	columnPath1 := "column_1"
	colprops1 := parquet.NewColumnEncryptionProperties(columnPath1,
		parquet.WithKey(ColumnEncryptionKey1), parquet.WithKeyID("kc1"))

	assert.Equal(t, columnPath1, colprops1.ColumnPath())
	assert.True(t, colprops1.IsEncrypted())
	assert.False(t, colprops1.IsEncryptedWithFooterKey())
	assert.Equal(t, ColumnEncryptionKey1, colprops1.Key())
	assert.Equal(t, "kc1", colprops1.KeyMetadata())
}

func TestColumnEncryptedWithFooterKey(t *testing.T) {
	t.Parallel()

	colPath1 := "column_1"
	colprops1 := parquet.NewColumnEncryptionProperties(colPath1)

	assert.Equal(t, colPath1, colprops1.ColumnPath())
	assert.True(t, colprops1.IsEncrypted())
	assert.True(t, colprops1.IsEncryptedWithFooterKey())
}

func TestUniformEncryption(t *testing.T) {
	t.Parallel()

	props := parquet.NewFileEncryptionProperties(FooterEncryptionKey, parquet.WithFooterKeyMetadata("kf"))

	assert.True(t, props.EncryptedFooter())
	assert.Equal(t, parquet.DefaultEncryptionAlgorithm, props.Algorithm().Algo)
	assert.Equal(t, FooterEncryptionKey, props.FooterKey())
	assert.Equal(t, "kf", props.FooterKeyMetadata())

	colPath := parquet.ColumnPathFromString("a_column")
	outColProps := props.ColumnEncryptionProperties(colPath.String())

	assert.True(t, outColProps.IsEncrypted())
	assert.True(t, outColProps.IsEncryptedWithFooterKey())
}

func TestEncryptFooterAndTwoColumns(t *testing.T) {
	t.Parallel()

	columnPath1 := parquet.ColumnPathFromString("column_1")
	columnPath2 := parquet.ColumnPathFromString("column_2")

	encryptedColumns := make(parquet.ColumnPathToEncryptionPropsMap)
	encryptedColumns[columnPath1.String()] = parquet.NewColumnEncryptionProperties(columnPath1.String(),
		parquet.WithKey(ColumnEncryptionKey1), parquet.WithKeyID("kc1"))
	encryptedColumns[columnPath2.String()] = parquet.NewColumnEncryptionProperties(columnPath2.String(),
		parquet.WithKey(ColumnEncryptionKey2), parquet.WithKeyID("kc2"))

	props := parquet.NewFileEncryptionProperties(FooterEncryptionKey,
		parquet.WithFooterKeyMetadata("kf"), parquet.WithEncryptedColumns(encryptedColumns))

	assert.True(t, props.EncryptedFooter())
	assert.Equal(t, parquet.DefaultEncryptionAlgorithm, props.Algorithm().Algo)
	assert.Equal(t, FooterEncryptionKey, props.FooterKey())

	outColProps1 := props.ColumnEncryptionProperties(columnPath1.String())
	assert.Equal(t, columnPath1.String(), outColProps1.ColumnPath())
	assert.True(t, outColProps1.IsEncrypted())
	assert.False(t, outColProps1.IsEncryptedWithFooterKey())
	assert.Equal(t, ColumnEncryptionKey1, outColProps1.Key())
	assert.Equal(t, "kc1", outColProps1.KeyMetadata())

	outColProps2 := props.ColumnEncryptionProperties(columnPath2.String())
	assert.Equal(t, columnPath2.String(), outColProps2.ColumnPath())
	assert.True(t, outColProps2.IsEncrypted())
	assert.False(t, outColProps2.IsEncryptedWithFooterKey())
	assert.Equal(t, ColumnEncryptionKey2, outColProps2.Key())
	assert.Equal(t, "kc2", outColProps2.KeyMetadata())

	columnPath3 := parquet.ColumnPathFromString("column_3")
	outColProps3 := props.ColumnEncryptionProperties(columnPath3.String())
	assert.Nil(t, outColProps3)
}

func TestEncryptTwoColumnsNotFooter(t *testing.T) {
	t.Parallel()

	columnPath1 := parquet.ColumnPathFromString("column_1")
	columnPath2 := parquet.ColumnPathFromString("column_2")

	encryptedColumns := make(parquet.ColumnPathToEncryptionPropsMap)
	encryptedColumns[columnPath1.String()] = parquet.NewColumnEncryptionProperties(columnPath1.String(),
		parquet.WithKey(ColumnEncryptionKey1), parquet.WithKeyID("kc1"))
	encryptedColumns[columnPath2.String()] = parquet.NewColumnEncryptionProperties(columnPath2.String(),
		parquet.WithKey(ColumnEncryptionKey2), parquet.WithKeyID("kc2"))

	props := parquet.NewFileEncryptionProperties(FooterEncryptionKey,
		parquet.WithFooterKeyMetadata("kf"), parquet.WithPlaintextFooter(), parquet.WithEncryptedColumns(encryptedColumns))

	assert.False(t, props.EncryptedFooter())
	assert.Equal(t, parquet.DefaultEncryptionAlgorithm, props.Algorithm().Algo)
	assert.Equal(t, FooterEncryptionKey, props.FooterKey())

	outColProps1 := props.ColumnEncryptionProperties(columnPath1.String())
	assert.Equal(t, columnPath1.String(), outColProps1.ColumnPath())
	assert.True(t, outColProps1.IsEncrypted())
	assert.False(t, outColProps1.IsEncryptedWithFooterKey())
	assert.Equal(t, ColumnEncryptionKey1, outColProps1.Key())
	assert.Equal(t, "kc1", outColProps1.KeyMetadata())

	outColProps2 := props.ColumnEncryptionProperties(columnPath2.String())
	assert.Equal(t, columnPath2.String(), outColProps2.ColumnPath())
	assert.True(t, outColProps2.IsEncrypted())
	assert.False(t, outColProps2.IsEncryptedWithFooterKey())
	assert.Equal(t, ColumnEncryptionKey2, outColProps2.Key())
	assert.Equal(t, "kc2", outColProps2.KeyMetadata())

	columnPath3 := "column_3"
	outColProps3 := props.ColumnEncryptionProperties(columnPath3)
	assert.Nil(t, outColProps3)
}

func TestUseAadPrefix(t *testing.T) {
	t.Parallel()

	props := parquet.NewFileEncryptionProperties(FooterEncryptionKey, parquet.WithAadPrefix(FileName))

	assert.Equal(t, FileName, string(props.Algorithm().Aad.AadPrefix))
	assert.False(t, props.Algorithm().Aad.SupplyAadPrefix)
}

func TestUseAadPrefixNotStoreInFile(t *testing.T) {
	t.Parallel()

	props := parquet.NewFileEncryptionProperties(FooterEncryptionKey,
		parquet.WithAadPrefix(FileName), parquet.DisableAadPrefixStorage())

	assert.Empty(t, props.Algorithm().Aad.AadPrefix)
	assert.True(t, props.Algorithm().Aad.SupplyAadPrefix)
}

func TestUseAES_GCM_CTR_V1Algo(t *testing.T) {
	t.Parallel()

	props := parquet.NewFileEncryptionProperties(FooterEncryptionKey,
		parquet.WithAlg(parquet.AesCtr))

	assert.Equal(t, parquet.AesCtr, props.Algorithm().Algo)
}

func TestUseKeyRetriever(t *testing.T) {
	t.Parallel()

	stringKr1 := make(encryption.StringKeyIDRetriever)
	stringKr1.PutKey("kf", FooterEncryptionKey)
	stringKr1.PutKey("kc1", ColumnEncryptionKey1)
	stringKr1.PutKey("kc2", ColumnEncryptionKey2)

	props := parquet.NewFileDecryptionProperties(parquet.WithKeyRetriever(stringKr1))
	assert.Equal(t, FooterEncryptionKey, props.KeyRetriever.GetKey([]byte("kf")))
	assert.Equal(t, ColumnEncryptionKey1, props.KeyRetriever.GetKey([]byte("kc1")))
	assert.Equal(t, ColumnEncryptionKey2, props.KeyRetriever.GetKey([]byte("kc2")))
}

func TestSupplyAadPrefix(t *testing.T) {
	props := parquet.NewFileDecryptionProperties(
		parquet.WithFooterKey(FooterEncryptionKey), parquet.WithDecryptAadPrefix(FileName))
	assert.Equal(t, FileName, props.AadPrefix())
}

func TestSetKey(t *testing.T) {
	columnPath1 := parquet.ColumnPathFromString("column_1")
	props := parquet.NewColumnDecryptionProperties(columnPath1.String(), parquet.WithDecryptKey(ColumnEncryptionKey1))
	assert.Equal(t, ColumnEncryptionKey1, props.Key())
}

func TestUsingExplicitFooterAndColumnKeys(t *testing.T) {
	colPath1 := "column_1"
	colPath2 := "column_2"
	decryptCols := make(parquet.ColumnPathToDecryptionPropsMap)
	decryptCols[colPath1] = parquet.NewColumnDecryptionProperties(colPath1, parquet.WithDecryptKey(ColumnEncryptionKey1))
	decryptCols[colPath2] = parquet.NewColumnDecryptionProperties(colPath2, parquet.WithDecryptKey(ColumnEncryptionKey2))

	props := parquet.NewFileDecryptionProperties(parquet.WithFooterKey(FooterEncryptionKey), parquet.WithColumnKeys(decryptCols))
	assert.Equal(t, FooterEncryptionKey, props.FooterKey())
	assert.Equal(t, ColumnEncryptionKey1, props.ColumnKey(colPath1))
	assert.Equal(t, ColumnEncryptionKey2, props.ColumnKey(colPath2))
}

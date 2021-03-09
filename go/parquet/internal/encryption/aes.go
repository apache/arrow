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

package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"io"

	"github.com/apache/arrow/go/parquet"
	"golang.org/x/xerrors"
)

const (
	GcmTagLength = 16
	NonceLength  = 12

	kGcmMode          = 0
	kCtrMode          = 1
	kCtrIVLen         = 16
	kBufferSizeLength = 4
)

const (
	FooterModule int8 = iota
	ColumnMetaModule
	DataPageModule
	DictPageModule
	DataPageHeaderModule
	DictPageHeaderModule
	ColumnIndexModule
	OffsetIndexModule
)

type aesEncryptor struct {
	mode                int
	ciphertextSizeDelta int
}

func NewAesEncryptor(alg parquet.Cipher, metadata bool) *aesEncryptor {
	ret := &aesEncryptor{}
	ret.ciphertextSizeDelta = kBufferSizeLength + NonceLength
	if metadata || alg == parquet.AesGcm {
		ret.mode = kGcmMode
		ret.ciphertextSizeDelta += GcmTagLength
	} else {
		ret.mode = kCtrMode
	}

	return ret
}

func (a *aesEncryptor) CiphertextSizeDelta() int { return a.ciphertextSizeDelta }

func (a *aesEncryptor) SignedFooterEncrypt(w io.Writer, footer, key, aad, nonce []byte) int {
	if a.mode != kGcmMode {
		panic("must use AES GCM (metadata) encryptor")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	if aead.NonceSize() != NonceLength {
		panic(xerrors.Errorf("nonce size mismatch %d, %d", aead.NonceSize(), NonceLength))
	}
	if aead.Overhead() != GcmTagLength {
		panic(xerrors.Errorf("tagsize mismatch %d %d", aead.Overhead(), GcmTagLength))
	}

	ciphertext := aead.Seal(nil, nonce, footer, aad)
	bufferSize := uint32(len(ciphertext) + len(nonce))
	if err := binary.Write(w, binary.LittleEndian, bufferSize); err != nil {
		panic(err)
	}
	w.Write(nonce)
	w.Write(ciphertext)
	return kBufferSizeLength + int(bufferSize)
}

func (a *aesEncryptor) Encrypt(w io.Writer, src, key, aad []byte) int {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}

	nonce := make([]byte, NonceLength)
	rand.Read(nonce)

	if a.mode == kGcmMode {
		aead, err := cipher.NewGCM(block)
		if err != nil {
			panic(err)
		}
		if aead.NonceSize() != NonceLength {
			panic(xerrors.Errorf("nonce size mismatch %d, %d", aead.NonceSize(), NonceLength))
		}
		if aead.Overhead() != GcmTagLength {
			panic(xerrors.Errorf("tagsize mismatch %d %d", aead.Overhead(), GcmTagLength))
		}

		ciphertext := aead.Seal(nil, nonce, src, aad)
		bufferSize := len(ciphertext) + len(nonce)
		if err := binary.Write(w, binary.LittleEndian, uint32(bufferSize)); err != nil {
			panic(err)
		}
		w.Write(nonce)
		w.Write(ciphertext)
		return kBufferSizeLength + bufferSize
	}

	// Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial
	// counter field.
	// The first 31 bits of the initial counter field are set to 0, the last bit
	// is set to 1.
	iv := make([]byte, kCtrIVLen)
	copy(iv, nonce)
	iv[kCtrIVLen-1] = 1

	bufferSize := NonceLength + len(src)
	if err := binary.Write(w, binary.LittleEndian, uint32(bufferSize)); err != nil {
		panic(err)
	}
	w.Write(nonce)
	cipher.StreamWriter{S: cipher.NewCTR(block, iv), W: w}.Write(src)
	return kBufferSizeLength + bufferSize
}

type aesDecryptor struct {
	mode                int
	ciphertextSizeDelta int
}

func newAesDecryptor(alg parquet.Cipher, metadata bool) *aesDecryptor {
	ret := &aesDecryptor{}
	ret.ciphertextSizeDelta = kBufferSizeLength + NonceLength
	if metadata || alg == parquet.AesGcm {
		ret.mode = kGcmMode
		ret.ciphertextSizeDelta += GcmTagLength
	} else {
		ret.mode = kCtrMode
	}

	return ret
}

func (a *aesDecryptor) CiphertextSizeDelta() int { return a.ciphertextSizeDelta }

func (a *aesDecryptor) Decrypt(cipherText, key, aad []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}

	writtenCiphertextLen := binary.LittleEndian.Uint32(cipherText)
	cipherLen := writtenCiphertextLen + kBufferSizeLength
	nonce := cipherText[kBufferSizeLength : kBufferSizeLength+NonceLength]

	if a.mode == kGcmMode {
		aead, err := cipher.NewGCM(block)
		if err != nil {
			panic(err)
		}

		plain, err := aead.Open(nil, nonce, cipherText[kBufferSizeLength+NonceLength:cipherLen], aad)
		if err != nil {
			panic(err)
		}
		return plain
	}

	// Parquet CTR IVs are comprised of a 12-byte nonce and a 4-byte initial
	// counter field.
	// The first 31 bits of the initial counter field are set to 0, the last bit
	// is set to 1.
	iv := make([]byte, kCtrIVLen)
	copy(iv, nonce)
	iv[kCtrIVLen-1] = 1

	stream := cipher.NewCTR(block, iv)
	dst := make([]byte, len(cipherText)-kBufferSizeLength-NonceLength)
	stream.XORKeyStream(dst, cipherText[kBufferSizeLength+NonceLength:])
	return dst
}

func CreateModuleAad(fileAad string, moduleType int8, rowGroupOrdinal, columnOrdinal, pageOrdinal int16) string {
	buf := bytes.NewBuffer([]byte(fileAad))
	buf.WriteByte(byte(moduleType))

	if moduleType == FooterModule {
		return buf.String()
	}

	binary.Write(buf, binary.LittleEndian, rowGroupOrdinal)
	binary.Write(buf, binary.LittleEndian, columnOrdinal)
	if DataPageModule != moduleType && DataPageHeaderModule != moduleType {
		return buf.String()
	}

	binary.Write(buf, binary.LittleEndian, pageOrdinal)
	return buf.String()
}

func CreateFooterAad(aadPrefix string) string {
	return CreateModuleAad(aadPrefix, FooterModule, -1, -1, -1)
}

func QuickUpdatePageAad(aad []byte, newPageOrdinal int16) {
	binary.LittleEndian.PutUint16(aad[len(aad)-2:], uint16(newPageOrdinal))
}

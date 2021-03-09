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

package thrift

import (
	"bytes"
	"context"
	"io"

	"github.com/apache/arrow/go/parquet/internal/encryption"
	"github.com/apache/thrift/lib/go/thrift"
)

var protocolFactory = thrift.NewTCompactProtocolFactory()

func DeserializeThrift(msg thrift.TStruct, buf []byte) (remain uint64, err error) {
	tbuf := &thrift.TMemoryBuffer{Buffer: bytes.NewBuffer(buf)}
	err = msg.Read(context.TODO(), protocolFactory.GetProtocol(tbuf))
	remain = tbuf.RemainingBytes()
	return
}

func SerializeThriftStream(msg thrift.TStruct, w io.Writer) error {
	return msg.Write(context.TODO(), protocolFactory.GetProtocol(thrift.NewStreamTransportW(w)))
}

func DeserializeThriftStream(msg thrift.TStruct, r io.Reader) error {
	return msg.Read(context.TODO(), protocolFactory.GetProtocol(thrift.NewStreamTransportR(r)))
}

type ThriftSerializer struct {
	thrift.TSerializer
}

func NewThriftSerializer() *ThriftSerializer {
	tbuf := thrift.NewTMemoryBufferLen(1024)
	return &ThriftSerializer{thrift.TSerializer{
		Transport: tbuf,
		Protocol:  protocolFactory.GetProtocol(tbuf),
	}}
}

func (t *ThriftSerializer) Serialize(msg thrift.TStruct, w io.Writer, enc encryption.Encryptor) (int, error) {
	b, err := t.Write(context.Background(), msg)
	if err != nil {
		return 0, err
	}

	if enc == nil {
		return w.Write(b)
	}

	var cipherBuf bytes.Buffer
	cipherBuf.Grow(enc.CiphertextSizeDelta() + len(b))
	enc.Encrypt(&cipherBuf, b)
	n, err := cipherBuf.WriteTo(w)
	return int(n), err
}

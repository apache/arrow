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

package arrow

type EncodedType interface {
	DataType
	Encoded() DataType
}

// RunLengthEncodedType is the datatype to represent a run-length encoded
// array of data.
type RunLengthEncodedType struct {
	enc DataType
}

func RunLengthEncodedOf(dt DataType) *RunLengthEncodedType {
	return &RunLengthEncodedType{enc: dt}
}

func (*RunLengthEncodedType) ID() Type     { return RUN_LENGTH_ENCODED }
func (*RunLengthEncodedType) Name() string { return "run_length_encoded" }
func (*RunLengthEncodedType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecAlwaysNull()}}
}

func (t *RunLengthEncodedType) String() string {
	return t.Name() + "<" + t.enc.String() + ">"
}

func (t *RunLengthEncodedType) Fingerprint() string {
	return typeFingerprint(t) + "{" + t.enc.Fingerprint() + "}"
}

func (t *RunLengthEncodedType) Encoded() DataType { return t.enc }

func (t *RunLengthEncodedType) Fields() []Field {
	return []Field{
		{Name: "run_ends", Type: PrimitiveTypes.Int32},
		{Name: "values", Type: t.enc, Nullable: true},
	}
}

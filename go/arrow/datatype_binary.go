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

type BinaryType struct{}

func (t *BinaryType) ID() Type            { return BINARY }
func (t *BinaryType) Name() string        { return "binary" }
func (t *BinaryType) String() string      { return "binary" }
func (t *BinaryType) binary()             {}
func (t *BinaryType) Fingerprint() string { return typeFingerprint(t) }
func (t *BinaryType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(),
		SpecFixedWidth(Int32SizeBytes), SpecVariableWidth()}}
}

type StringType struct{}

func (t *StringType) ID() Type            { return STRING }
func (t *StringType) Name() string        { return "utf8" }
func (t *StringType) String() string      { return "utf8" }
func (t *StringType) binary()             {}
func (t *StringType) Fingerprint() string { return typeFingerprint(t) }
func (t *StringType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(),
		SpecFixedWidth(Int32SizeBytes), SpecVariableWidth()}}
}

type LargeBinaryType struct{}

func (t *LargeBinaryType) ID() Type            { return LARGE_BINARY }
func (t *LargeBinaryType) Name() string        { return "large_binary" }
func (t *LargeBinaryType) String() string      { return "large_binary" }
func (t *LargeBinaryType) binary()             {}
func (t *LargeBinaryType) Fingerprint() string { return typeFingerprint(t) }
func (t *LargeBinaryType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(),
		SpecFixedWidth(Int64SizeBytes), SpecVariableWidth()}}
}

type LargeStringType struct{}

func (t *LargeStringType) ID() Type            { return LARGE_STRING }
func (t *LargeStringType) Name() string        { return "large_utf8" }
func (t *LargeStringType) String() string      { return "large_utf8" }
func (t *LargeStringType) binary()             {}
func (t *LargeStringType) Fingerprint() string { return typeFingerprint(t) }
func (t *LargeStringType) Layout() DataTypeLayout {
	return DataTypeLayout{Buffers: []BufferSpec{SpecBitmap(),
		SpecFixedWidth(Int64SizeBytes), SpecVariableWidth()}}
}

var (
	BinaryTypes = struct {
		Binary      BinaryDataType
		String      BinaryDataType
		LargeBinary BinaryDataType
		LargeString BinaryDataType
	}{
		Binary:      &BinaryType{},
		String:      &StringType{},
		LargeBinary: &LargeBinaryType{},
		LargeString: &LargeStringType{},
	}
)

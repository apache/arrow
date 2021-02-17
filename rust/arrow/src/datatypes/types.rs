// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::{ArrowNativeType, DataType};

/// A subtype of native types that represents legal dictionary keys.
/// See <https://arrow.apache.org/docs/format/Columnar.html>
pub unsafe trait ArrowDictionaryKeyType: ArrowNativeType {
    const DATA_TYPE: DataType;
}

unsafe impl ArrowDictionaryKeyType for i8 {
    const DATA_TYPE: DataType = DataType::Int8;
}
unsafe impl ArrowDictionaryKeyType for i16 {
    const DATA_TYPE: DataType = DataType::Int16;
}
unsafe impl ArrowDictionaryKeyType for i32 {
    const DATA_TYPE: DataType = DataType::Int32;
}
unsafe impl ArrowDictionaryKeyType for i64 {
    const DATA_TYPE: DataType = DataType::Int64;
}
unsafe impl ArrowDictionaryKeyType for u8 {
    const DATA_TYPE: DataType = DataType::UInt8;
}
unsafe impl ArrowDictionaryKeyType for u16 {
    const DATA_TYPE: DataType = DataType::UInt16;
}
unsafe impl ArrowDictionaryKeyType for u32 {
    const DATA_TYPE: DataType = DataType::UInt32;
}
unsafe impl ArrowDictionaryKeyType for u64 {
    const DATA_TYPE: DataType = DataType::UInt64;
}

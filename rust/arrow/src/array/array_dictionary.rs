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

use std::fmt;
use std::iter::IntoIterator;
use std::mem;
use std::{any::Any, sync::Arc};
use std::{convert::From, iter::FromIterator};

use super::{
    make_array, Array, ArrayData, ArrayDataRef, ArrayRef, PrimitiveArray,
    PrimitiveBuilder, StringArray, StringBuilder, StringDictionaryBuilder,
};
use crate::datatypes::ArrowNativeType;
use crate::datatypes::{ArrowDictionaryKeyType, ArrowPrimitiveType, DataType};

/// A dictionary array where each element is a single value indexed by an integer key.
/// This is mostly used to represent strings or a limited set of primitive types as integers,
/// for example when doing NLP analysis or representing chromosomes by name.
///
/// Example **with nullable** data:
///
/// ```
/// use arrow::array::{DictionaryArray, Int8Array};
/// use arrow::datatypes::Int8Type;
/// let test = vec!["a", "a", "b", "c"];
/// let array : DictionaryArray<Int8Type> = test.iter().map(|&x| if x == "b" {None} else {Some(x)}).collect();
/// assert_eq!(array.keys(), &Int8Array::from(vec![Some(0), Some(0), None, Some(1)]));
/// ```
///
/// Example **without nullable** data:
///
/// ```
/// use arrow::array::{DictionaryArray, Int8Array};
/// use arrow::datatypes::Int8Type;
/// let test = vec!["a", "a", "b", "c"];
/// let array : DictionaryArray<Int8Type> = test.into_iter().collect();
/// assert_eq!(array.keys(), &Int8Array::from(vec![0, 0, 1, 2]));
/// ```
pub struct DictionaryArray<K: ArrowPrimitiveType> {
    /// Data of this dictionary. Note that this is _not_ compatible with the C Data interface,
    /// as, in the current implementation, `values` below are the first child of this struct.
    data: ArrayDataRef,

    /// The keys of this dictionary. These are constructed from the buffer and null bitmap
    /// of `data`.
    /// Also, note that these do not correspond to the true values of this array. Rather, they map
    /// to the real values.
    keys: PrimitiveArray<K>,

    /// Array of dictionary values (can by any DataType).
    values: ArrayRef,

    /// Values are ordered.
    is_ordered: bool,
}

impl<'a, K: ArrowPrimitiveType> DictionaryArray<K> {
    /// Return an iterator to the keys of this dictionary.
    pub fn keys(&self) -> &PrimitiveArray<K> {
        &self.keys
    }

    /// Returns an array view of the keys of this dictionary
    pub fn keys_array(&self) -> PrimitiveArray<K> {
        let data = self.data_ref();
        let keys_data = ArrayData::new(
            K::DATA_TYPE,
            data.len(),
            Some(data.null_count()),
            data.null_buffer().cloned(),
            data.offset(),
            data.buffers().to_vec(),
            vec![],
        );
        PrimitiveArray::<K>::from(Arc::new(keys_data))
    }

    /// Returns the lookup key by doing reverse dictionary lookup
    pub fn lookup_key(&self, value: &str) -> Option<K::Native> {
        let rd_buf: &StringArray =
            self.values.as_any().downcast_ref::<StringArray>().unwrap();

        (0..rd_buf.len())
            .position(|i| rd_buf.value(i) == value)
            .map(K::Native::from_usize)
            .flatten()
    }

    /// Returns an `ArrayRef` to the dictionary values.
    pub fn values(&self) -> ArrayRef {
        self.values.clone()
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_ref().data_type().clone()
    }

    /// The length of the dictionary is the length of the keys array.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether this dictionary is empty
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    // Currently exists for compatibility purposes with Arrow IPC.
    pub fn is_ordered(&self) -> bool {
        self.is_ordered
    }
}

/// Constructs a `DictionaryArray` from an array data reference.
impl<T: ArrowPrimitiveType> From<ArrayDataRef> for DictionaryArray<T> {
    fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "DictionaryArray data should contain a single buffer only (keys)."
        );
        assert_eq!(
            data.child_data().len(),
            1,
            "DictionaryArray should contain a single child array (values)."
        );

        if let DataType::Dictionary(key_data_type, _) = data.data_type() {
            if key_data_type.as_ref() != &T::DATA_TYPE {
                panic!("DictionaryArray's data type must match.")
            };
            // create a zero-copy of the keys' data
            let keys = PrimitiveArray::<T>::from(Arc::new(ArrayData::new(
                T::DATA_TYPE,
                data.len(),
                Some(data.null_count()),
                data.null_buffer().cloned(),
                data.offset(),
                data.buffers().to_vec(),
                vec![],
            )));
            let values = make_array(data.child_data()[0].clone());
            Self {
                data,
                keys,
                values,
                is_ordered: false,
            }
        } else {
            panic!("DictionaryArray must have Dictionary data type.")
        }
    }
}

/// Constructs a `DictionaryArray` from an iterator of optional strings.
impl<'a, T: ArrowPrimitiveType + ArrowDictionaryKeyType> FromIterator<Option<&'a str>>
    for DictionaryArray<T>
{
    fn from_iter<I: IntoIterator<Item = Option<&'a str>>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let key_builder = PrimitiveBuilder::<T>::new(lower);
        let value_builder = StringBuilder::new(256);
        let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
        it.for_each(|i| {
            if let Some(i) = i {
                // Note: impl ... for Result<DictionaryArray<T>> fails with
                // error[E0117]: only traits defined in the current crate can be implemented for arbitrary types
                builder
                    .append(i)
                    .expect("Unable to append a value to a dictionary array.");
            } else {
                builder
                    .append_null()
                    .expect("Unable to append a null value to a dictionary array.");
            }
        });

        builder.finish()
    }
}

/// Constructs a `DictionaryArray` from an iterator of strings.
impl<'a, T: ArrowPrimitiveType + ArrowDictionaryKeyType> FromIterator<&'a str>
    for DictionaryArray<T>
{
    fn from_iter<I: IntoIterator<Item = &'a str>>(iter: I) -> Self {
        let it = iter.into_iter();
        let (lower, _) = it.size_hint();
        let key_builder = PrimitiveBuilder::<T>::new(lower);
        let value_builder = StringBuilder::new(256);
        let mut builder = StringDictionaryBuilder::new(key_builder, value_builder);
        it.for_each(|i| {
            builder
                .append(i)
                .expect("Unable to append a value to a dictionary array.");
        });

        builder.finish()
    }
}

impl<T: ArrowPrimitiveType> Array for DictionaryArray<T> {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }

    fn get_buffer_memory_size(&self) -> usize {
        // Since both `keys` and `values` derive (are references from) `data`, we only need to account for `data`.
        self.data.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size()
            + self.keys.get_array_memory_size()
            + self.values.get_array_memory_size()
            + mem::size_of_val(self)
    }
}

impl<T: ArrowPrimitiveType> fmt::Debug for DictionaryArray<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "DictionaryArray {{keys: {:?} values: {:?}}}",
            self.keys, self.values
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        array::Int16Array,
        datatypes::{Int32Type, Int8Type, UInt32Type, UInt8Type},
    };
    use crate::{
        array::Int16DictionaryArray, array::PrimitiveDictionaryBuilder,
        datatypes::DataType,
    };
    use crate::{buffer::Buffer, datatypes::ToByteSlice};

    #[test]
    fn test_dictionary_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int8)
            .len(8)
            .add_buffer(Buffer::from(
                &[10_i8, 11, 12, 13, 14, 15, 16, 17].to_byte_slice(),
            ))
            .build();

        // Construct a buffer for value offsets, for the nested array:
        let keys = Buffer::from(&[2_i16, 3, 4].to_byte_slice());

        // Construct a dictionary array from the above two
        let key_type = DataType::Int16;
        let value_type = DataType::Int8;
        let dict_data_type =
            DataType::Dictionary(Box::new(key_type), Box::new(value_type));
        let dict_data = ArrayData::builder(dict_data_type.clone())
            .len(3)
            .add_buffer(keys.clone())
            .add_child_data(value_data.clone())
            .build();
        let dict_array = Int16DictionaryArray::from(dict_data);

        let values = dict_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int8, dict_array.value_type());
        assert_eq!(3, dict_array.len());

        // Null count only makes sense in terms of the component arrays.
        assert_eq!(0, dict_array.null_count());
        assert_eq!(0, dict_array.values().null_count());
        assert_eq!(dict_array.keys(), &Int16Array::from(vec![2_i16, 3, 4]));

        // Now test with a non-zero offset
        let dict_data = ArrayData::builder(dict_data_type)
            .len(2)
            .offset(1)
            .add_buffer(keys)
            .add_child_data(value_data.clone())
            .build();
        let dict_array = Int16DictionaryArray::from(dict_data);

        let values = dict_array.values();
        assert_eq!(value_data, values.data());
        assert_eq!(DataType::Int8, dict_array.value_type());
        assert_eq!(2, dict_array.len());
        assert_eq!(dict_array.keys(), &Int16Array::from(vec![3_i16, 4]));
    }

    #[test]
    fn test_dictionary_array_fmt_debug() {
        let key_builder = PrimitiveBuilder::<UInt8Type>::new(3);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        builder.append(12345678).unwrap();
        builder.append_null().unwrap();
        builder.append(22345678).unwrap();
        let array = builder.finish();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<UInt8>\n[\n  0,\n  null,\n  1,\n] values: PrimitiveArray<UInt32>\n[\n  12345678,\n  22345678,\n]}\n",
            format!("{:?}", array)
        );

        let key_builder = PrimitiveBuilder::<UInt8Type>::new(20);
        let value_builder = PrimitiveBuilder::<UInt32Type>::new(2);
        let mut builder = PrimitiveDictionaryBuilder::new(key_builder, value_builder);
        for _ in 0..20 {
            builder.append(1).unwrap();
        }
        let array = builder.finish();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<UInt8>\n[\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n  0,\n] values: PrimitiveArray<UInt32>\n[\n  1,\n]}\n",
            format!("{:?}", array)
        );
    }

    #[test]
    fn test_dictionary_array_from_iter() {
        let test = vec!["a", "a", "b", "c"];
        let array: DictionaryArray<Int8Type> = test
            .iter()
            .map(|&x| if x == "b" { None } else { Some(x) })
            .collect();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<Int8>\n[\n  0,\n  0,\n  null,\n  1,\n] values: StringArray\n[\n  \"a\",\n  \"c\",\n]}\n",
            format!("{:?}", array)
        );

        let array: DictionaryArray<Int8Type> = test.into_iter().collect();
        assert_eq!(
            "DictionaryArray {keys: PrimitiveArray<Int8>\n[\n  0,\n  0,\n  1,\n  2,\n] values: StringArray\n[\n  \"a\",\n  \"b\",\n  \"c\",\n]}\n",
            format!("{:?}", array)
        );
    }

    #[test]
    fn test_dictionary_array_reverse_lookup_key() {
        let test = vec!["a", "a", "b", "c"];
        let array: DictionaryArray<Int8Type> = test.into_iter().collect();

        assert_eq!(array.lookup_key("c"), Some(2));

        // Direction of building a dictionary is the iterator direction
        let test = vec!["t3", "t3", "t2", "t2", "t1", "t3", "t4", "t1", "t0"];
        let array: DictionaryArray<Int8Type> = test.into_iter().collect();

        assert_eq!(array.lookup_key("t1"), Some(2));
        assert_eq!(array.lookup_key("non-existent"), None);
    }

    #[test]
    fn test_dictionary_keys_as_primitive_array() {
        let test = vec!["a", "b", "c", "a"];
        let array: DictionaryArray<Int8Type> = test.into_iter().collect();

        let keys = array.keys_array();
        assert_eq!(&DataType::Int8, keys.data_type());
        assert_eq!(0, keys.null_count());
        assert_eq!(&[0, 1, 2, 0], keys.values());
    }

    #[test]
    fn test_dictionary_keys_as_primitive_array_with_null() {
        let test = vec![Some("a"), None, Some("b"), None, None, Some("a")];
        let array: DictionaryArray<Int32Type> = test.into_iter().collect();

        let keys = array.keys_array();
        assert_eq!(&DataType::Int32, keys.data_type());
        assert_eq!(3, keys.null_count());

        assert_eq!(true, keys.is_valid(0));
        assert_eq!(false, keys.is_valid(1));
        assert_eq!(true, keys.is_valid(2));
        assert_eq!(false, keys.is_valid(3));
        assert_eq!(false, keys.is_valid(4));
        assert_eq!(true, keys.is_valid(5));

        assert_eq!(0, keys.value(0));
        assert_eq!(1, keys.value(2));
        assert_eq!(0, keys.value(5));
    }
}

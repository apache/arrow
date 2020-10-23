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

//! Defines sort kernel for `ArrayRef`

use std::cmp::{Ordering, Reverse};

use crate::array::*;
use crate::compute::take;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

use crate::buffer::MutableBuffer;
use num::ToPrimitive;
use std::sync::Arc;
use TimeUnit::*;

/// Sort the `ArrayRef` using `SortOptions`.
///
/// Performs a stable sort on values and indices. Nulls are ordered according to the `nulls_first` flag in `options`.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value.
///
/// Returns an `ArrowError::ComputeError(String)` if the array type is either unsupported by `sort_to_indices` or `take`.
///
pub fn sort(values: &ArrayRef, options: Option<SortOptions>) -> Result<ArrayRef> {
    let indices = sort_to_indices(values, options)?;
    take(values, &indices, None)
}

/// Sort elements from `ArrayRef` into an unsigned integer (`UInt32Array`) of indices.
/// For floating point arrays any NaN values are considered to be greater than any other non-null value
pub fn sort_to_indices(
    values: &ArrayRef,
    options: Option<SortOptions>,
) -> Result<UInt32Array> {
    let options = options.unwrap_or_default();
    let indices = 0..(values.len().to_u32().ok_or_else(|| {
        ArrowError::ComputeError(
            "Sorting currently only supports u32 indices".to_string(),
        )
    })?);
    // partition indices into valid (`v`) and null (`n` indices
    // for floating point types the valid indices are further partioned into non-NaN and NaN
    let (v, n, nan): (Vec<u32>, Vec<u32>, Vec<u32>) =
        if values.data_type() == &DataType::Float32 {
            let array = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("Unable to downcast array");
            let (v, n): (Vec<u32>, Vec<u32>) =
                indices.partition(|index| array.is_valid(*index as usize));
            let has_nan = v.iter().any(|index| array.value(*index as usize).is_nan());
            let (v, nan) = if has_nan {
                v.into_iter()
                    .partition(|index| !array.value(*index as usize).is_nan())
            } else {
                (v, vec![])
            };
            (v, n, nan)
        } else if values.data_type() == &DataType::Float64 {
            let array = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Unable to downcast array");
            let (v, n): (Vec<u32>, Vec<u32>) =
                indices.partition(|index| array.is_valid(*index as usize));
            let has_nan = v.iter().any(|index| array.value(*index as usize).is_nan());
            let (v, nan) = if has_nan {
                v.into_iter()
                    .partition(|index| !array.value(*index as usize).is_nan())
            } else {
                (v, vec![])
            };
            (v, n, nan)
        } else {
            let (v, n) = indices.partition(|index| values.is_valid(*index as usize));
            (v, n, vec![])
        };
    match values.data_type() {
        DataType::Boolean => sort_primitive::<BooleanType>(values, v, n, nan, &options),
        DataType::Int8 => sort_primitive::<Int8Type>(values, v, n, nan, &options),
        DataType::Int16 => sort_primitive::<Int16Type>(values, v, n, nan, &options),
        DataType::Int32 => sort_primitive::<Int32Type>(values, v, n, nan, &options),
        DataType::Int64 => sort_primitive::<Int64Type>(values, v, n, nan, &options),
        DataType::UInt8 => sort_primitive::<UInt8Type>(values, v, n, nan, &options),
        DataType::UInt16 => sort_primitive::<UInt16Type>(values, v, n, nan, &options),
        DataType::UInt32 => sort_primitive::<UInt32Type>(values, v, n, nan, &options),
        DataType::UInt64 => sort_primitive::<UInt64Type>(values, v, n, nan, &options),
        DataType::Float32 => sort_primitive::<Float32Type>(values, v, n, nan, &options),
        DataType::Float64 => sort_primitive::<Float64Type>(values, v, n, nan, &options),
        DataType::Date32(_) => sort_primitive::<Date32Type>(values, v, n, nan, &options),
        DataType::Date64(_) => sort_primitive::<Date64Type>(values, v, n, nan, &options),
        DataType::Time32(Second) => {
            sort_primitive::<Time32SecondType>(values, v, n, nan, &options)
        }
        DataType::Time32(Millisecond) => {
            sort_primitive::<Time32MillisecondType>(values, v, n, nan, &options)
        }
        DataType::Time64(Microsecond) => {
            sort_primitive::<Time64MicrosecondType>(values, v, n, nan, &options)
        }
        DataType::Time64(Nanosecond) => {
            sort_primitive::<Time64NanosecondType>(values, v, n, nan, &options)
        }
        DataType::Timestamp(Second, _) => {
            sort_primitive::<TimestampSecondType>(values, v, n, nan, &options)
        }
        DataType::Timestamp(Millisecond, _) => {
            sort_primitive::<TimestampMillisecondType>(values, v, n, nan, &options)
        }
        DataType::Timestamp(Microsecond, _) => {
            sort_primitive::<TimestampMicrosecondType>(values, v, n, nan, &options)
        }
        DataType::Timestamp(Nanosecond, _) => {
            sort_primitive::<TimestampNanosecondType>(values, v, n, nan, &options)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            sort_primitive::<IntervalYearMonthType>(values, v, n, nan, &options)
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            sort_primitive::<IntervalDayTimeType>(values, v, n, nan, &options)
        }
        DataType::Duration(TimeUnit::Second) => {
            sort_primitive::<DurationSecondType>(values, v, n, nan, &options)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            sort_primitive::<DurationMillisecondType>(values, v, n, nan, &options)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            sort_primitive::<DurationMicrosecondType>(values, v, n, nan, &options)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            sort_primitive::<DurationNanosecondType>(values, v, n, nan, &options)
        }
        DataType::Utf8 => sort_string(values, v, n, &options),
        DataType::Dictionary(key_type, value_type)
            if *value_type.as_ref() == DataType::Utf8 =>
        {
            match key_type.as_ref() {
                DataType::Int8 => {
                    sort_string_dictionary::<Int8Type>(values, v, n, &options)
                }
                DataType::Int16 => {
                    sort_string_dictionary::<Int16Type>(values, v, n, &options)
                }
                DataType::Int32 => {
                    sort_string_dictionary::<Int32Type>(values, v, n, &options)
                }
                DataType::Int64 => {
                    sort_string_dictionary::<Int64Type>(values, v, n, &options)
                }
                DataType::UInt8 => {
                    sort_string_dictionary::<UInt8Type>(values, v, n, &options)
                }
                DataType::UInt16 => {
                    sort_string_dictionary::<UInt16Type>(values, v, n, &options)
                }
                DataType::UInt32 => {
                    sort_string_dictionary::<UInt32Type>(values, v, n, &options)
                }
                DataType::UInt64 => {
                    sort_string_dictionary::<UInt64Type>(values, v, n, &options)
                }
                t => Err(ArrowError::ComputeError(format!(
                    "Sort not supported for dictionary key type {:?}",
                    t
                ))),
            }
        }
        t => Err(ArrowError::ComputeError(format!(
            "Sort not supported for data type {:?}",
            t
        ))),
    }
}

/// Options that define how sort kernels should behave
#[derive(Clone, Copy, Debug)]
pub struct SortOptions {
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl Default for SortOptions {
    fn default() -> Self {
        Self {
            descending: false,
            // default to nulls first to match spark's behavior
            nulls_first: true,
        }
    }
}

/// Sort primitive values
fn sort_primitive<T>(
    values: &ArrayRef,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    nan_indices: Vec<u32>,
    options: &SortOptions,
) -> Result<UInt32Array>
where
    T: ArrowPrimitiveType,
    T::Native: std::cmp::PartialOrd,
{
    let values = as_primitive_array::<T>(values);
    let descending = options.descending;

    // create tuples that are used for sorting
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, values.value(index as usize)))
        .collect::<Vec<(u32, T::Native)>>();

    let mut nulls = null_indices;
    let mut nans = nan_indices;

    let valids_len = valids.len();
    let nulls_len = nulls.len();
    let nans_len = nans.len();

    if !descending {
        valids.sort_by(|a, b| a.1.partial_cmp(&b.1).expect("unexpected NaN"));
    } else {
        valids.sort_by(|a, b| a.1.partial_cmp(&b.1).expect("unexpected NaN").reverse());
        // reverse to keep a stable ordering
        nans.reverse();
        nulls.reverse();
    }

    // collect results directly into a buffer instead of a vec to avoid another aligned allocation
    let mut result = MutableBuffer::new(values.len() * std::mem::size_of::<u32>());
    // sets len to capacity so we can access the whole buffer as a typed slice
    result.resize(values.len() * std::mem::size_of::<u32>())?;
    let result_slice: &mut [u32] = result.typed_data_mut();

    debug_assert_eq!(result_slice.len(), nulls_len + nans_len + valids_len);

    if options.nulls_first {
        result_slice[0..nulls_len].copy_from_slice(&nulls);
        insert_valid_and_nan_values(result_slice, nulls_len, valids, nans, descending);
    } else {
        // nulls last
        insert_valid_and_nan_values(result_slice, 0, valids, nans, descending);
        result_slice[valids_len + nans_len..].copy_from_slice(nulls.as_slice())
    }

    let result_data = Arc::new(ArrayData::new(
        DataType::UInt32,
        values.len(),
        Some(0),
        None,
        0,
        vec![result.freeze()],
        vec![],
    ));

    Ok(UInt32Array::from(result_data))
}

// insert valid and nan values in the correct order depending on the descending flag
fn insert_valid_and_nan_values<T: ArrowNativeType>(
    result_slice: &mut [u32],
    offset: usize,
    valids: Vec<(u32, T)>,
    nans: Vec<u32>,
    descending: bool,
) {
    let valids_len = valids.len();
    let nans_len = nans.len();

    // helper to append the index part of the valid tuples
    let append_valids = move |dst_slice: &mut [u32]| {
        debug_assert_eq!(dst_slice.len(), valids_len);
        dst_slice
            .iter_mut()
            .zip(valids.into_iter())
            .for_each(|(dst, src)| *dst = src.0)
    };

    // NaNs are considered greater than all number which means
    // for descending order they come before valid numbers
    // for ascending order they come after valid numbers
    if descending {
        result_slice[offset..offset + nans_len].copy_from_slice(nans.as_slice());
        append_valids(
            &mut result_slice[offset + nans_len..offset + nans_len + valids_len],
        );
    } else {
        append_valids(&mut result_slice[offset..offset + valids_len]);
        result_slice[offset + valids_len..offset + valids_len + nans_len]
            .copy_from_slice(nans.as_slice());
    }
}

/// Sort strings
fn sort_string(
    values: &ArrayRef,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: &SortOptions,
) -> Result<UInt32Array> {
    let values = as_string_array(values);

    sort_string_helper(
        values,
        value_indices,
        null_indices,
        options,
        |array, idx| array.value(idx as usize),
    )
}

/// Sort dictionary encoded strings
fn sort_string_dictionary<T: ArrowDictionaryKeyType>(
    values: &ArrayRef,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: &SortOptions,
) -> Result<UInt32Array> {
    let values: &DictionaryArray<T> = as_dictionary_array::<T>(values);

    let keys: &PrimitiveArray<T> = &values.keys_array();

    let dict = values.values();
    let dict: &StringArray = as_string_array(&dict);

    sort_string_helper(
        keys,
        value_indices,
        null_indices,
        options,
        |array: &PrimitiveArray<T>, idx| -> &str {
            let key: T::Native = array.value(idx as usize);
            dict.value(key.to_usize().unwrap())
        },
    )
}

/// shared implementation between dictionary encoded and plain string arrays
#[inline]
fn sort_string_helper<'a, A: Array, F>(
    values: &'a A,
    value_indices: Vec<u32>,
    null_indices: Vec<u32>,
    options: &SortOptions,
    value_fn: F,
) -> Result<UInt32Array>
where
    F: Fn(&'a A, u32) -> &str,
{
    let mut valids = value_indices
        .into_iter()
        .map(|index| (index, value_fn(&values, index)))
        .collect::<Vec<(u32, &str)>>();
    let mut nulls = null_indices;
    if !options.descending {
        valids.sort_by_key(|a| a.1);
    } else {
        valids.sort_by_key(|a| Reverse(a.1));
        nulls.reverse();
    }
    // collect the order of valid tuplies
    let mut valid_indices: Vec<u32> = valids.iter().map(|tuple| tuple.0).collect();

    if options.nulls_first {
        nulls.append(&mut valid_indices);
        return Ok(UInt32Array::from(nulls));
    }

    // no need to sort nulls as they are in the correct order already
    valid_indices.append(&mut nulls);

    Ok(UInt32Array::from(valid_indices))
}

/// One column to be used in lexicographical sort
#[derive(Clone, Debug)]
pub struct SortColumn {
    pub values: ArrayRef,
    pub options: Option<SortOptions>,
}

/// Sort a list of `ArrayRef` using `SortOptions` provided for each array.
///
/// Performs a stable lexicographical sort on values and indices.
///
/// Returns an `ArrowError::ComputeError(String)` if any of the array type is either unsupported by
/// `lexsort_to_indices` or `take`.
///
/// Example:
///
/// ```
/// use std::convert::From;
/// use std::sync::Arc;
/// use arrow::array::{ArrayRef, StringArray, PrimitiveArray, as_primitive_array};
/// use arrow::compute::kernels::sort::{SortColumn, SortOptions, lexsort};
/// use arrow::datatypes::Int64Type;
///
/// let sorted_columns = lexsort(&vec![
///     SortColumn {
///         values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
///             None,
///             Some(-2),
///             Some(89),
///             Some(-64),
///             Some(101),
///         ])) as ArrayRef,
///         options: None,
///     },
///     SortColumn {
///         values: Arc::new(StringArray::from(vec![
///             Some("hello"),
///             Some("world"),
///             Some(","),
///             Some("foobar"),
///             Some("!"),
///         ])) as ArrayRef,
///         options: Some(SortOptions {
///             descending: true,
///             nulls_first: false,
///         }),
///     },
/// ]).unwrap();
///
/// assert_eq!(as_primitive_array::<Int64Type>(&sorted_columns[0]).value(1), -64);
/// assert!(sorted_columns[0].is_null(0));
/// ```
pub fn lexsort(columns: &[SortColumn]) -> Result<Vec<ArrayRef>> {
    let indices = lexsort_to_indices(columns)?;
    columns
        .iter()
        .map(|c| take(&c.values, &indices, None))
        .collect()
}

/// Sort elements lexicographically from a list of `ArrayRef` into an unsigned integer
/// (`UInt32Array`) of indices.
pub fn lexsort_to_indices(columns: &[SortColumn]) -> Result<UInt32Array> {
    if columns.len() == 1 {
        // fallback to non-lexical sort
        let column = &columns[0];
        return sort_to_indices(&column.values, column.options);
    }

    let mut row_count = None;
    // convert ArrayRefs to OrdArray trait objects and perform row count check
    let flat_columns = columns
        .iter()
        .map(|column| -> Result<(&Array, Box<OrdArray>, SortOptions)> {
            // row count check
            let curr_row_count = column.values.len() - column.values.offset();
            match row_count {
                None => {
                    row_count = Some(curr_row_count);
                }
                Some(cnt) => {
                    if curr_row_count != cnt {
                        return Err(ArrowError::ComputeError(
                            "lexical sort columns have different row counts".to_string(),
                        ));
                    }
                }
            }
            // flatten and convert to OrdArray
            Ok((
                column.values.as_ref(),
                as_ordarray(&column.values)?,
                column.options.unwrap_or_default(),
            ))
        })
        .collect::<Result<Vec<(&Array, Box<OrdArray>, SortOptions)>>>()?;

    let lex_comparator = |a_idx: &usize, b_idx: &usize| -> Ordering {
        for column in flat_columns.iter() {
            let values = &column.0;
            let ord_array = &column.1;
            let sort_option = column.2;

            match (values.is_valid(*a_idx), values.is_valid(*b_idx)) {
                (true, true) => {
                    match ord_array.cmp_value(*a_idx, *b_idx) {
                        // equal, move on to next column
                        Ordering::Equal => continue,
                        order => {
                            if sort_option.descending {
                                return order.reverse();
                            } else {
                                return order;
                            }
                        }
                    }
                }
                (false, true) => {
                    return if sort_option.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    };
                }
                (true, false) => {
                    return if sort_option.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    };
                }
                // equal, move on to next column
                (false, false) => continue,
            }
        }

        Ordering::Equal
    };

    let mut value_indices = (0..row_count.unwrap()).collect::<Vec<usize>>();
    value_indices.sort_by(lex_comparator);

    Ok(UInt32Array::from(
        value_indices
            .into_iter()
            .map(|i| i as u32)
            .collect::<Vec<u32>>(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;
    use std::iter::FromIterator;
    use std::sync::Arc;

    fn test_sort_to_indices_primitive_arrays<T>(
        data: Vec<Option<T::Native>>,
        options: Option<SortOptions>,
        expected_data: Vec<u32>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>> + ArrayEqual,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = UInt32Array::from(expected_data);
        let output = sort_to_indices(&(Arc::new(output) as ArrayRef), options).unwrap();
        assert!(output.equals(&expected))
    }

    fn test_sort_primitive_arrays<T>(
        data: Vec<Option<T::Native>>,
        options: Option<SortOptions>,
        expected_data: Vec<Option<T::Native>>,
    ) where
        T: ArrowPrimitiveType,
        PrimitiveArray<T>: From<Vec<Option<T::Native>>> + ArrayEqual,
    {
        let output = PrimitiveArray::<T>::from(data);
        let expected = PrimitiveArray::<T>::from(expected_data);
        let output = sort(&(Arc::new(output) as ArrayRef), options).unwrap();
        let output = output.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        assert!(output.equals(&expected))
    }

    fn test_sort_to_indices_string_arrays(
        data: Vec<Option<&str>>,
        options: Option<SortOptions>,
        expected_data: Vec<u32>,
    ) {
        let output = StringArray::from(data);
        let expected = UInt32Array::from(expected_data);
        let output = sort_to_indices(&(Arc::new(output) as ArrayRef), options).unwrap();
        assert!(output.equals(&expected))
    }

    fn test_sort_string_arrays(
        data: Vec<Option<&str>>,
        options: Option<SortOptions>,
        expected_data: Vec<Option<&str>>,
    ) {
        let output = StringArray::from(data);
        let expected = StringArray::from(expected_data);
        let output = sort(&(Arc::new(output) as ArrayRef), options).unwrap();
        let output = output.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(output.equals(&expected))
    }

    fn test_sort_string_dict_arrays<T: ArrowDictionaryKeyType>(
        data: Vec<Option<&str>>,
        options: Option<SortOptions>,
        expected_data: Vec<Option<&str>>,
    ) {
        let array = DictionaryArray::<T>::from_iter(data.into_iter());
        let array_values = array.values();
        let dict = array_values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Unable to get dictionary values");

        let sorted = sort(&(Arc::new(array) as ArrayRef), options).unwrap();
        let sorted = sorted
            .as_any()
            .downcast_ref::<DictionaryArray<T>>()
            .unwrap();
        let sorted_values = sorted.values();
        let sorted_dict = sorted_values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Unable to get dictionary values");
        let sorted_keys = sorted.keys_array();

        assert!(sorted_dict.equals(dict));

        let sorted_strings = StringArray::try_from(
            (0..sorted.len())
                .map(|i| {
                    if sorted.is_valid(i) {
                        Some(sorted_dict.value(sorted_keys.value(i).to_usize().unwrap()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<Option<&str>>>(),
        )
        .expect("Unable to create string array from dictionary");
        let expected =
            StringArray::try_from(expected_data).expect("Unable to create string array");

        assert!(sorted_strings.equals(&expected))
    }

    fn test_lex_sort_arrays(input: Vec<SortColumn>, expected_output: Vec<ArrayRef>) {
        let sorted = lexsort(&input).unwrap();
        let sorted2cmp = sorted.iter().map(|arr| -> Box<&dyn ArrayEqual> {
            match arr.data_type() {
                DataType::Int64 => Box::new(as_primitive_array::<Int64Type>(&arr)),
                DataType::UInt32 => Box::new(as_primitive_array::<UInt32Type>(&arr)),
                DataType::Utf8 => Box::new(as_string_array(&arr)),
                DataType::Dictionary(key_type, _) => match key_type.as_ref() {
                    DataType::Int8 => Box::new(as_dictionary_array::<Int8Type>(&arr)),
                    DataType::Int16 => Box::new(as_dictionary_array::<Int16Type>(&arr)),
                    DataType::Int32 => Box::new(as_dictionary_array::<Int32Type>(&arr)),
                    _ => panic!("unexpected dictionary key type"),
                },
                _ => panic!("unexpected array type"),
            }
        });
        for (i, values) in sorted2cmp.enumerate() {
            assert!(
                values.equals(&(*expected_output[i])),
                "expect {:#?} to be: {:#?}",
                sorted,
                expected_output
            );
        }
    }

    #[test]
    fn test_sort_to_indices_primitives() {
        test_sort_to_indices_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Float32Type>(
            vec![
                None,
                Some(-0.05),
                Some(2.225),
                Some(-1.01),
                Some(-0.05),
                None,
            ],
            None,
            vec![0, 5, 3, 1, 4, 2],
        );
        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![
                None,
                Some(-0.05),
                Some(2.225),
                Some(-1.01),
                Some(-0.05),
                None,
            ],
            None,
            vec![0, 5, 3, 1, 4, 2],
        );

        // descending
        test_sort_to_indices_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![2, 1, 4, 3, 5, 0], // [2, 4, 1, 3, 5, 0]
        );

        test_sort_to_indices_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![2, 1, 4, 3, 5, 0],
        );

        test_sort_to_indices_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![2, 1, 4, 3, 5, 0],
        );

        test_sort_to_indices_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![2, 1, 4, 3, 5, 0],
        );

        test_sort_to_indices_primitive_arrays::<Float32Type>(
            vec![
                None,
                Some(0.005),
                Some(20.22),
                Some(-10.3),
                Some(0.005),
                None,
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![2, 1, 4, 3, 5, 0],
        );

        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![2, 1, 4, 3, 5, 0],
        );

        // descending, nulls first
        test_sort_to_indices_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![5, 0, 2, 1, 4, 3], // [5, 0, 2, 4, 1, 3]
        );

        test_sort_to_indices_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![5, 0, 2, 1, 4, 3], // [5, 0, 2, 4, 1, 3]
        );

        test_sort_to_indices_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![5, 0, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![5, 0, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Float32Type>(
            vec![None, Some(0.1), Some(0.2), Some(-1.3), Some(0.01), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![5, 0, 2, 1, 4, 3],
        );

        test_sort_to_indices_primitive_arrays::<Float64Type>(
            vec![None, Some(10.1), Some(100.2), Some(-1.3), Some(10.01), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![5, 0, 2, 1, 4, 3],
        );

        // boolean
        test_sort_to_indices_primitive_arrays::<BooleanType>(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            None,
            vec![0, 5, 1, 4, 2, 3],
        );

        // boolean, descending
        test_sort_to_indices_primitive_arrays::<BooleanType>(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![2, 3, 1, 4, 5, 0],
        );

        // boolean, descending, nulls first
        test_sort_to_indices_primitive_arrays::<BooleanType>(
            vec![None, Some(false), Some(true), Some(true), Some(false), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![5, 0, 2, 3, 1, 4],
        );
    }

    #[test]
    fn test_sort_primitives() {
        // default case
        test_sort_primitive_arrays::<UInt8Type>(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
        );
        test_sort_primitive_arrays::<UInt16Type>(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
        );
        test_sort_primitive_arrays::<UInt32Type>(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
        );
        test_sort_primitive_arrays::<UInt64Type>(
            vec![None, Some(3), Some(5), Some(2), Some(3), None],
            None,
            vec![None, None, Some(2), Some(3), Some(3), Some(5)],
        );

        // descending
        test_sort_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![Some(2), Some(0), Some(0), Some(-1), None, None],
        );
        test_sort_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![Some(2), Some(0), Some(0), Some(-1), None, None],
        );
        test_sort_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![Some(2), Some(0), Some(0), Some(-1), None, None],
        );
        test_sort_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![Some(2), Some(0), Some(0), Some(-1), None, None],
        );

        // descending, nulls first
        test_sort_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![None, None, Some(2), Some(0), Some(0), Some(-1)],
        );
        test_sort_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![None, None, Some(2), Some(0), Some(0), Some(-1)],
        );
        test_sort_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![None, None, Some(2), Some(0), Some(0), Some(-1)],
        );
        test_sort_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![None, None, Some(2), Some(0), Some(0), Some(-1)],
        );
        test_sort_primitive_arrays::<Float32Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![None, None, Some(2.0), Some(0.0), Some(0.0), Some(-1.0)],
        );
        test_sort_primitive_arrays::<Float64Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(f64::NAN), None],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![None, None, Some(f64::NAN), Some(2.0), Some(0.0), Some(-1.0)],
        );
        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
        );

        // int8 nulls first
        test_sort_primitive_arrays::<Int8Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![None, None, Some(-1), Some(0), Some(0), Some(2)],
        );
        test_sort_primitive_arrays::<Int16Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![None, None, Some(-1), Some(0), Some(0), Some(2)],
        );
        test_sort_primitive_arrays::<Int32Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![None, None, Some(-1), Some(0), Some(0), Some(2)],
        );
        test_sort_primitive_arrays::<Int64Type>(
            vec![None, Some(0), Some(2), Some(-1), Some(0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![None, None, Some(-1), Some(0), Some(0), Some(2)],
        );
        test_sort_primitive_arrays::<Float32Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(0.0), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![None, None, Some(-1.0), Some(0.0), Some(0.0), Some(2.0)],
        );
        test_sort_primitive_arrays::<Float64Type>(
            vec![None, Some(0.0), Some(2.0), Some(-1.0), Some(f64::NAN), None],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![None, None, Some(-1.0), Some(0.0), Some(2.0), Some(f64::NAN)],
        );
        test_sort_primitive_arrays::<Float64Type>(
            vec![Some(f64::NAN), Some(f64::NAN), Some(f64::NAN), Some(1.0)],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![Some(1.0), Some(f64::NAN), Some(f64::NAN), Some(f64::NAN)],
        );
    }

    #[test]
    fn test_sort_to_indices_strings() {
        test_sort_to_indices_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            None,
            vec![0, 3, 5, 1, 4, 2],
        );

        test_sort_to_indices_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![2, 4, 1, 5, 3, 0],
        );

        test_sort_to_indices_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![0, 3, 5, 1, 4, 2],
        );

        test_sort_to_indices_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![3, 0, 2, 4, 1, 5],
        );
    }

    #[test]
    fn test_sort_strings() {
        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            None,
            vec![
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
                None,
                None,
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_arrays(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![
                None,
                None,
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
            ],
        );
    }

    #[test]
    fn test_sort_string_dicts() {
        test_sort_string_dict_arrays::<Int8Type>(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            None,
            vec![
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_dict_arrays::<Int16Type>(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: false,
            }),
            vec![
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
                None,
                None,
            ],
        );

        test_sort_string_dict_arrays::<Int32Type>(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
            vec![
                None,
                None,
                Some("-ad"),
                Some("bad"),
                Some("glad"),
                Some("sad"),
            ],
        );

        test_sort_string_dict_arrays::<Int16Type>(
            vec![
                None,
                Some("bad"),
                Some("sad"),
                None,
                Some("glad"),
                Some("-ad"),
            ],
            Some(SortOptions {
                descending: true,
                nulls_first: true,
            }),
            vec![
                None,
                None,
                Some("sad"),
                Some("glad"),
                Some("bad"),
                Some("-ad"),
            ],
        );
    }

    #[test]
    fn test_lex_sort_single_column() {
        let input = vec![SortColumn {
            values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(17),
                Some(2),
                Some(-1),
                Some(0),
            ])) as ArrayRef,
            options: None,
        }];
        let expected = vec![Arc::new(PrimitiveArray::<Int64Type>::from(vec![
            Some(-1),
            Some(0),
            Some(2),
            Some(17),
        ])) as ArrayRef];
        test_lex_sort_arrays(input, expected);
    }

    #[test]
    fn test_lex_sort_unaligned_rows() {
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![None, Some(-1)]))
                    as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![Some("foo")])) as ArrayRef,
                options: None,
            },
        ];
        assert!(
            lexsort(&input).is_err(),
            "lexsort should reject columns with different row counts"
        );
    }

    #[test]
    fn test_lex_sort_mixed_types() {
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    Some(0),
                    Some(2),
                    Some(-1),
                    Some(0),
                ])) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(PrimitiveArray::<UInt32Type>::from(vec![
                    Some(101),
                    Some(8),
                    Some(7),
                    Some(102),
                ])) as ArrayRef,
                options: None,
            },
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    Some(-1),
                    Some(-2),
                    Some(-3),
                    Some(-4),
                ])) as ArrayRef,
                options: None,
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(0),
                Some(0),
                Some(2),
            ])) as ArrayRef,
            Arc::new(PrimitiveArray::<UInt32Type>::from(vec![
                Some(7),
                Some(101),
                Some(102),
                Some(8),
            ])) as ArrayRef,
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-3),
                Some(-1),
                Some(-4),
                Some(-2),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);

        // test mix of string and in64 with option
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    Some(0),
                    Some(2),
                    Some(-1),
                    Some(0),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("9"),
                    Some("7"),
                    Some("bar"),
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(2),
                Some(0),
                Some(0),
                Some(-1),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("9"),
                Some("foo"),
                Some("bar"),
                Some("7"),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);

        // test sort with nulls first
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    None,
                    Some(-1),
                    Some(2),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("world"),
                    Some("hello"),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                None,
                None,
                Some(2),
                Some(-1),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                None,
                Some("foo"),
                Some("hello"),
                Some("world"),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);

        // test sort with nulls last
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    None,
                    Some(-1),
                    Some(2),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("world"),
                    Some("hello"),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(2),
                Some(-1),
                None,
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("hello"),
                Some("world"),
                Some("foo"),
                None,
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);

        // test sort with opposite options
        let input = vec![
            SortColumn {
                values: Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                    None,
                    Some(-1),
                    Some(2),
                    Some(-1),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            },
            SortColumn {
                values: Arc::new(StringArray::from(vec![
                    Some("foo"),
                    Some("bar"),
                    Some("world"),
                    Some("hello"),
                    None,
                ])) as ArrayRef,
                options: Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                }),
            },
        ];
        let expected = vec![
            Arc::new(PrimitiveArray::<Int64Type>::from(vec![
                Some(-1),
                Some(-1),
                Some(2),
                None,
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("hello"),
                Some("bar"),
                Some("world"),
                None,
                Some("foo"),
            ])) as ArrayRef,
        ];
        test_lex_sort_arrays(input, expected);
    }
}

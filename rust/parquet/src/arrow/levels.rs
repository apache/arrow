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

//! Parquet definition and repetition levels
//!
//! Contains the algorithm for computing definition and repetition levels.
//! The algorithm works by tracking the slots of an array that should
//! ultimately be populated when writing to Parquet.
//! Parquet achieves nesting through definition levels and repetition levels \[1\].
//! Definition levels specify how many optional fields in the part for the column
//! are defined.
//! Repetition levels specify at what repeated field (list) in the path a column
//! is defined.
//!
//! In a nested data structure such as `a.b.c`, one can see levels as defining
//! whether a record is defined at `a`, `a.b`, or `a.b.c`.
//! Optional fields are nullable fields, thus if all 3 fields
//! are nullable, the maximum definition could be = 3 if there are no lists.
//!
//! The algorithm in this module computes the necessary information to enable
//! the writer to keep track of which columns are at which levels, and to extract
//! the correct values at the correct slots from Arrow arrays.
//!
//! It works by walking a record batch's arrays, keeping track of what values
//! are non-null, their positions and computing what their levels are.
//!
//! \[1\] [parquet-format#nested-encoding](https://github.com/apache/parquet-format#nested-encoding)

use arrow::array::{make_array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;

/// Keeps track of the level information per array that is needed to write an Arrow array to Parquet.
///
/// When a nested schema is traversed, intermediate [LevelInfo] structs are created to track
/// the state of parent arrays. When a primitive Arrow array is encountered, a final [LevelInfo]
/// is created, and this is what is used to index into the array when writing data to Parquet.
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct LevelInfo {
    /// Array's definition levels
    pub definition: Vec<i16>,
    /// Array's optional repetition levels
    pub repetition: Option<Vec<i16>>,
    /// Array's offsets, 64-bit is used to accommodate large offset arrays
    pub array_offsets: Vec<i64>,
    // TODO: Convert to an Arrow Buffer after ARROW-10766 is merged.
    /// Array's logical validity mask, whcih gets unpacked for list children.
    /// If the parent of an array is null, all children are logically treated as
    /// null. This mask keeps track of that.
    ///
    pub array_mask: Vec<bool>,
    /// The maximum definition at this level, 0 at the record batch
    pub max_definition: i16,
    /// Whether this array or any of its parents is a list
    pub is_list: bool,
    /// Whether the current array is nullable (affects definition levels)
    pub is_nullable: bool,
}

impl LevelInfo {
    /// Create a new [LevelInfo] from a record batch.
    ///
    /// This is a convenience function to populate the starting point of the traversal.
    pub(crate) fn new_from_batch(batch: &RecordBatch) -> Self {
        let num_rows = batch.num_rows();
        Self {
            // a batch has no definition level yet
            definition: vec![0; num_rows],
            // a batch has no repetition as it is not a list
            repetition: None,
            // a batch has sequential offsets, should be num_rows + 1
            array_offsets: (0..=(num_rows as i64)).collect(),
            // all values at a batch-level are non-null
            array_mask: vec![true; num_rows],
            max_definition: 0,
            is_list: false,
            // a batch is treated as nullable even though it has no nulls,
            // this is required to compute nested type levels correctly
            is_nullable: false,
        }
    }

    /// Compute nested levels of the Arrow array, recursing into lists and structs.
    ///
    /// Returns a list of `LevelInfo`, where each level is for nested primitive arrays.
    pub(crate) fn calculate_array_levels(
        &self,
        array: &ArrayRef,
        field: &Field,
    ) -> Vec<Self> {
        let (array_offsets, array_mask) = Self::get_array_offsets_and_masks(array);
        match array.data_type() {
            DataType::Null => vec![Self {
                definition: self.definition.clone(),
                repetition: self.repetition.clone(),
                array_offsets: self.array_offsets.clone(),
                array_mask,
                max_definition: self.max_definition.max(1),
                is_list: self.is_list,
                is_nullable: true, // always nullable as all values are nulls
            }],
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Timestamp(_, _)
            | DataType::Date32(_)
            | DataType::Date64(_)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::LargeBinary => {
                // we return a vector of 1 value to represent the primitive
                vec![self.calculate_child_levels(
                    array_offsets,
                    array_mask,
                    false,
                    field.is_nullable(),
                )]
            }
            DataType::FixedSizeBinary(_) => unimplemented!(),
            DataType::Decimal(_, _) => unimplemented!(),
            DataType::List(list_field) | DataType::LargeList(list_field) => {
                // Calculate the list level
                let list_level = self.calculate_child_levels(
                    array_offsets,
                    array_mask,
                    true,
                    field.is_nullable(),
                );

                // Construct the child array of the list, and get its offset + mask
                let array_data = array.data();
                let child_data = array_data.child_data().get(0).unwrap();
                let child_array = make_array(child_data.clone());
                let (child_offsets, child_mask) =
                    Self::get_array_offsets_and_masks(&child_array);

                match child_array.data_type() {
                    // TODO: The behaviour of a <list<null>> is untested
                    DataType::Null => vec![list_level],
                    DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Timestamp(_, _)
                    | DataType::Date32(_)
                    | DataType::Date64(_)
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Duration(_)
                    | DataType::Interval(_)
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Dictionary(_, _) => {
                        vec![list_level.calculate_child_levels(
                            child_offsets,
                            child_mask,
                            false,
                            list_field.is_nullable(),
                        )]
                    }
                    DataType::FixedSizeBinary(_) => unimplemented!(),
                    DataType::Decimal(_, _) => unimplemented!(),
                    DataType::List(_) | DataType::LargeList(_) | DataType::Struct(_) => {
                        list_level.calculate_array_levels(&child_array, list_field)
                    }
                    DataType::FixedSizeList(_, _) => unimplemented!(),
                    DataType::Union(_) => unimplemented!(),
                }
            }
            DataType::FixedSizeList(_, _) => unimplemented!(),
            DataType::Struct(struct_fields) => {
                let struct_array: &StructArray = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Unable to get struct array");
                let struct_level = self.calculate_child_levels(
                    array_offsets,
                    array_mask,
                    false,
                    field.is_nullable(),
                );
                let mut struct_levels = vec![];
                struct_array
                    .columns()
                    .into_iter()
                    .zip(struct_fields)
                    .for_each(|(child_array, child_field)| {
                        let mut levels =
                            struct_level.calculate_array_levels(child_array, child_field);
                        struct_levels.append(&mut levels);
                    });
                struct_levels
            }
            DataType::Union(_) => unimplemented!(),
            DataType::Dictionary(_, _) => {
                // Need to check for these cases not implemented in C++:
                // - "Writing DictionaryArray with nested dictionary type not yet supported"
                // - "Writing DictionaryArray with null encoded in dictionary type not yet supported"
                // vec![self.get_primitive_def_levels(array, field, array_mask)]
                vec![self.calculate_child_levels(
                    array_offsets,
                    array_mask,
                    false,
                    field.is_nullable(),
                )]
            }
        }
    }

    /// Calculate child/leaf array levels.
    ///
    /// The algorithm works by incrementing definitions of array values based on whether:
    /// - a value is optional or required (is_nullable)
    /// - a list value is repeated + optional or required (is_list)
    ///
    /// A record batch always starts at a populated definition = level 0.
    /// When a batch only has a primitive, i.e. `<batch<primitive[a]>>, column `a`
    /// can only have a maximum level of 1 if it is not null.
    /// If it is not null, we increment by 1, such that the null slots will = level 1.
    /// The above applies to types that have no repetition (anything not a list or map).
    ///
    /// If a batch has lists, then we increment by up to 2 levels:
    /// - 1 level for the list (repeated)
    /// - 1 level if the list itself is nullable (optional)
    ///
    /// A list's child then gets incremented using the above rules.
    ///
    /// *Exceptions*
    ///
    /// There are 2 exceptions from the above rules:
    ///
    /// 1. When at the root of the schema: We always increment the
    /// level regardless of whether the child is nullable or not. If we do not do
    /// this, we could have a non-nullable array having a definition of 0.
    ///
    /// 2. List parent, non-list child: We always increment the level in this case,
    /// regardless of whether the child is nullable or not.
    ///
    /// *Examples*
    ///
    /// A batch with only a primitive that's non-nullable. `<primitive[required]>`:
    /// * We don't increment the definition level as the array is not optional.
    /// * This would leave us with a definition of 0, so the first exception applies.
    /// * The definition level becomes 1.
    ///
    /// A batch with only a primitive that's nullable. `<primitive[optional]>`:
    /// * The definition level becomes 1, as we increment it once.
    ///
    /// A batch with a single non-nullable list (both list and child not null):
    /// * We calculate the level twice, for the list, and for the child.
    /// * At the list, the level becomes 1, where 0 indicates that the list is
    ///  empty, and 1 says it's not (determined through offsets).
    /// * At the primitive level, the second exception applies. The level becomes 2.
    fn calculate_child_levels(
        &self,
        // we use 64-bit offsets to also accommodate large arrays
        array_offsets: Vec<i64>,
        array_mask: Vec<bool>,
        is_list: bool,
        is_nullable: bool,
    ) -> Self {
        let mut definition = vec![];
        let mut repetition = vec![];
        let mut merged_array_mask = vec![];

        // determine the total level increment based on data types
        let max_definition = match is_list {
            false => {
                // first exception, start of a batch, and not list
                if self.max_definition == 0 {
                    1
                } else if self.is_list {
                    // second exception, always increment after a list
                    self.max_definition + 1
                } else {
                    self.max_definition + is_nullable as i16
                }
            }
            true => self.max_definition + 1 + is_nullable as i16,
        };

        match (self.is_list, is_list) {
            (false, false) => {
                self.definition
                    .iter()
                    .zip(array_mask.into_iter().zip(&self.array_mask))
                    .for_each(|(def, (child_mask, parent_mask))| {
                        merged_array_mask.push(*parent_mask && child_mask);
                        match (parent_mask, child_mask) {
                            (true, true) => {
                                definition.push(max_definition);
                            }
                            (true, false) => {
                                // The child is only legally null if its array is nullable.
                                // Thus parent's max_definition is lower
                                definition.push(if *def <= self.max_definition {
                                    *def
                                } else {
                                    self.max_definition
                                });
                            }
                            // if the parent was false, retain its definitions
                            (false, _) => {
                                definition.push(*def);
                            }
                        }
                    });

                debug_assert_eq!(definition.len(), merged_array_mask.len());

                Self {
                    definition,
                    repetition: self.repetition.clone(), // it's None
                    array_offsets,
                    array_mask: merged_array_mask,
                    max_definition,
                    is_list: false,
                    is_nullable,
                }
            }
            (true, true) => {
                // parent is a list or descendant of a list, and child is a list
                let reps = self.repetition.clone().unwrap();
                // Calculate the 2 list hierarchy definitions in advance
                // List is not empty, but null
                let l2 = max_definition - is_nullable as i16;
                // List is not empty, and not null
                let l3 = max_definition;

                let mut nulls_seen = 0;

                self.array_offsets.windows(2).for_each(|w| {
                    let start = w[0] as usize;
                    let end = w[1] as usize;
                    let parent_len = end - start;

                    if parent_len == 0 {
                        // If the parent length is 0, there won't be a slot for the child
                        let index = start + nulls_seen;
                        definition.push(self.definition[index]);
                        repetition.push(0);
                        merged_array_mask.push(self.array_mask[index]);
                        nulls_seen += 1;
                    } else {
                        (start..end).for_each(|parent_index| {
                            let index = parent_index + nulls_seen;

                            // parent is either defined at this level, or earlier
                            let parent_def = self.definition[index];
                            let parent_rep = reps[index];
                            let parent_mask = self.array_mask[index];

                            // valid parent, index into children
                            let child_start = array_offsets[parent_index] as usize;
                            let child_end = array_offsets[parent_index + 1] as usize;
                            let child_len = child_end - child_start;
                            let child_mask = array_mask[parent_index];
                            let merged_mask = parent_mask && child_mask;

                            if child_len == 0 {
                                definition.push(parent_def);
                                repetition.push(parent_rep);
                                merged_array_mask.push(merged_mask);
                            } else {
                                (child_start..child_end).for_each(|child_index| {
                                    let rep = match (
                                        parent_index == start,
                                        child_index == child_start,
                                    ) {
                                        (true, true) => parent_rep,
                                        (true, false) => parent_rep + 2,
                                        (false, true) => parent_rep,
                                        (false, false) => parent_rep + 1,
                                    };

                                    definition.push(if !parent_mask {
                                        parent_def
                                    } else if child_mask {
                                        l3
                                    } else {
                                        l2
                                    });
                                    repetition.push(rep);
                                    merged_array_mask.push(merged_mask);
                                });
                            }
                        });
                    }
                });

                debug_assert_eq!(definition.len(), merged_array_mask.len());

                Self {
                    definition,
                    repetition: Some(repetition),
                    array_offsets,
                    array_mask: merged_array_mask,
                    max_definition,
                    is_list: true,
                    is_nullable,
                }
            }
            (true, false) => {
                // List and primitive (or struct).
                // The list can have more values than the primitive, indicating that there
                // are slots where the list is empty. We use a counter to track this behaviour.
                let mut nulls_seen = 0;

                // let child_max_definition = list_max_definition + is_nullable as i16;
                // child values are a function of parent list offsets
                let reps = self.repetition.as_deref().unwrap();
                self.array_offsets.windows(2).for_each(|w| {
                    let start = w[0] as usize;
                    let end = w[1] as usize;
                    let parent_len = end - start;

                    if parent_len == 0 {
                        let index = start + nulls_seen;
                        definition.push(self.definition[index]);
                        repetition.push(reps[index]);
                        merged_array_mask.push(self.array_mask[index]);
                        nulls_seen += 1;
                    } else {
                        // iterate through the array, adjusting child definitions for nulls
                        (start..end).for_each(|child_index| {
                            let index = child_index + nulls_seen;
                            let child_mask = array_mask[child_index];
                            let parent_mask = self.array_mask[index];
                            let parent_def = self.definition[index];

                            if !parent_mask || parent_def < self.max_definition {
                                definition.push(parent_def);
                                repetition.push(reps[index]);
                                merged_array_mask.push(parent_mask);
                            } else {
                                definition.push(max_definition - !child_mask as i16);
                                repetition.push(reps[index]);
                                merged_array_mask.push(child_mask);
                            }
                        });
                    }
                });

                debug_assert_eq!(definition.len(), merged_array_mask.len());

                Self {
                    definition,
                    repetition: Some(repetition),
                    array_offsets: self.array_offsets.clone(),
                    array_mask: merged_array_mask,
                    max_definition,
                    is_list: true,
                    is_nullable,
                }
            }
            (false, true) => {
                // Encountering a list for the first time.
                // Calculate the 2 list hierarchy definitions in advance

                // List is not empty, but null (if nullable)
                let l2 = max_definition - is_nullable as i16;
                // List is not empty, and not null
                let l3 = max_definition;

                self.definition
                    .iter()
                    .enumerate()
                    .for_each(|(parent_index, def)| {
                        let child_from = array_offsets[parent_index];
                        let child_to = array_offsets[parent_index + 1];
                        let child_len = child_to - child_from;
                        let child_mask = array_mask[parent_index];
                        let parent_mask = self.array_mask[parent_index];

                        match (parent_mask, child_len) {
                            (true, 0) => {
                                // empty slot that is valid, i.e. {"parent": {"child": [] } }
                                definition.push(if child_mask {
                                    l2
                                } else {
                                    self.max_definition
                                });
                                repetition.push(0);
                                merged_array_mask.push(child_mask);
                            }
                            (false, 0) => {
                                definition.push(*def);
                                repetition.push(0);
                                merged_array_mask.push(child_mask);
                            }
                            (true, _) => {
                                (child_from..child_to).for_each(|child_index| {
                                    definition.push(if child_mask { l3 } else { l2 });
                                    // mark the first child slot as 0, and the next as 1
                                    repetition.push(if child_index == child_from {
                                        0
                                    } else {
                                        1
                                    });
                                    merged_array_mask.push(child_mask);
                                });
                            }
                            (false, _) => {
                                (child_from..child_to).for_each(|child_index| {
                                    definition.push(*def);
                                    // mark the first child slot as 0, and the next as 1
                                    repetition.push(if child_index == child_from {
                                        0
                                    } else {
                                        1
                                    });
                                    merged_array_mask.push(false);
                                });
                            }
                        }
                    });

                debug_assert_eq!(definition.len(), merged_array_mask.len());

                Self {
                    definition,
                    repetition: Some(repetition),
                    array_offsets,
                    array_mask: merged_array_mask,
                    max_definition,
                    is_list: true,
                    is_nullable,
                }
            }
        }
    }

    /// Get the offsets of an array as 64-bit values, and validity masks as booleans
    /// - Primitive, binary and struct arrays' offsets will be a sequence, masks obtained
    ///   from validity bitmap
    /// - List array offsets will be the value offsets, masks are computed from offsets
    fn get_array_offsets_and_masks(array: &ArrayRef) -> (Vec<i64>, Vec<bool>) {
        match array.data_type() {
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32(_)
            | DataType::Date64(_)
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Struct(_)
            | DataType::Dictionary(_, _)
            | DataType::Decimal(_, _) => {
                let array_mask = match array.data().null_buffer() {
                    Some(buf) => get_bool_array_slice(buf, array.offset(), array.len()),
                    None => vec![true; array.len()],
                };
                ((0..=(array.len() as i64)).collect(), array_mask)
            }
            DataType::List(_) => {
                let data = array.data();
                let offsets = unsafe { data.buffers()[0].typed_data::<i32>() };
                let offsets = offsets
                    .to_vec()
                    .into_iter()
                    .map(|v| v as i64)
                    .collect::<Vec<i64>>();
                let masks = offsets.windows(2).map(|w| w[1] > w[0]).collect();
                (offsets, masks)
            }
            DataType::LargeList(_) => {
                let offsets =
                    unsafe { array.data().buffers()[0].typed_data::<i64>() }.to_vec();
                let masks = offsets.windows(2).map(|w| w[1] > w[0]).collect();
                (offsets, masks)
            }
            DataType::FixedSizeBinary(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Union(_) => {
                unimplemented!("Getting offsets not yet implemented")
            }
        }
    }

    /// Given a level's information, calculate the offsets required to index an array correctly.
    pub(crate) fn filter_array_indices(&self) -> Vec<usize> {
        // happy path if not dealing with lists
        if !self.is_list {
            return self
                .definition
                .iter()
                .enumerate()
                .filter_map(|(i, def)| {
                    if *def == self.max_definition {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect();
        }
        let mut filtered = vec![];
        // remove slots that are false from definition_mask
        let mut index = 0;
        self.definition.iter().for_each(|def| {
            if *def == self.max_definition {
                filtered.push(index);
            }
            if *def >= self.max_definition - self.is_nullable as i16 {
                index += 1;
            }
        });
        filtered
    }
}

/// Convert an Arrow buffer to a boolean array slice
/// TODO: this was created for buffers, so might not work for bool array, might be slow too
#[inline]
fn get_bool_array_slice(
    buffer: &arrow::buffer::Buffer,
    offset: usize,
    len: usize,
) -> Vec<bool> {
    let data = buffer.as_slice();
    (offset..(len + offset))
        .map(|i| arrow::util::bit_util::get_bit(data, i))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::ListArray,
        array::{Array, ArrayData, Int32Array},
        buffer::Buffer,
        datatypes::Schema,
    };
    use arrow::{
        array::{Float32Array, Float64Array, Int16Array},
        datatypes::ToByteSlice,
    };

    use super::*;

    #[test]
    fn test_calculate_array_levels_twitter_example() {
        // based on the example at https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
        // [[a, b, c], [d, e, f, g]], [[h], [i,j]]
        let parent_levels = LevelInfo {
            definition: vec![0, 0],
            repetition: None,
            array_offsets: vec![0, 1, 2], // 2 records, root offsets always sequential
            array_mask: vec![true, true], // both lists defined
            max_definition: 0,
            is_list: false,     // root is never list
            is_nullable: false, // root in example is non-nullable
        };
        // offset into array, each level1 has 2 values
        let array_offsets = vec![0, 2, 4];
        let array_mask = vec![true, true];

        // calculate level1 levels
        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            true,
            false,
        );
        //
        let expected_levels = LevelInfo {
            definition: vec![1, 1, 1, 1],
            repetition: Some(vec![0, 1, 0, 1]),
            array_offsets,
            array_mask: vec![true, true, true, true],
            max_definition: 1,
            is_list: true,
            is_nullable: false,
        };
        // the separate asserts make it easier to see what's failing
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_mask, &expected_levels.array_mask);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.is_list, &expected_levels.is_list);
        assert_eq!(&levels.is_nullable, &expected_levels.is_nullable);
        // this assert is to help if there are more variables added to the struct
        assert_eq!(&levels, &expected_levels);

        // level2
        let parent_levels = levels;
        let array_offsets = vec![0, 3, 7, 8, 10];
        let array_mask = vec![true, true, true, true];
        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            true,
            false,
        );
        let expected_levels = LevelInfo {
            definition: vec![2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
            repetition: Some(vec![0, 2, 2, 1, 2, 2, 2, 0, 1, 2]),
            array_offsets,
            array_mask: vec![true; 10],
            max_definition: 2,
            is_list: true,
            is_nullable: false,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_mask, &expected_levels.array_mask);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.is_list, &expected_levels.is_list);
        assert_eq!(&levels.is_nullable, &expected_levels.is_nullable);
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_one_level_1() {
        // This test calculates the levels for a non-null primitive array
        let parent_levels = LevelInfo {
            definition: vec![0; 10],
            repetition: None,
            array_offsets: (0..=10).collect(),
            array_mask: vec![true; 10],
            max_definition: 0,
            is_list: false,
            is_nullable: false,
        };
        let array_offsets: Vec<i64> = (0..=10).collect();
        let array_mask = vec![true; 10];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            false,
            false,
        );
        let expected_levels = LevelInfo {
            definition: vec![1; 10],
            repetition: None,
            array_offsets,
            array_mask,
            max_definition: 1,
            is_list: false,
            is_nullable: false,
        };
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_one_level_2() {
        // This test calculates the levels for a non-null primitive array
        let parent_levels = LevelInfo {
            definition: vec![0; 5],
            repetition: None,
            array_offsets: (0..=5).collect(),
            array_mask: vec![true, true, true, true, true],
            max_definition: 0,
            is_list: false,
            is_nullable: false,
        };
        let array_offsets: Vec<i64> = (0..=5).collect();
        let array_mask = vec![true, false, true, true, false];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            false,
            true,
        );
        let expected_levels = LevelInfo {
            definition: vec![1, 0, 1, 1, 0],
            repetition: None,
            array_offsets,
            array_mask,
            max_definition: 1,
            is_list: false,
            is_nullable: true,
        };
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_array_levels_1() {
        // if all array values are defined (e.g. batch<list<_>>)
        // [[0], [1], [2], [3], [4]]
        let parent_levels = LevelInfo {
            definition: vec![0; 5],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, true, true, true, true],
            max_definition: 0,
            is_list: false,
            is_nullable: false,
        };
        let array_offsets = vec![0, 2, 2, 4, 8, 11];
        let array_mask = vec![true, false, true, true, true];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            true,
            true,
        );
        // array: [[0, 0], _1_, [2, 2], [3, 3, 3, 3], [4, 4, 4]]
        // all values are defined as we do not have nulls on the root (batch)
        // repetition:
        //   0: 0, 1
        //   1:
        //   2: 0, 1
        //   3: 0, 1, 1, 1
        //   4: 0, 1, 1
        let expected_levels = LevelInfo {
            definition: vec![2, 2, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2],
            repetition: Some(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            array_offsets,
            array_mask: vec![
                true, true, false, true, true, true, true, true, true, true, true, true,
            ],
            max_definition: 2,
            is_list: true,
            is_nullable: true,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.is_list, &expected_levels.is_list);
        assert_eq!(&levels.is_nullable, &expected_levels.is_nullable);
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_array_levels_2() {
        // If some values are null
        //
        // This emulates an array in the form: <struct<list<?>>
        // with values:
        // - 0: [0, 1], but is null because of the struct
        // - 1: []
        // - 2: [2, 3], but is null because of the struct
        // - 3: [4, 5, 6, 7]
        // - 4: [8, 9, 10]
        //
        // If the first values of a list are null due to a parent, we have to still account for them
        // while indexing, because they would affect the way the child is indexed
        // i.e. in the above example, we have to know that [0, 1] has to be skipped
        let parent_levels = LevelInfo {
            definition: vec![0, 1, 0, 1, 1],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![false, true, false, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: true,
        };
        let array_offsets = vec![0, 2, 2, 4, 8, 11];
        let array_mask = vec![true, false, true, true, true];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            true,
            true,
        );
        let expected_levels = LevelInfo {
            // 0 1 [2] are 0 (not defined at level 1)
            // [2] is 1, but has 0 slots so is not populated (defined at level 1 only)
            // 2 3 [4] are 0
            // 4 5 6 7 [8] are 1 (defined at level 1 only)
            // 8 9 10 [11] are 2 (defined at both levels)
            definition: vec![0, 0, 1, 0, 0, 3, 3, 3, 3, 3, 3, 3],
            repetition: Some(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            array_offsets,
            array_mask: vec![
                false, false, false, false, false, true, true, true, true, true, true,
                true,
            ],
            max_definition: 3,
            is_nullable: true,
            is_list: true,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.is_list, &expected_levels.is_list);
        assert_eq!(&levels.is_nullable, &expected_levels.is_nullable);
        assert_eq!(&levels, &expected_levels);

        // nested lists (using previous test)
        let nested_parent_levels = levels;
        let array_offsets = vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22];
        let array_mask = vec![
            true, true, true, true, true, true, true, true, true, true, true,
        ];
        let levels = nested_parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            true,
            true,
        );
        let expected_levels = LevelInfo {
            // (def: 0) 0 1 [2] are 0 (take parent)
            // (def: 0) 2 3 [4] are 0 (take parent)
            // (def: 0) 4 5 [6] are 0 (take parent)
            // (def: 0) 6 7 [8] are 0 (take parent)
            // (def: 1) 8 9 [10] are 1 (take parent)
            // (def: 1) 10 11 [12] are 1 (take parent)
            // (def: 1) 12 23 [14] are 1 (take parent)
            // (def: 1) 14 15 [16] are 1 (take parent)
            // (def: 2) 16 17 [18] are 2 (defined at all levels)
            // (def: 2) 18 19 [20] are 2 (defined at all levels)
            // (def: 2) 20 21 [22] are 2 (defined at all levels)
            //
            // 0 1 [2] are 0 (not defined at level 1)
            // [2] is 1, but has 0 slots so is not populated (defined at level 1 only)
            // 2 3 [4] are 0
            // 4 5 6 7 [8] are 1 (defined at level 1 only)
            // 8 9 10 [11] are 2 (defined at both levels)
            //
            // 0: [[100, 101], [102, 103]]
            // 1: []
            // 2: [[104, 105], [106, 107]]
            // 3: [[108, 109], [110, 111], [112, 113], [114, 115]]
            // 4: [[116, 117], [118, 119], [120, 121]]
            definition: vec![
                0, 0, 0, 0, 1, 0, 0, 0, 0, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            ],
            repetition: Some(vec![
                0, 2, 1, 2, 0, 0, 2, 1, 2, 0, 2, 1, 2, 1, 2, 1, 2, 0, 2, 1, 2, 1, 2,
            ]),
            array_offsets,
            array_mask: vec![
                false, false, false, false, false, false, false, false, false, true,
                true, true, true, true, true, true, true, true, true, true, true, true,
                true,
            ],
            max_definition: 5,
            is_nullable: true,
            is_list: true,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.array_mask, &expected_levels.array_mask);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.is_list, &expected_levels.is_list);
        assert_eq!(&levels.is_nullable, &expected_levels.is_nullable);
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_array_levels_nested_list() {
        // if all array values are defined (e.g. batch<list<_>>)
        // The array at this level looks like:
        // 0: [a]
        // 1: [a]
        // 2: [a]
        // 3: [a]
        let parent_levels = LevelInfo {
            definition: vec![1, 1, 1, 1],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4],
            array_mask: vec![true, true, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: false,
        };
        // 0: null ([], but mask is false, so it's not just an empty list)
        // 1: [1, 2, 3]
        // 2: [4, 5]
        // 3: [6, 7]
        let array_offsets = vec![0, 1, 4, 6, 8];
        let array_mask = vec![false, true, true, true];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            true,
            true,
        );
        // 0: [null], level 1 is defined, but not 2
        // 1: [1, 2, 3]
        // 2: [4, 5]
        // 3: [6, 7]
        let expected_levels = LevelInfo {
            definition: vec![2, 3, 3, 3, 3, 3, 3, 3],
            repetition: Some(vec![0, 0, 1, 1, 0, 1, 0, 1]),
            array_offsets,
            array_mask: vec![false, true, true, true, true, true, true, true],
            max_definition: 3,
            is_list: true,
            is_nullable: true,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.is_list, &expected_levels.is_list);
        assert_eq!(&levels.is_nullable, &expected_levels.is_nullable);
        assert_eq!(&levels, &expected_levels);

        // nested lists (using previous test)
        let nested_parent_levels = levels;
        // 0: [null] (was a populated null slot at the parent)
        // 1: [201]
        // 2: [202, 203]
        // 3: null ([])
        // 4: [204, 205, 206]
        // 5: [207, 208, 209, 210]
        // 6: [] (tests a non-null empty list slot)
        // 7: [211, 212, 213, 214, 215]
        let array_offsets = vec![0, 1, 2, 4, 4, 7, 11, 11, 16];
        // logically, the fist slot of the mask is false
        let array_mask = vec![true, true, true, false, true, true, true, true];
        let levels = nested_parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask,
            true,
            true,
        );
        // We have 7 array values, and at least 15 primitives (from array_offsets)
        // 0: (-)[null], parent was null, no value populated here
        // 1: (0)[201], (1)[202, 203], (2)[[null]]
        // 2: (3)[204, 205, 206], (4)[207, 208, 209, 210]
        // 3: (5)[[]], (6)[211, 212, 213, 214, 215]
        //
        // In a JSON syntax with the schema: <struct<list<list<primitive>>>>, this translates into:
        // 0: {"struct": [ null ]}
        // 1: {"struct": [ [201], [202, 203], [] ]}
        // 2: {"struct": [ [204, 205, 206], [207, 208, 209, 210] ]}
        // 3: {"struct": [ [], [211, 212, 213, 214, 215] ]}
        let expected_levels = LevelInfo {
            definition: vec![2, 5, 5, 5, 3, 5, 5, 5, 5, 5, 5, 5, 3, 5, 5, 5, 5, 5],
            repetition: Some(vec![0, 0, 1, 2, 1, 0, 2, 2, 1, 2, 2, 2, 0, 1, 2, 2, 2, 2]),
            array_mask: vec![
                false, true, true, true, false, true, true, true, true, true, true, true,
                true, true, true, true, true, true,
            ],
            array_offsets,
            is_list: true,
            is_nullable: true,
            max_definition: 5,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.array_mask, &expected_levels.array_mask);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.is_list, &expected_levels.is_list);
        assert_eq!(&levels.is_nullable, &expected_levels.is_nullable);
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_nested_struct_levels() {
        // tests a <struct[a]<struct[b]<int[c]>>
        // array:
        //  - {a: {b: {c: 1}}}
        //  - {a: {b: {c: null}}}
        //  - {a: {b: {c: 3}}}
        //  - {a: {b: null}}
        //  - {a: null}}
        //  - {a: {b: {c: 6}}}
        let a_levels = LevelInfo {
            definition: vec![1, 1, 1, 1, 0, 1],
            repetition: None,
            array_offsets: (0..=6).collect(),
            array_mask: vec![true, true, true, true, false, true],
            max_definition: 1,
            is_list: false,
            is_nullable: true,
        };
        // b's offset and mask
        let b_offsets: Vec<i64> = (0..=6).collect();
        let b_mask = vec![true, true, true, false, false, true];
        // b's expected levels
        let b_expected_levels = LevelInfo {
            definition: vec![2, 2, 2, 1, 0, 2],
            repetition: None,
            array_offsets: (0..=6).collect(),
            array_mask: vec![true, true, true, false, false, true],
            max_definition: 2,
            is_list: false,
            is_nullable: true,
        };
        let b_levels =
            a_levels.calculate_child_levels(b_offsets.clone(), b_mask, false, true);
        assert_eq!(&b_expected_levels, &b_levels);

        // c's offset and mask
        let c_offsets = b_offsets;
        let c_mask = vec![true, false, true, false, false, true];
        // c's expected levels
        let c_expected_levels = LevelInfo {
            definition: vec![3, 2, 3, 1, 0, 3],
            repetition: None,
            array_offsets: c_offsets.clone(),
            array_mask: vec![true, false, true, false, false, true],
            max_definition: 3,
            is_list: false,
            is_nullable: true,
        };
        let c_levels = b_levels.calculate_child_levels(c_offsets, c_mask, false, true);
        assert_eq!(&c_expected_levels, &c_levels);
    }

    #[test]
    fn list_single_column() {
        // this tests the level generation from the arrow_writer equivalent test

        let a_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let a_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());
        let a_list_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
        let a_list_data = ArrayData::builder(a_list_type.clone())
            .len(5)
            .add_buffer(a_value_offsets)
            .null_bit_buffer(Buffer::from(vec![0b00011011]))
            .add_child_data(a_values.data())
            .build();

        assert_eq!(a_list_data.null_count(), 1);

        let a = ListArray::from(a_list_data);
        let values = Arc::new(a);

        let schema = Schema::new(vec![Field::new("item", a_list_type, true)]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();

        let expected_batch_level = LevelInfo {
            definition: vec![0; 5],
            repetition: None,
            array_offsets: (0..=5).collect(),
            array_mask: vec![true, true, true, true, true],
            max_definition: 0,
            is_list: false,
            is_nullable: false,
        };

        let batch_level = LevelInfo::new_from_batch(&batch);
        assert_eq!(&batch_level, &expected_batch_level);

        // calculate the list's level
        let mut levels = vec![];
        batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .for_each(|(array, field)| {
                let mut array_levels = batch_level.calculate_array_levels(array, field);
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 1);

        let list_level = levels.get(0).unwrap();

        let expected_level = LevelInfo {
            definition: vec![3, 3, 3, 0, 3, 3, 3, 3, 3, 3, 3],
            repetition: Some(vec![0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1]),
            array_offsets: vec![0, 1, 3, 3, 6, 10],
            array_mask: vec![
                true, true, true, false, true, true, true, true, true, true, true,
            ],
            max_definition: 3,
            is_list: true,
            is_nullable: true,
        };
        assert_eq!(&list_level.definition, &expected_level.definition);
        assert_eq!(&list_level.repetition, &expected_level.repetition);
        assert_eq!(&list_level.array_offsets, &expected_level.array_offsets);
        assert_eq!(&list_level.array_mask, &expected_level.array_mask);
        assert_eq!(&list_level.max_definition, &expected_level.max_definition);
        assert_eq!(&list_level.is_list, &expected_level.is_list);
        assert_eq!(&list_level.is_nullable, &expected_level.is_nullable);
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn mixed_struct_list() {
        // this tests the level generation from the equivalent arrow_writer_complex test

        // define schema
        let struct_field_d = Field::new("d", DataType::Float64, true);
        let struct_field_f = Field::new("f", DataType::Float32, true);
        let struct_field_g = Field::new(
            "g",
            DataType::List(Box::new(Field::new("items", DataType::Int16, false))),
            false,
        );
        let struct_field_e = Field::new(
            "e",
            DataType::Struct(vec![struct_field_f.clone(), struct_field_g.clone()]),
            true,
        );
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, true),
            Field::new(
                "c",
                DataType::Struct(vec![struct_field_d.clone(), struct_field_e.clone()]),
                false,
            ),
        ]);

        // create some data
        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![Some(1), None, None, Some(4), Some(5)]);
        let d = Float64Array::from(vec![None, None, None, Some(1.0), None]);
        let f = Float32Array::from(vec![Some(0.0), None, Some(333.3), None, Some(5.25)]);

        let g_value = Int16Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[1], [2, 3], null, [4, 5, 6], [7, 8, 9, 10]]
        let g_value_offsets =
            arrow::buffer::Buffer::from(&[0, 1, 3, 3, 6, 10].to_byte_slice());

        // Construct a list array from the above two
        let g_list_data = ArrayData::builder(struct_field_g.data_type().clone())
            .len(5)
            .add_buffer(g_value_offsets)
            .add_child_data(g_value.data())
            .build();
        let g = ListArray::from(g_list_data);

        let e = StructArray::from(vec![
            (struct_field_f, Arc::new(f) as ArrayRef),
            (struct_field_g, Arc::new(g) as ArrayRef),
        ]);

        let c = StructArray::from(vec![
            (struct_field_d, Arc::new(d) as ArrayRef),
            (struct_field_e, Arc::new(e) as ArrayRef),
        ]);

        // build a record batch
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b), Arc::new(c)],
        )
        .unwrap();

        //////////////////////////////////////////////
        let expected_batch_level = LevelInfo {
            definition: vec![0; 5],
            repetition: None,
            array_offsets: (0..=5).collect(),
            array_mask: vec![true, true, true, true, true],
            max_definition: 0,
            is_list: false,
            is_nullable: false,
        };

        let batch_level = LevelInfo::new_from_batch(&batch);
        assert_eq!(&batch_level, &expected_batch_level);

        // calculate the list's level
        let mut levels = vec![];
        batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .for_each(|(array, field)| {
                let mut array_levels = batch_level.calculate_array_levels(array, field);
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 5);

        // test "a" levels
        let list_level = levels.get(0).unwrap();

        let expected_level = LevelInfo {
            definition: vec![1, 1, 1, 1, 1],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, true, true, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: false,
        };
        assert_eq!(list_level, &expected_level);

        // test "b" levels
        let list_level = levels.get(1).unwrap();

        let expected_level = LevelInfo {
            definition: vec![1, 0, 0, 1, 1],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, false, false, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: true,
        };
        assert_eq!(list_level, &expected_level);

        // test "d" levels
        let list_level = levels.get(2).unwrap();

        let expected_level = LevelInfo {
            definition: vec![1, 1, 1, 2, 1],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![false, false, false, true, false],
            max_definition: 2,
            is_list: false,
            is_nullable: true,
        };
        assert_eq!(list_level, &expected_level);

        // test "f" levels
        let list_level = levels.get(3).unwrap();

        let expected_level = LevelInfo {
            definition: vec![3, 2, 3, 2, 3],
            repetition: None,
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, false, true, false, true],
            max_definition: 3,
            is_list: false,
            is_nullable: true,
        };
        assert_eq!(list_level, &expected_level);
    }

    #[test]
    fn test_filter_array_indices() {
        let level = LevelInfo {
            definition: vec![3, 3, 3, 1, 3, 3, 3],
            repetition: Some(vec![0, 1, 1, 0, 0, 1, 1]),
            array_offsets: vec![0, 3, 3, 6],
            array_mask: vec![true, true, true, false, true, true, true],
            max_definition: 3,
            is_list: true,
            is_nullable: true,
        };

        let expected = vec![0, 1, 2, 3, 4, 5];
        let filter = level.filter_array_indices();
        assert_eq!(expected, filter);
    }
}

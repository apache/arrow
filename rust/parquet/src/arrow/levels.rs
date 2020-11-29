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
//! The algorithm works by tracking the slots of an array that should ultimately be populated when
//! writing to Parquet.
//! Parquet achieves nesting through definition levels and repetition levels [1].
//! Definition levels specify how many optional fields in the part for the column are defined.
//! Repetition levels specify at what repeated field (list) in the path a column is defined.
//!
//! In a nested data structure such as `a.b.c`, one can see levels as defining whether a record is
//! defined at `a`, `a.b`, or `a.b.c`. Optional fields are nullable fields, thus if all 3 fiedls
//! are nullable, the maximum definition will be = 3.
//!
//! The algorithm in this module computes the necessary information to enable the writer to keep
//! track of which columns are at which levels, and to ultimately extract the correct values at
//! the correct slots from Arrow arrays.
//!
//! It works by walking a record batch's arrays, keeping track of what values are non-null, their
//! positions and computing what their levels are.
//! We use an eager approach that increments definition levels where incrementable, and decrements
//! if a value being checked is null.
//!
//! [1] https://github.com/apache/parquet-format#nested-encoding

use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;

/// Keeps track of the level information per array that is needed to write an Arrow aray to Parquet.
///
/// When a nested schema is traversed, intermediate [LevelInfo] structs are created to track
/// the state of parent arrays. When a primitive Arrow array is encountered, a final [LevelInfo]
/// is created, and this is what is used to index into the array when writing data to Parquet.
///
/// Note: for convenience, the final primitive array's level info can omit some values below if
/// none of that array's parents were repetitive (i.e `is_list` is false)
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct LevelInfo {
    /// Array's definition levels
    pub definition: Vec<i16>,
    /// Array's optional repetition levels
    pub repetition: Option<Vec<i16>>,
    /// Array's offsets, 64-bit is used to accommodate large offset arrays
    pub array_offsets: Vec<i64>,
    /// Array's validity mask
    ///
    /// While this looks like `definition_mask`, they serve different purposes.
    /// This mask is for the immediate array, while the `definition_mask` tracks
    /// the cumulative effect of all masks from the root (batch) to the current array.
    pub array_mask: Vec<bool>,
    /// Definition mask, to indicate null ListArray slots that should be skipped
    pub definition_mask: Vec<(bool, i16)>,
    /// The maximum definition at this level, 1 at the record batch
    pub max_definition: i16,
    /// Whether this array or any of its parents is a list, in which case the
    /// `definition_mask` would be used to index correctly into list children.
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
            // a batch is treated as all-defined
            definition: vec![1; num_rows],
            // a batch has no repetition as it is not a list
            repetition: None,
            // all values of a batch as deemed to be defined at level 1
            definition_mask: vec![(true, 1); num_rows],
            // a batch has sequential offsets, should be num_rows + 1
            array_offsets: (0..=(num_rows as i64)).collect(),
            // all values at a batch-level are non-null
            array_mask: vec![true; num_rows],
            max_definition: 1,
            is_list: false,
            // a batch is treated as nullable even though it has no nulls,
            // this is required to compute nested type levels correctly
            is_nullable: true,
        }
    }

    /// Compute nested levels of the Arrow array, recursing into lists and structs.
    ///
    /// Returns a list of `LevelInfo`, where each level is for nested primitive arrays.
    ///
    /// The algorithm works by eagerly incrementing non-null values, and decrementing
    /// when a value is null.
    ///
    /// *Examples:*
    ///
    /// A record batch always starts at a populated definition = level 1.
    /// When a batch only has a primitive, i.e. `<batch<primitive[a]>>, column `a`
    /// can only have a maximum level of 1 if it is not null.
    /// If it is null, we decrement by 1, such that the null slots will = level 0.
    ///
    /// If a batch has nested arrays (list, struct, union, etc.), then the incrementing
    /// takes place.
    /// A `<batch<struct[a]<primitive[b]>>` will have up to 2 levels (if nullable).
    /// When calculating levels for `a`, we start with level 1 from the batch,
    /// then if the struct slot is not empty, we increment by 1, such that we'd have `[2, 2, 2]`
    /// if all 3 slots are not null.
    /// If there is an empty slot, we decrement, leaving us with `[2, 0 (1-1), 2]` as the
    /// null slot effectively means that no record is populated for the row altogether.
    ///
    /// When we encounter `b` which is primitive, we check if the supplied definition levels
    /// equal the maximum level (i.e. level = 2). If the level < 2, then the parent of the
    /// primitive (`a`) is already null, and `b` is kept as null.
    /// If the level == 2, then we check if `b`'s slot is null, decrementing if it is null.
    /// Thus we could have a final definition as: `[2, 0, 1]` indicating that only the first
    /// slot is populated for `a.b`, the second one is all null, and only `a` has a value on the last.
    ///
    /// If expressed as JSON, this would be:
    ///
    /// ```json
    /// {"a": {"b": 1}}
    /// {"a": null}
    /// {"a": {"b": null}}
    /// ```
    ///
    /// *Lists*
    ///
    /// TODO
    ///
    /// *Non-nullable arrays*
    ///
    /// If an array is non-nullable, this is accounted for when converting the Arrow schema to a
    /// Parquet schema.
    /// When dealing with `<batch<primitive[_]>>` there is no issue, as the maximum
    /// level will always be = 1.
    ///
    /// When dealing with nested types, the logic becomes a bit complicated.
    /// A non-nullable struct; `<batch<struct{non-null}[a]<primitive[b]>>>` will only
    /// have 1 maximum level, where 0 means `b` is null, and 1 means `b` is not null.
    ///
    /// We account for the above by checking if the `Field` is nullable, and adjusting
    /// the `level` variable to determine which level the next child should increment or
    /// decrement from.
    pub(crate) fn calculate_array_levels(
        &self,
        array: &ArrayRef,
        field: &Field,
        level: i16,
    ) -> Vec<Self> {
        match array.data_type() {
            DataType::Null => vec![Self {
                definition: self.definition.iter().map(|d| (d - 1).max(0)).collect(),
                repetition: self.repetition.clone(),
                definition_mask: self.definition_mask.clone(),
                array_offsets: self.array_offsets.clone(),
                array_mask: self.array_mask.clone(),
                // nulls will have all definitions being 0, so max value is reduced
                max_definition: level - 1,
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
                // it is safe to inherit the parent level's repetition, but we have to calculate
                // the child's own definition levels
                vec![Self {
                    definition: self.get_primitive_def_levels(array, field),
                    // TODO: if we change this when working on lists, then update the above comment
                    repetition: self.repetition.clone(),
                    definition_mask: self.definition_mask.clone(),
                    array_offsets: self.array_offsets.clone(),
                    array_mask: self.array_mask.clone(),
                    is_list: self.is_list,
                    // if the current value is non-null, but it's a child of another, we reduce
                    // the max definition to indicate that all its applicable values can be taken
                    max_definition: level - ((!field.is_nullable() && level > 1) as i16),
                    is_nullable: field.is_nullable(),
                }]
            }
            DataType::FixedSizeBinary(_) => unimplemented!(),
            DataType::Decimal(_, _) => unimplemented!(),
            DataType::List(_list_field) | DataType::LargeList(_list_field) => {
                // TODO: ARROW-10766, it is better to not write lists at all until they are correct
                todo!("List writing not yet implemented, see ARROW-10766")
            }
            DataType::FixedSizeList(_, _) => unimplemented!(),
            DataType::Struct(struct_fields) => {
                let struct_array: &StructArray = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Unable to get struct array");
                let array_len = struct_array.len();
                let mut struct_def_levels = Vec::with_capacity(array_len);
                let mut struct_mask = Vec::with_capacity(array_len);
                // we can have a <struct<struct<_>>, in which case we should check
                // the parent struct in the child struct's offsets
                for (i, def_level) in self.definition.iter().enumerate() {
                    if *def_level == level {
                        if !field.is_nullable() {
                            // if the field is non-nullable and current definition = parent,
                            // then we should neither increment nor decrement the level
                            struct_def_levels.push(level);
                        } else if struct_array.is_valid(i) {
                            // Increment to indicate that this value is not null
                            // The next level will decrement if it is null
                            struct_def_levels.push(level + 1);
                        } else {
                            // decrement to show that only the previous level is populated
                            // we only decrement if previous field is nullable because if it
                            // was not nullable, we can't decrement beyond its level
                            struct_def_levels.push(level - (self.is_nullable as i16));
                        }
                    } else {
                        // this means that the previous level's slot was null, so we preserve it
                        struct_def_levels.push(*def_level);
                    }
                    // TODO: is it more efficient to use `bitvec` here?
                    struct_mask.push(struct_array.is_valid(i));
                }
                // create levels for struct's fields, we accumulate them in this vec
                let mut struct_levels = vec![];
                let struct_level_info = Self {
                    definition: struct_def_levels,
                    // inherit the parent's repetition
                    repetition: self.repetition.clone(),
                    // Is it correct to increment this by 1 level?
                    definition_mask: self
                        .definition_mask
                        .iter()
                        .map(|(state, index)| (*state, index + 1))
                        .collect(),
                    // logically, a struct should inherit its parent's offsets
                    array_offsets: self.array_offsets.clone(),
                    // this should be just the struct's mask, not its parent's
                    array_mask: struct_mask,
                    max_definition: self.max_definition + (field.is_nullable() as i16),
                    is_list: self.is_list,
                    is_nullable: field.is_nullable(),
                };
                struct_array
                    .columns()
                    .into_iter()
                    .zip(struct_fields)
                    .for_each(|(col, struct_field)| {
                        let mut levels = struct_level_info.calculate_array_levels(
                            col,
                            struct_field,
                            level + (field.is_nullable() as i16),
                        );
                        struct_levels.append(&mut levels);
                    });
                struct_levels
            }
            DataType::Union(_) => unimplemented!(),
            DataType::Dictionary(_, _) => {
                // Need to check for these cases not implemented in C++:
                // - "Writing DictionaryArray with nested dictionary type not yet supported"
                // - "Writing DictionaryArray with null encoded in dictionary type not yet supported"
                vec![Self {
                    definition: self.get_primitive_def_levels(array, field),
                    repetition: self.repetition.clone(),
                    definition_mask: self.definition_mask.clone(),
                    array_offsets: self.array_offsets.clone(),
                    array_mask: self.array_mask.clone(),
                    is_list: self.is_list,
                    max_definition: level,
                    is_nullable: field.is_nullable(),
                }]
            }
        }
    }

    /// Get the definition levels of the numeric array, with level 0 being null and 1 being not null
    /// In the case where the array in question is a child of either a list or struct, the levels
    /// are incremented in accordance with the `level` parameter.
    /// Parent levels are either 0 or 1, and are used to higher (correct terminology?) leaves as null
    fn get_primitive_def_levels(&self, array: &ArrayRef, field: &Field) -> Vec<i16> {
        let mut array_index = 0;
        let max_def_level = self.definition.iter().max().unwrap();
        let mut primitive_def_levels = vec![];
        self.definition.iter().for_each(|def_level| {
            if !field.is_nullable() && *max_def_level > 1 {
                primitive_def_levels.push(*def_level - 1);
                array_index += 1;
            } else if def_level < max_def_level {
                primitive_def_levels.push(*def_level);
                array_index += 1;
            } else {
                primitive_def_levels.push(def_level - array.is_null(array_index) as i16);
                array_index += 1;
            }
        });
        primitive_def_levels
    }
}

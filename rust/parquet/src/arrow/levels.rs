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

use arrow::array::{make_array, Array, ArrayRef, StructArray};
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
        // TODO: we need the array mask of the child, which we should AND with the parent
        let (_, array_mask) = Self::get_array_offsets_and_masks(array);
        match array.data_type() {
            DataType::Null => vec![Self {
                definition: self.definition.iter().map(|d| (d - 1).max(0)).collect(),
                repetition: self.repetition.clone(),
                definition_mask: self.definition_mask.clone(),
                array_offsets: self.array_offsets.clone(),
                array_mask,
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
                // vec![Self {
                //     definition: ,
                //     // TODO: if we change this when working on lists, then update the above comment
                //     repetition: self.repetition.clone(),
                //     definition_mask: self.definition_mask.clone(),
                //     array_offsets: self.array_offsets.clone(),
                //     array_mask: self.array_mask.clone(),
                //     is_list: self.is_list,
                //     // if the current value is non-null, but it's a child of another, we reduce
                //     // the max definition to indicate that all its applicable values can be taken
                //     max_definition: level - ((!field.is_nullable() && level > 1) as i16),
                //     is_nullable: field.is_nullable(),
                // }]
                vec![self.get_primitive_def_levels(array, field, array_mask)]
            }
            DataType::FixedSizeBinary(_) => unimplemented!(),
            DataType::Decimal(_, _) => unimplemented!(),
            DataType::List(list_field) | DataType::LargeList(list_field) => {
                let array_data = array.data();
                let child_data = array_data.child_data().get(0).unwrap();
                // // get list offsets
                let (offsets, mask) = Self::get_array_offsets_and_masks(array);
                let child_array = make_array(child_data.clone());
                let (_, child_mask) = Self::get_array_offsets_and_masks(&child_array);

                // TODO: (21-12-2020), I got a thought that this might be duplicating
                // what the primitive levels do. Does it make sense to calculate both?
                let list_level = self.calculate_list_child_levels(
                    offsets,
                    mask,
                    true,
                    field.is_nullable(),
                    level + 1,
                );

                // if datatype is a primitive, we can construct levels of the child array
                match child_array.data_type() {
                    // TODO: The behaviour of a <list<null>> is untested
                    DataType::Null => vec![Self {
                        definition: list_level
                            .definition
                            .iter()
                            .map(|d| (d - 1).max(0))
                            .collect(),
                        repetition: list_level.repetition.clone(),
                        definition_mask: list_level.definition_mask.clone(),
                        array_offsets: list_level.array_offsets.clone(),
                        array_mask: list_level.array_mask.clone(),
                        // nulls will have all definitions being 0, so max value is reduced
                        max_definition: level,
                        is_list: true,
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
                    | DataType::Timestamp(_, _)
                    | DataType::Date32(_)
                    | DataType::Date64(_)
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Duration(_)
                    | DataType::Interval(_) => {
                        // vec![Self {
                        //     definition: list_level
                        //         .get_primitive_def_levels(&child_array, list_field),
                        //     // TODO: if we change this when working on lists, then update the above comment
                        //     repetition: list_level.repetition.clone(),
                        //     definition_mask: list_level.definition_mask.clone(),
                        //     array_offsets: list_level.array_offsets.clone(),
                        //     array_mask: list_level.array_mask,
                        //     is_list: true,
                        //     // if the current value is non-null, but it's a child of another, we reduce
                        //     // the max definition to indicate that all its applicable values can be taken
                        //     max_definition: level + 1,
                        //     is_nullable: list_field.is_nullable(),
                        // }]
                        vec![list_level.get_primitive_def_levels(
                            &child_array,
                            list_field,
                            child_mask,
                        )]
                    }
                    DataType::Binary | DataType::Utf8 | DataType::LargeUtf8 => {
                        unimplemented!()
                    }
                    DataType::FixedSizeBinary(_) => unimplemented!(),
                    DataType::Decimal(_, _) => unimplemented!(),
                    DataType::LargeBinary => unimplemented!(),
                    DataType::List(_) | DataType::LargeList(_) => {
                        // TODO: nested list
                        unimplemented!()
                    }
                    DataType::FixedSizeList(_, _) => unimplemented!(),
                    DataType::Struct(_) => list_level.calculate_array_levels(
                        &child_array,
                        list_field,
                        level + (field.is_nullable() as i16),
                    ),
                    DataType::Union(_) => unimplemented!(),
                    DataType::Dictionary(_, _) => unimplemented!(),
                }
            }
            DataType::FixedSizeList(_, _) => unimplemented!(),
            DataType::Struct(struct_fields) => {
                let struct_array: &StructArray = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Unable to get struct array");
                let array_len = struct_array.len();
                let mut struct_def_levels = Vec::with_capacity(array_len);
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
                    array_mask: self
                        .array_mask
                        .iter()
                        .zip(array_mask)
                        .map(|(a, b)| *a && b)
                        .collect(),
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
                // vec![Self {
                //     definition: self.get_primitive_def_levels(array, field),
                //     repetition: self.repetition.clone(),
                //     definition_mask: self.definition_mask.clone(),
                //     array_offsets: self.array_offsets.clone(),
                //     array_mask: self.array_mask.clone(),
                //     is_list: self.is_list,
                //     max_definition: level,
                //     is_nullable: field.is_nullable(),
                // }]
                vec![self.get_primitive_def_levels(array, field, array_mask)]
            }
        }
    }

    /// Get the definition levels of the numeric array, with level 0 being null and 1 being not null
    /// In the case where the array in question is a child of either a list or struct, the levels
    /// are incremented in accordance with the `level` parameter.
    /// Parent levels are either 0 or 1, and are used to higher (correct terminology?) leaves as null
    fn get_primitive_def_levels(
        &self,
        array: &ArrayRef,
        field: &Field,
        array_mask: Vec<bool>,
    ) -> Self {
        debug_assert_eq!(array.data_type(), field.data_type());
        let mut array_index = 0;
        let max_def_level = self.definition.iter().max().unwrap();
        debug_assert_eq!(*max_def_level, self.max_definition);
        let mut primitive_def_levels = vec![];
        // TODO: if we end up not needing to change definitions, rather clone the array
        let mut definition_mask = vec![];
        let mut merged_mask: Vec<bool> = vec![];
        let mut array_mask_index = 0;
        self.definition.iter().zip(&self.definition_mask).for_each(
            |(def_level, mask)| {
                // append to mask to account for null list values not represented in child
                let is_valid = if mask.0 && mask.1 >= *max_def_level {
                    array_mask_index += 1;
                    mask.0 && array_mask[array_mask_index - 1]
                } else {
                    false
                };
                merged_mask.push(is_valid);
                if !field.is_nullable() && *max_def_level > 1 {
                    primitive_def_levels.push(*def_level - 1);
                    definition_mask.push((is_valid, mask.1));
                    array_index += 1;
                } else if def_level < max_def_level {
                    primitive_def_levels.push(*def_level);
                    definition_mask.push(*mask);
                    array_index += 1;
                } else {
                    primitive_def_levels
                        .push(def_level - array.is_null(array_index) as i16);
                    definition_mask.push((is_valid, mask.1));
                    array_index += 1;
                }
            },
        );
        Self {
            definition: primitive_def_levels,
            repetition: self.repetition.clone(),
            array_offsets: self.array_offsets.clone(),
            array_mask: merged_mask,
            definition_mask,
            max_definition: self.max_definition,
            is_list: self.is_list,
            is_nullable: field.is_nullable(),
        }
    }

    /// This is the actual algorithm that computes the levels based on the array's characteristics.
    fn calculate_list_child_levels(
        &self,
        // we use 64-bit offsets to also accommodate large arrays
        array_offsets: Vec<i64>,
        array_mask: Vec<bool>,
        is_list: bool,
        is_nullable: bool,
        current_def_level: i16,
    ) -> Self {
        let mut definition = vec![];
        let mut repetition = vec![];
        let mut definition_mask = vec![];
        let has_repetition = self.is_list || is_list;
        let mut merged_array_mask = vec![];

        // keep track of parent definition nulls seen through the definition_mask
        let mut nulls_seen = 0;

        // we use this index to determine if a repetition should be populated based
        // on its definition at the index. It needs to be outside of the loop
        let mut def_index = 0;

        // Index into offsets ([0, 1], [1, 3], [3, 3], ...) to get the array slot's length.
        // If we are dealing with a list, or a descendant of a list, values could be 0 or many
        //
        // A list that has no empty slots should return the same slots as its offsets,
        // plus an accumulation of parent list slots that are empty.
        self.array_offsets
            .windows(2)
            .enumerate()
            .for_each(|(w_index, w)| {
                // get the index of the start (from) and end (to)
                let from = w[0] as usize;
                let to = w[1] as usize;
                let parent_len = to - from;
                let is_parent_valid = self.array_mask[w_index];
                let is_child_valid = array_mask[w_index];
                let is_valid = is_parent_valid && is_child_valid;
                let parent_mask = self.definition_mask[w_index];

                // if the parent is null, the slots in the child do not matter, we have a null
                if !is_parent_valid {
                    definition.push(parent_mask.1 - 1);
                    repetition.push(0);
                    definition_mask.push(parent_mask);
                    if parent_len > 0 {
                        merged_array_mask.push(is_valid);
                    }
                    // we can only extend nulls if we're dealing with lists
                    if self.is_list || is_list {
                        nulls_seen += 1;
                    }
                } else {
                    // If the parent slot is empty, fill it once to show the nullness.
                    // There is an edge-case where this child slot's parent is null, in which case we should
                    // inherit the parent's levels instead of creating them at this level
                    if parent_len == 0 {
                        // increase the def_index so we don't index incorrectly when computing repetition
                        def_index += 1;
                        merged_array_mask.push(is_valid);
                        // check if the parent is null
                        if !parent_mask.0 {
                            // we subtract 1 because we want the first level that was null, which will be
                            // the level before we had to set the mask as null
                            definition.push(parent_mask.1 - 1);
                            repetition.push(0);
                            definition_mask.push(parent_mask);
                        } else {
                            // reflect a null slot at current level
                            definition.push(self.max_definition);
                            repetition.push(0);
                            definition_mask.push((false, current_def_level));
                        }
                    }

                    // If it's not empty, iterate through the values, checking if they should be null because
                    // of any null prior parents (using self.definition_mask)
                    (from..to).for_each(|index| {
                        // if the parent definition mask is false, the array slots must be false too
                        let mask = array_mask[index];
                        let array_from = array_offsets[index];
                        let array_to = array_offsets[index + 1];
                        merged_array_mask.push(is_valid);

                        let parent_def_level = &self.definition[index + nulls_seen];

                        // if array_len == 0, the child is null
                        let array_len = array_to - array_from;

                        // compute the definition level
                        // what happens if array's len is 0?
                        if array_len == 0 {
                            definition.push(self.max_definition - !is_child_valid as i16);
                            repetition.push(0); // TODO: validate that this is 0 for deeply nested lists
                            definition_mask.push((false, current_def_level));
                            // increase the def_index so we don't index incorrectly when computing repetition
                            def_index += 1;
                        }
                        (array_from..array_to).for_each(|_| {
                            if !parent_mask.0 {
                                definition.push(self.definition[w_index]);
                                // repetition.push(1); // TODO: should this be 0?
                                definition_mask.push(parent_mask);
                            } else {
                                definition.push(
                                    if *parent_def_level == self.max_definition {
                                        // TODO: haven't validated this in deeply-nested lists
                                        self.max_definition + mask as i16
                                    } else {
                                        *parent_def_level
                                    },
                                );
                                definition_mask.push((true, current_def_level));
                            }
                        });

                        if has_repetition && array_len > 0 {
                            // compute the repetition level

                            match &self.repetition {
                                Some(rep) => {
                                    // make index mutable so we can traverse the parent with it
                                    let max_rep = rep.iter().max().cloned().unwrap_or(0);
                                    let parent_rep = rep[index];
                                    // we check if we are seeing the first value of the parent
                                    if index == from {
                                        repetition.push(0); // was parent_rep
                                        def_index += 1;
                                        (1..array_len).for_each(|_| {
                                            repetition.push({
                                                if parent_rep == max_rep {
                                                    parent_rep + 1
                                                } else {
                                                    parent_rep + 2
                                                }
                                            }); // was parent_rep + 1
                                            def_index += 1;
                                        });
                                    } else {
                                        repetition.push(1);
                                        def_index += 1;
                                        (1..array_len).for_each(|_| {
                                            repetition.push(if parent_rep == max_rep {
                                                parent_rep + 1
                                            } else {
                                                parent_rep + 2
                                            }); // was parent_rep + 1
                                            def_index += 1;
                                        });
                                    }
                                }
                                None => {
                                    if definition[def_index] == current_def_level {
                                        repetition.push(0);
                                        def_index += 1;
                                        (1..array_len).for_each(|_| {
                                            repetition.push(1); // was parent_rep + 1
                                            def_index += 1;
                                        });
                                    } else {
                                        repetition.push(0);
                                        def_index += 1;
                                        (1..array_len).for_each(|_| {
                                            repetition.push(1); // was parent_rep + 1
                                            def_index += 1;
                                        });
                                    }
                                }
                            }
                        }
                    });
                }
            });

        Self {
            definition,
            repetition: if !has_repetition {
                None
            } else {
                Some(repetition)
            },
            definition_mask,
            array_mask: merged_array_mask,
            array_offsets,
            is_list: has_repetition,
            max_definition: current_def_level,
            is_nullable,
        }
    }

    /// Get the offsets of an array as 64-bit values, and validity masks as booleans
    /// - Primitive, binary and struct arrays' offsets will be a sequence, masks obtained from validity bitmap
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
}

/// Convert an Arrow buffer to a boolean array slice
/// TODO: this was created for buffers, so might not work for bool array, might be slow too
#[inline]
fn get_bool_array_slice(
    buffer: &arrow::buffer::Buffer,
    offset: usize,
    len: usize,
) -> Vec<bool> {
    let data = buffer.data();
    (offset..(len + offset))
        .map(|i| arrow::util::bit_util::get_bit(data, i))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::ListArray,
        array::{ArrayData, Int32Array},
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
            definition_mask: vec![(true, 1), (true, 1)],
            array_offsets: vec![0, 1, 2], // 2 records, root offsets always sequential
            array_mask: vec![true, true], // both lists defined
            max_definition: 0, // at the root, set to 0 (only works in this example, we start at 1 with Arrow data)
            is_list: false,    // root is never list
            is_nullable: false, // root in example is non-nullable
        };
        // offset into array, each level1 has 2 values
        let array_offsets = vec![0, 2, 4];
        let array_mask = vec![true, true];

        // calculate level1 levels
        let levels = parent_levels.calculate_list_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            true,
            false,
            1,
        );
        //
        let expected_levels = LevelInfo {
            definition: vec![1, 1, 1, 1],
            repetition: Some(vec![0, 1, 0, 1]),
            definition_mask: vec![(true, 1), (true, 1), (true, 1), (true, 1)],
            array_offsets,
            array_mask,
            max_definition: 1,
            is_list: true,
            is_nullable: false,
        };
        // the separate asserts make it easier to see what's failing
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.definition_mask, &expected_levels.definition_mask);
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
        let levels = parent_levels.calculate_list_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            true,
            false,
            2,
        );
        let expected_levels = LevelInfo {
            definition: vec![2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
            repetition: Some(vec![0, 2, 2, 1, 2, 2, 2, 0, 1, 2]),
            definition_mask: vec![
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
            ],
            array_offsets,
            array_mask,
            max_definition: 2,
            is_list: true,
            is_nullable: false,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.definition_mask, &expected_levels.definition_mask);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.is_list, &expected_levels.is_list);
        assert_eq!(&levels.is_nullable, &expected_levels.is_nullable);
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_one_level_1() {
        // This test calculates the levels for a non-null primitive array
        let parent_levels = LevelInfo {
            definition: vec![1; 10],
            repetition: None,
            definition_mask: vec![(true, 1); 10],
            array_offsets: (0..=10).collect(),
            array_mask: vec![true; 10],
            max_definition: 1,
            is_list: false,
            is_nullable: false,
        };
        let array_offsets: Vec<i64> = (0..=10).collect();
        let array_mask = vec![true; 10];

        let levels = parent_levels.calculate_list_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            false,
            false,
            2,
        );
        let expected_levels = LevelInfo {
            definition: vec![2; 10],
            repetition: None,
            definition_mask: vec![(true, 2); 10],
            array_offsets,
            array_mask,
            max_definition: 2,
            is_list: false,
            is_nullable: false,
        };
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_one_level_2() {
        // This test calculates the levels for a non-null primitive array
        let parent_levels = LevelInfo {
            definition: vec![1; 5],
            repetition: None,
            definition_mask: vec![(true, 1), (true, 1), (true, 1), (true, 1), (true, 1)],
            array_offsets: (0..=5).collect(),
            array_mask: vec![true, true, true, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: false,
        };
        let array_offsets: Vec<i64> = (0..=5).collect();
        let array_mask = vec![true, false, true, true, false];

        let levels = parent_levels.calculate_list_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            false,
            false,
            2,
        );
        let expected_levels = LevelInfo {
            definition: vec![2, 1, 2, 2, 1],
            repetition: None,
            definition_mask: vec![(true, 2); 5],
            array_offsets,
            array_mask,
            max_definition: 2,
            is_list: false,
            is_nullable: false,
        };
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    fn test_calculate_array_levels_1() {
        // if all array values are defined (e.g. batch<list<_>>)
        // [[0], [1], [2], [3], [4]]
        let parent_levels = LevelInfo {
            definition: vec![1; 5],
            repetition: None,
            definition_mask: vec![(true, 1), (true, 1), (true, 1), (true, 1), (true, 1)],
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, true, true, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: false,
        };
        let array_offsets = vec![0, 2, 2, 4, 8, 11];
        let array_mask = vec![true, false, true, true, true];

        let levels = parent_levels.calculate_list_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            true,
            false,
            2,
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
            definition_mask: vec![
                (true, 2),
                (true, 2),
                (false, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
            ],
            array_offsets,
            array_mask,
            max_definition: 2,
            is_list: true,
            is_nullable: false,
        };
        assert_eq!(levels, expected_levels);
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
            definition_mask: vec![
                (false, 1),
                (true, 1),
                (false, 1),
                (true, 1),
                (true, 1),
            ],
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![false, true, false, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: true,
        };
        let array_offsets = vec![0, 2, 2, 4, 8, 11];
        let array_mask = vec![true, false, true, true, true];

        let levels = parent_levels.calculate_list_child_levels(
            array_offsets.clone(),
            array_mask,
            true,
            true,
            2,
        );
        let expected_levels = LevelInfo {
            // 0 1 [2] are 0 (not defined at level 1)
            // [2] is 1, but has 0 slots so is not populated (defined at level 1 only)
            // 2 3 [4] are 0
            // 4 5 6 7 [8] are 1 (defined at level 1 only)
            // 8 9 10 [11] are 2 (defined at both levels)
            definition: vec![0, 0, 1, 0, 0, 2, 2, 2, 2, 2, 2, 2],
            repetition: Some(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            definition_mask: vec![
                (false, 1),
                (false, 1),
                (false, 2),
                (false, 1),
                (false, 1),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
            ],
            array_offsets,
            array_mask: vec![false, false, false, true, true],
            max_definition: 2,
            is_nullable: true,
            is_list: true,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.definition_mask, &expected_levels.definition_mask);
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
        let levels = nested_parent_levels.calculate_list_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            true,
            true,
            3,
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
                0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            ],
            // TODO: this doesn't feel right, needs some validation
            repetition: Some(vec![
                0, 0, 0, 0, 0i16, 0, 0, 0, 0, 0, 3, 1, 3, 1, 3, 1, 3, 0, 3, 1, 3, 1, 3,
            ]),
            definition_mask: vec![
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
                (false, 0),
            ],
            array_offsets,
            array_mask,
            max_definition: 3,
            is_nullable: true,
            is_list: true,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.definition_mask, &expected_levels.definition_mask);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
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
            definition_mask: vec![(true, 1), (true, 1), (true, 1), (true, 1)],
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

        let levels = parent_levels.calculate_list_child_levels(
            array_offsets.clone(),
            array_mask,
            true,
            true,
            2,
        );
        // 0: [null], level 1 is defined, but not 2
        // 1: [1, 2, 3]
        // 2: [4, 5]
        // 3: [6, 7]
        let expected_levels = LevelInfo {
            definition: vec![1, 2, 2, 2, 2, 2, 2, 2],
            repetition: Some(vec![0, 0, 1, 1, 0, 1, 0, 1]),
            definition_mask: vec![(true, 2); 8],
            array_offsets,
            array_mask: vec![false, true, true, true],
            max_definition: 2,
            is_list: true,
            is_nullable: true,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.definition_mask, &expected_levels.definition_mask);
        assert_eq!(&levels.array_offsets, &expected_levels.array_offsets);
        assert_eq!(&levels.max_definition, &expected_levels.max_definition);
        assert_eq!(&levels.is_list, &expected_levels.is_list);
        assert_eq!(&levels.is_nullable, &expected_levels.is_nullable);
        assert_eq!(&levels, &expected_levels);

        // nested lists (using previous test)
        let nested_parent_levels = levels;
        // 0: [201]
        // 1: [202, 203]
        // 2: null ([])
        // 3: [204, 205, 206]
        // 4: [207, 208, 209, 210]
        // 5: [] (tests a non-null empty list slot)
        // 6: [211, 212, 213, 214, 215]
        let array_offsets = vec![0, 1, 3, 3, 6, 10, 10, 15];
        let array_mask = vec![true, true, false, true, true, true, true];
        let levels = nested_parent_levels.calculate_list_child_levels(
            array_offsets,
            array_mask,
            true,
            true,
            3,
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
            definition: vec![1, 3, 3, 3, 2, 3, 3, 3, 3, 3, 3, 3, 2, 3, 3, 3, 3, 3],
            // TODO: 2020/12/05 ended here
            // TODO: have a suspicion that this is missing an increment (i.e. some should be + 1)
            repetition: Some(vec![0, 0, 1, 2, 0, 0, 2, 2, 1, 2, 2, 2, 0, 1, 2, 2, 2, 2]),
            definition_mask: vec![
                (false, 2),
                (true, 3),
                (true, 3),
                (true, 3),
                (false, 3),
                (true, 3),
                (true, 3),
                (true, 3),
                (true, 3),
                (true, 3),
                (true, 3),
                (true, 3),
                (false, 3),
                (true, 3),
                (true, 3),
                (true, 3),
                (true, 3),
                (true, 3),
            ],
            array_mask: vec![true, true, false, true, true, true, true],
            array_offsets: vec![0, 1, 3, 3, 6, 10, 10, 15],
            is_list: true,
            is_nullable: true,
            max_definition: 3,
        };
        assert_eq!(&levels.definition, &expected_levels.definition);
        assert_eq!(&levels.repetition, &expected_levels.repetition);
        assert_eq!(&levels.definition_mask, &expected_levels.definition_mask);
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
            // should all be true if we haven't encountered a list
            definition_mask: vec![(true, 1); 6],
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
            definition_mask: vec![
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 1),
                (true, 2),
            ],
            array_offsets: (0..=6).collect(),
            array_mask: vec![true, true, true, false, false, true],
            max_definition: 2,
            is_list: false,
            is_nullable: true,
        };
        let b_levels = a_levels.calculate_list_child_levels(
            b_offsets.clone(),
            b_mask,
            false,
            true,
            2,
        );
        assert_eq!(&b_expected_levels, &b_levels);

        // c's offset and mask
        let c_offsets = b_offsets;
        let c_mask = vec![true, false, true, false, false, true];
        // c's expected levels
        let c_expected_levels = LevelInfo {
            definition: vec![3, 2, 3, 1, 0, 3],
            repetition: None,
            definition_mask: vec![
                (true, 3),
                (true, 3),
                (true, 3),
                (true, 2),
                (true, 1),
                (true, 3),
            ],
            array_offsets: c_offsets.clone(),
            array_mask: vec![true, false, true, false, false, true],
            max_definition: 3,
            is_list: false,
            is_nullable: true,
        };
        let c_levels =
            b_levels.calculate_list_child_levels(c_offsets, c_mask, false, true, 3);
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
            definition: vec![1, 1, 1, 1, 1],
            repetition: None,
            definition_mask: vec![(true, 1); 5],
            array_offsets: (0..=5).collect(),
            array_mask: vec![true, true, true, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: true,
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
                let mut array_levels =
                    batch_level.calculate_array_levels(array, field, 1);
                levels.append(&mut array_levels);
            });
        assert_eq!(levels.len(), 1);

        let list_level = levels.get(0).unwrap();

        let expected_level = LevelInfo {
            definition: vec![2, 2, 2, 0, 2, 2, 2, 2, 2, 2, 2],
            repetition: Some(vec![0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1]),
            definition_mask: vec![
                (true, 2),
                (true, 2),
                (true, 2),
                (false, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
            ],
            array_offsets: vec![0, 1, 3, 3, 6, 10],
            array_mask: vec![
                true, true, true, false, true, true, true, true, true, true, true,
            ],
            max_definition: 2,
            is_list: true,
            is_nullable: true,
        };
        assert_eq!(&list_level.definition, &expected_level.definition);
        assert_eq!(&list_level.repetition, &expected_level.repetition);
        assert_eq!(&list_level.definition_mask, &expected_level.definition_mask);
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
            // Field::new(
            //     "c",
            //     DataType::Struct(vec![struct_field_d.clone(), struct_field_e.clone()]),
            //     false,
            // ),
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
            definition: vec![1, 1, 1, 1, 1],
            repetition: None,
            definition_mask: vec![(true, 1); 5],
            array_offsets: (0..=5).collect(),
            array_mask: vec![true, true, true, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: true,
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
                let mut array_levels =
                    batch_level.calculate_array_levels(array, field, 1);
                levels.append(&mut array_levels);
            });
        // assert_eq!(levels.len(), 5);

        // test "a" levels
        let list_level = levels.get(0).unwrap();

        let expected_level = LevelInfo {
            definition: vec![1, 1, 1, 1, 1],
            repetition: None,
            definition_mask: vec![(true, 1), (true, 1), (true, 1), (true, 1), (true, 1)],
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
            definition_mask: vec![
                (true, 1),
                (false, 1),
                (false, 1),
                (true, 1),
                (true, 1),
            ],
            array_offsets: vec![0, 1, 2, 3, 4, 5],
            array_mask: vec![true, false, false, true, true],
            max_definition: 1,
            is_list: false,
            is_nullable: true,
        };
        assert_eq!(list_level, &expected_level);

        todo!("levels for arrays 3-5 not yet tested")
    }
}

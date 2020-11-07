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

//! Contains the logic for computing definition and repetition levels

#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct LevelInfo {
    /// Array's definition levels
    pub definition: Vec<i16>,
    /// Array's optional repetition levels
    pub repetition: Option<Vec<i16>>,
    /// Definition mask, to indicate null ListArray slots that should be skipped
    pub definition_mask: Vec<(bool, i16)>,
    /// Array's offsets, 64-bit is used to accommodate large offset arrays
    pub array_offsets: Vec<i64>,
    /// Array's validity mask
    pub array_mask: Vec<bool>,
    /// The maximum definition at this level, 0 at the root (record batch) [TODO: the 0 might be inaccurate]
    pub max_definition: i16,
    /// Whether this array or any of its parents is a list
    pub is_list: bool,
    /// Whether the array is nullable (affects definition levels)
    pub is_nullable: bool,
}

impl LevelInfo {
    fn calculate_child_levels(
        &self,
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

        // keep track of parent definition nulls seen through the definition_mask
        let mut nulls_seen = 0;

        // push any initial array slots that are null
        while !self.definition_mask[nulls_seen].0
            && self.definition_mask[nulls_seen].1 + 2 < current_def_level
        {
            definition_mask.push(self.definition_mask[nulls_seen]);
            definition.push(self.definition[nulls_seen]);
            repetition.push(0); // TODO is it always 0?
            nulls_seen += 1;
            println!("Definition length e: {}", definition.len());
        }

        // we use this index to determine if a repetition should be populated based
        // on its definition at the index. It needs to be outside of the loop
        let mut def_index = 0;

        self.array_offsets.windows(2).for_each(|w| {
        // the parent's index allows us to iterate through its offsets and the child's
        let from = w[0] as usize;
        let to = w[1] as usize;
        // dbg!((from, to));
        // if the parent slot is empty, fill it once to show the nullness
        if from == to {
            definition.push(self.max_definition - 1);
            repetition.push(0);
            definition_mask.push((false, self.max_definition - 1));
            println!("Definition length d: {}", definition.len());
        }

        (from..to).for_each(|index| {
            println!(
                "Array level: {}, parent offset: {}",
                current_def_level, index
            );
            let parent_mask = &self.definition_mask[index + nulls_seen];
            // TODO: this might need to be < instead of ==, but we generate duplicates in that case
            if !parent_mask.0 && parent_mask.1 == current_def_level {
                println!("Parent mask c: {:?}", parent_mask);
                nulls_seen += 1;
                definition.push(self.max_definition);
                repetition.push(1);
                definition_mask.push(*parent_mask);
                println!("Definition length c: {}", definition.len());
            }
            let mask = array_mask[index];
            let array_from = array_offsets[index];
            let array_to = array_offsets[index + 1];

            let parent_def_level = &self.definition[index + nulls_seen];

            // if array_len == 0, the child is null
            let array_len = array_to - array_from;

            // compute the definition level
            // what happens if array's len is 0?
            if array_len == 0 {
                definition.push(self.max_definition);
                repetition.push(0); // TODO: validate that this is 0 for deeply nested lists
                definition_mask.push((false, current_def_level));
                println!("Definition length b: {}", definition.len());
            }
            (array_from..array_to).for_each(|_| {
                definition.push(if *parent_def_level == self.max_definition {
                    // TODO: haven't validated this in deeply-nested lists
                    self.max_definition + mask as i16
                } else {
                    *parent_def_level
                });
                definition_mask.push((true, current_def_level));
                println!("Definition length a: {}", definition.len());
            });

            // 11-11-2020 (23:57GMT)
            // we are pushing defined repetitions even if a definition is < max
            // I had initially separated the repetition logic here so that I
            // don't perform a `has_repetition` check on each loop.
            // The downside's that I now need to index into `definitions` so I
            // can check if a value is defined or not.

            if has_repetition && array_len > 0 {
                // compute the repetition level

                // dbg!(&definition);
                // dbg!(current_def_level, parent_level.max_definition);
                // dbg!(&parent_level.repetition);
                match &self.repetition {
                    Some(rep) => {
                        let parent_rep = rep[index];
                        // TODO(11/11/2020) need correct variable to mask repetitions correctly
                        if definition[def_index] == current_def_level {
                            repetition.push(parent_rep);
                            println!("* Index {} definition is {}, and repetition is {}. Current def: {}", def_index, definition[def_index], parent_rep, current_def_level);
                            dbg!(&repetition);
                            def_index += 1;
                            (1..array_len).for_each(|_| {
                                println!("* Index {} definition is {}, and repetition is {}. Current def: {}", def_index, definition[def_index], parent_rep, current_def_level);
                                repetition.push(current_def_level); // was parent_rep + 1
                                def_index += 1;
                            });
                        } else {
                            (0..array_len).for_each(|_| {
                                println!("* Index {} definition is {}, and repetition is {}. Current def: {}", def_index, definition[def_index], parent_rep, current_def_level);
                                repetition.push(0); // TODO: should it be anything else?
                                // TODO: use an append instead of pushes
                                def_index += 1;
                            });
                        }
                    }
                    None => {
                        println!("+ Index {} definition is {}, and repetition is 0. Current def: {}", def_index, definition[def_index], current_def_level);
                        // if definition[def_index] == current_def_level {
                            repetition.push(0);
                            def_index += 1;
                            (1..array_len).for_each(|_| {
                                repetition.push(1); // TODO: is it always 0 and 1?
                                def_index += 1;
                            });
                        // } else {
                        //     (0..array_len).for_each(|_| {
                        //         repetition.push(0); // TODO: should it be anything else?
                        //                             // TODO: use an append instead of pushes
                        //         def_index += 1;
                        //     });
                        // }
                    }
                }
            }
        });
    });

        let lev = LevelInfo {
            definition,
            repetition: if !has_repetition {
                None
            } else {
                Some(repetition)
            },
            definition_mask,
            array_mask,
            array_offsets,
            is_list: has_repetition,
            max_definition: current_def_level,
            is_nullable,
        };

        println!("done");

        lev
    }
}

#[cfg(test)]
mod tests {
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
            max_definition: 0,            // at the root, set to 0
            is_list: false,               // root is never list
            is_nullable: false,           // root in example is non-nullable
        };
        // offset into array, each level1 has 2 values
        let array_offsets = vec![0, 2, 4];
        let array_mask = vec![true, true];

        // calculate level1 levels
        let levels = parent_levels.calculate_child_levels(
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
        assert_eq!(levels, expected_levels);

        // level2
        let parent_levels = levels;
        let array_offsets = vec![0, 3, 7, 8, 10];
        let array_mask = vec![true, true, true, true];
        let levels = parent_levels.calculate_child_levels(
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
            1,
        );
        let expected_levels = LevelInfo {
            definition: vec![1; 10],
            repetition: None,
            definition_mask: vec![(true, 1); 10],
            array_offsets,
            array_mask,
            max_definition: 1,
            is_list: false,
            is_nullable: false,
        };
        assert_eq!(&levels, &expected_levels);
    }

    #[test]
    #[ignore]
    fn test_calculate_one_level_2() {
        // This test calculates the levels for a non-null primitive array
        let parent_levels = LevelInfo {
            definition: vec![1; 5],
            repetition: None,
            definition_mask: vec![
                (true, 1),
                (false, 1),
                (true, 1),
                (true, 1),
                (false, 1),
            ],
            array_offsets: (0..=5).collect(),
            array_mask: vec![true, false, true, true, false],
            max_definition: 0,
            is_list: false,
            is_nullable: true,
        };
        let array_offsets: Vec<i64> = (0..=5).collect();
        let array_mask = vec![true, false, true, true, false];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            false,
            false,
            1,
        );
        let expected_levels = LevelInfo {
            definition: vec![1; 5],
            repetition: None,
            definition_mask: vec![(true, 1); 5],
            array_offsets,
            array_mask,
            max_definition: 1,
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
            definition: vec![0, 0, 0, 0, 0],
            repetition: None,
            definition_mask: vec![(true, 1), (true, 1), (true, 1), (true, 1), (true, 1)],
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
            array_mask.clone(),
            true,
            false,
            1,
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
            definition: vec![1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            repetition: Some(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            definition_mask: vec![
                (true, 1),
                (true, 1),
                (false, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
            ],
            array_offsets,
            array_mask,
            max_definition: 1,
            is_list: true,
            is_nullable: false,
        };
        assert_eq!(levels, expected_levels);
    }

    #[test]
    #[ignore]
    fn test_calculate_array_levels_2() {
        // if some values are null
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
            max_definition: 0,
            is_list: false,
            is_nullable: true,
        };
        let array_offsets = vec![0, 2, 2, 4, 8, 11];
        let array_mask = vec![true, false, true, true, true];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            true,
            true,
            1,
        );
        let expected_levels = LevelInfo {
            // 0 1 [2] are 0 (not defined at level 1)
            // [2] is 1, but has 0 slots so is not populated (defined at level 1 only)
            // 2 3 [4] are 0
            // 4 5 6 7 [8] are 1 (defined at level 1 only)
            // 8 9 10 [11] are 2 (defined at both levels)
            definition: vec![0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1],
            repetition: Some(vec![0, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1]),
            definition_mask: vec![
                (true, 1),
                (true, 1),
                (false, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
            ],
            array_offsets,
            array_mask,
            max_definition: 1,
            is_nullable: true,
            is_list: true,
        };
        assert_eq!(&levels, &expected_levels);

        // nested lists (using previous test)
        let _nested_parent_levels = levels;
        let array_offsets = vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22];
        let array_mask = vec![
            true, true, true, true, true, true, true, true, true, true, true,
        ];
        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            true,
            true,
            2,
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
            definition: vec![
                0, 0, 0, 0, 0i16, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
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
        assert_eq!(levels, expected_levels);
    }

    #[test]
    #[ignore]
    fn test_calculate_array_levels_nested_list() {
        // if all array values are defined (e.g. batch<list<_>>)
        let parent_levels = LevelInfo {
            definition: vec![0, 0, 0, 0],
            repetition: None,
            definition_mask: vec![(true, 1), (true, 1), (true, 1), (true, 1)],
            array_offsets: vec![0, 1, 2, 3, 4],
            array_mask: vec![true, true, true, true],
            max_definition: 0,
            is_list: false,
            is_nullable: false,
        };
        let array_offsets = vec![0, 0, 3, 5, 7];
        let array_mask = vec![false, true, true, true];

        let levels = parent_levels.calculate_child_levels(
            array_offsets.clone(),
            array_mask.clone(),
            true,
            false,
            1,
        );
        let expected_levels = LevelInfo {
            definition: vec![0, 1, 1, 1, 1, 1, 1, 1],
            repetition: Some(vec![0, 0, 1, 1, 0, 1, 0, 1]),
            definition_mask: vec![
                (false, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
                (true, 1),
            ],
            array_offsets,
            array_mask,
            max_definition: 1,
            is_list: true,
            is_nullable: false,
        };
        assert_eq!(levels, expected_levels);

        // nested lists (using previous test)
        let _nested_parent_levels = levels;
        let array_offsets = vec![0, 1, 3, 3, 6, 10, 10, 15];
        let array_mask = vec![true, true, false, true, true, false, true];
        let levels = parent_levels.calculate_child_levels(
            array_offsets,
            array_mask,
            true,
            true,
            2,
        );
        let expected_levels = LevelInfo {
            definition: vec![0, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2],
            repetition: Some(vec![0, 0, 1, 2, 1, 0, 2, 2, 1, 2, 2, 2, 0, 1, 2, 2, 2, 2]),
            definition_mask: vec![
                (false, 1),
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
                (false, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
                (true, 2),
            ],
            array_mask: vec![true, true, false, true, true, false, true],
            array_offsets: vec![0, 1, 3, 3, 6, 10, 10, 15],
            is_list: true,
            is_nullable: true,
            max_definition: 2,
        };
        assert_eq!(levels, expected_levels);
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
            definition_mask: vec![(true, 2); 6],
            array_offsets: (0..=6).collect(),
            array_mask: vec![true, true, true, false, false, true],
            max_definition: 2,
            is_list: false,
            is_nullable: true,
        };
        let b_levels =
            a_levels.calculate_child_levels(b_offsets.clone(), b_mask, false, true, 2);
        assert_eq!(&b_expected_levels, &b_levels);

        // c's offset and mask
        let c_offsets = b_offsets;
        let c_mask = vec![true, false, true, false, false, true];
        // c's expected levels
        let c_expected_levels = LevelInfo {
            definition: vec![3, 2, 3, 1, 0, 3],
            repetition: None,
            definition_mask: vec![(true, 3); 6],
            array_offsets: c_offsets.clone(),
            array_mask: vec![true, false, true, false, false, true],
            max_definition: 3,
            is_list: false,
            is_nullable: true,
        };
        let c_levels = b_levels.calculate_child_levels(c_offsets, c_mask, false, true, 3);
        assert_eq!(&c_expected_levels, &c_levels);
    }
}

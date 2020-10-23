use arrow::array as arrow_array;
// TODO:
// use crate::errors::{ParquetError, Result};

use std::ops::Range;

struct PathWriteContext {
    rep_levels: Vec<i16>,
    def_levels: Vec<i16>,
}

#[derive(Default)]
struct PathBuilder {
    nullable_in_parent: bool,
    info: PathInfo,
    paths: Vec<PathInfo>,
}

impl PathBuilder {
    fn new(start_nullable: bool) -> Self {
        Self {
            nullable_in_parent: start_nullable,
            ..Default::default()
        }
    }
}

/// Contains static information derived from traversing the schema.
#[derive(Default)]
struct PathInfo {
    path: Vec<Box<dyn Node>>,
    primitive_array: Option<arrow_array::ArrayRef>,
    max_def_level: i16,
    max_rep_level: i16,
    has_dictionary: bool,
    leaf_is_nullable: bool,
}

trait Node {
    fn run(&self, range: Range<i64>, context: &mut PathWriteContext);
}
//
// enum Node {
//     NullableTerminalNode,
//     ListNode,
//     LargeListNode,
//     FixedSizeListNode,
//     NullableNode,
//     AllPresentTerminalNode,
//     AllNullsTerminalNode,
// }

struct MultipathLevelBuilder {
    // root_range: Range<i64>, TODO
    data: arrow_array::ArrayDataRef,
    path_builder: PathBuilder,
}

impl MultipathLevelBuilder {
    fn make(array: arrow_array::ArrayRef, array_field_nullable: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let constructor = PathBuilder::new(array_field_nullable);
        // RETURN_NOT_OK(VisitArrayInline(array, constructor.get()));
        Ok(Self {
            data: array.data(),
            path_builder: constructor,
        })
    }

    fn write(
        array: arrow_array::ArrayRef,
        array_field_nullable: bool,
        // context: TODO,
        // callback: TODO,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let builder = Self::make(array, array_field_nullable)?;

        // PathBuilder constructor(array_field_nullable);
        // RETURN_NOT_OK(VisitArrayInline(array, &constructor));
        // for (int leaf_idx = 0; leaf_idx < builder->GetLeafCount(); leaf_idx++) {
        //   RETURN_NOT_OK(builder->Write(leaf_idx, context, callback));
        // }

        Ok(())
    }
}

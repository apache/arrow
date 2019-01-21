use std::{
    collections::HashMap,
    fmt::{self, Debug},
    ops::Index,
    slice::{self, SliceIndex},
    vec,
};

use crate::{
    basic::{LogicalType, Repetition},
    column::reader::ColumnReader,
    errors::ParquetError,
    record::{
        reader::{MapReader, RepeatedReader},
        schemas::{ListSchema, ListSchemaType},
        Deserialize,
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

/// Returns true if repeated type is an element type for the list.
/// Used to determine legacy list types.
/// This method is copied from Spark Parquet reader and is based on the reference:
/// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#backward-compatibility-rules
pub(super) fn parse_list<T: Deserialize>(
    schema: &Type,
) -> Result<ListSchema<T::Schema>, ParquetError> {
    if schema.is_group()
        && schema.get_basic_info().logical_type() == LogicalType::LIST
        && schema.get_fields().len() == 1
    {
        let sub_schema = schema.get_fields().into_iter().nth(0).unwrap();
        if sub_schema.get_basic_info().repetition() == Repetition::REPEATED {
            return Ok(
                if sub_schema.is_group()
                    && sub_schema.get_fields().len() == 1
                    && sub_schema.name() != "array"
                    && sub_schema.name() != format!("{}_tuple", schema.name())
                {
                    let element = sub_schema.get_fields().into_iter().nth(0).unwrap();
                    let list_name = if sub_schema.name() == "list" {
                        None
                    } else {
                        Some(sub_schema.name().to_owned())
                    };
                    let element_name = if element.name() == "element" {
                        None
                    } else {
                        Some(element.name().to_owned())
                    };

                    ListSchema(
                        T::parse(&*element)?.1,
                        ListSchemaType::List(list_name, element_name),
                    )
                } else {
                    let element_name = sub_schema.name().to_owned();
                    ListSchema(
                        T::parse(&*sub_schema)?.1,
                        ListSchemaType::ListCompat(element_name),
                    )
                },
            );
        }
    }
    Err(ParquetError::General(String::from(
        "Couldn't parse List<T>",
    )))
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct List<T>(pub(in super::super) Vec<T>);

impl<T> Deserialize for List<T>
where
    T: Deserialize,
{
    // existential type Reader: Reader<Item = Self>;
    type Reader = MapReader<RepeatedReader<T::Reader>, fn(Vec<T>) -> Result<Self, ParquetError>>;
    type Schema = ListSchema<T::Schema>;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        if !schema.is_schema() && schema.get_basic_info().repetition() == Repetition::REQUIRED {
            return parse_list::<T>(schema).map(|schema2| (schema.name().to_owned(), schema2));
        }
        // A repeated field that is neither contained by a `LIST`- or `MAP`-annotated group
        // nor annotated by `LIST` or `MAP` should be interpreted as a required list of
        // required elements where the element type is the type of the field.
        if schema.get_basic_info().repetition() == Repetition::REPEATED {
            let mut schema2: Type = schema.clone();
            let basic_info = match schema2 {
                Type::PrimitiveType {
                    ref mut basic_info, ..
                } => basic_info,
                Type::GroupType {
                    ref mut basic_info, ..
                } => basic_info,
            };
            basic_info.set_repetition(Some(Repetition::REQUIRED));
            return Ok((
                schema.name().to_owned(),
                ListSchema(T::parse(&schema2)?.1, ListSchemaType::Repeated),
            ));
        }
        Err(ParquetError::General(String::from(
            "Couldn't parse List<T>",
        )))
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        MapReader(
            match schema.1 {
                ListSchemaType::List(ref list_name, ref element_name) => {
                    let list_name = list_name.as_ref().map(|x| &**x).unwrap_or("list");
                    let element_name = element_name.as_ref().map(|x| &**x).unwrap_or("element");

                    path.push(list_name.to_owned());
                    path.push(element_name.to_owned());
                    let reader = T::reader(
                        &schema.0,
                        path,
                        curr_def_level + 1,
                        curr_rep_level + 1,
                        paths,
                        batch_size,
                    );
                    path.pop().unwrap();
                    path.pop().unwrap();

                    RepeatedReader {
                        def_level: curr_def_level,
                        rep_level: curr_rep_level,
                        reader,
                    }
                }
                ListSchemaType::ListCompat(ref element_name) => {
                    path.push(element_name.to_owned());
                    let reader = T::reader(
                        &schema.0,
                        path,
                        curr_def_level + 1,
                        curr_rep_level + 1,
                        paths,
                        batch_size,
                    );
                    path.pop().unwrap();

                    RepeatedReader {
                        def_level: curr_def_level,
                        rep_level: curr_rep_level,
                        reader,
                    }
                }
                ListSchemaType::Repeated => {
                    let reader = T::reader(
                        &schema.0,
                        path,
                        curr_def_level + 1,
                        curr_rep_level + 1,
                        paths,
                        batch_size,
                    );
                    RepeatedReader {
                        def_level: curr_def_level,
                        rep_level: curr_rep_level,
                        reader,
                    }
                }
            },
            (|x| Ok(List(x))) as fn(_) -> _,
        )
    }
}

impl<T> List<T> {
    pub fn iter(&self) -> slice::Iter<'_, T> {
        self.0.iter()
    }

    pub fn into_iter(self) -> vec::IntoIter<T> {
        self.0.into_iter()
    }
}
impl<T> From<Vec<T>> for List<T> {
    fn from(vec: Vec<T>) -> Self {
        List(vec)
    }
}
impl<T> Into<Vec<T>> for List<T> {
    fn into(self) -> Vec<T> {
        self.0
    }
}
impl<T, I> Index<I> for List<T>
where
    I: SliceIndex<[T]>,
{
    type Output = <I as SliceIndex<[T]>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.0.index(index)
    }
}
impl<T> Debug for List<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        f.debug_list().entries(self.iter()).finish()
    }
}

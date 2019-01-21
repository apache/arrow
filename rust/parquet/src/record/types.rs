mod array;
mod decimal;
mod group;
mod list;
mod map;
mod numbers;
mod option;
mod time;
mod tuple;
mod value;

use super::schemas::ValueSchema;
use crate::errors::ParquetError;

pub use self::{
    array::*, decimal::*, group::*, list::*, map::*, numbers::*, option::*, time::*, tuple::*,
    value::*,
};

pub trait Downcast<T> {
    fn downcast(self) -> Result<T, ParquetError>;
}

fn downcast<T>((name, schema): (String, ValueSchema)) -> Result<(String, T), ParquetError>
where
    ValueSchema: Downcast<T>,
{
    schema.downcast().map(|schema| (name, schema))
}

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub struct Root<T>(pub T);

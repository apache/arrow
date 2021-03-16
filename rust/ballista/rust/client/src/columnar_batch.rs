// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and

use std::{collections::HashMap, sync::Arc};

use ballista_core::error::{ballista_error, Result};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::scalar::ScalarValue;

pub type MaybeColumnarBatch = Result<Option<ColumnarBatch>>;

/// Batch of columnar data.
#[allow(dead_code)]
#[derive(Debug, Clone)]

pub struct ColumnarBatch {
    schema: Arc<Schema>,
    columns: HashMap<String, ColumnarValue>,
}

impl ColumnarBatch {
    pub fn from_arrow(batch: &RecordBatch) -> Self {
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(i, array)| {
                (
                    batch.schema().field(i).name().clone(),
                    ColumnarValue::Columnar(array.clone()),
                )
            })
            .collect();

        Self {
            schema: batch.schema(),
            columns,
        }
    }

    pub fn from_values(values: &[ColumnarValue], schema: &Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name().clone(), values[i].clone()))
            .collect();

        Self {
            schema: Arc::new(schema.clone()),
            columns,
        }
    }

    pub fn to_arrow(&self) -> Result<RecordBatch> {
        let arrays = self
            .schema
            .fields()
            .iter()
            .map(|c| {
                match self.column(c.name())? {
                    ColumnarValue::Columnar(array) => Ok(array.clone()),
                    ColumnarValue::Scalar(_, _) => {
                        // note that this can be implemented easily if needed
                        Err(ballista_error("Cannot convert scalar value to Arrow array"))
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    pub fn num_rows(&self) -> usize {
        self.columns[self.schema.field(0).name()].len()
    }

    pub fn column(&self, name: &str) -> Result<&ColumnarValue> {
        Ok(&self.columns[name])
    }

    pub fn memory_size(&self) -> usize {
        self.columns.values().map(|c| c.memory_size()).sum()
    }
}

/// A columnar value can either be a scalar value or an Arrow array.
#[allow(dead_code)]
#[derive(Debug, Clone)]

pub enum ColumnarValue {
    Scalar(ScalarValue, usize),
    Columnar(ArrayRef),
}

impl ColumnarValue {
    pub fn len(&self) -> usize {
        match self {
            ColumnarValue::Scalar(_, n) => *n,
            ColumnarValue::Columnar(array) => array.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn data_type(&self) -> &DataType {
        match self {
            ColumnarValue::Columnar(array) => array.data_type(),
            ColumnarValue::Scalar(value, _) => match value {
                ScalarValue::UInt8(_) => &DataType::UInt8,
                ScalarValue::UInt16(_) => &DataType::UInt16,
                ScalarValue::UInt32(_) => &DataType::UInt32,
                ScalarValue::UInt64(_) => &DataType::UInt64,
                ScalarValue::Int8(_) => &DataType::Int8,
                ScalarValue::Int16(_) => &DataType::Int16,
                ScalarValue::Int32(_) => &DataType::Int32,
                ScalarValue::Int64(_) => &DataType::Int64,
                ScalarValue::Float32(_) => &DataType::Float32,
                ScalarValue::Float64(_) => &DataType::Float64,
                _ => unimplemented!(),
            },
        }
    }

    pub fn to_arrow(&self) -> ArrayRef {
        match self {
            ColumnarValue::Columnar(array) => array.clone(),
            ColumnarValue::Scalar(value, n) => value.to_array_of_size(*n),
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            ColumnarValue::Columnar(array) => array.get_array_memory_size(),
            _ => 0,
        }
    }
}

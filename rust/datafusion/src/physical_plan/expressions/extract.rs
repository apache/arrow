use crate::error::Result;
use core::fmt;
use std::{any::Any, sync::Arc};

use arrow::{
    array::{Date32Array, Date64Array, TimestampNanosecondArray},
    compute::hour,
    datatypes::{DataType, Schema, TimeUnit},
    record_batch::RecordBatch,
};

use crate::{
    error::DataFusionError,
    logical_plan::DatePart,
    physical_plan::{ColumnarValue, PhysicalExpr},
};

impl fmt::Display for Extract {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXTRACT({} AS {:?})", self.date_part, self.expr)
    }
}

/// InList
#[derive(Debug)]
pub struct Extract {
    date_part: DatePart,
    expr: Arc<dyn PhysicalExpr>,
}

impl Extract {
    /// Create new Extract expression
    pub fn new(date_part: DatePart, expr: Arc<dyn PhysicalExpr>) -> Self {
        Self { date_part, expr }
    }
}

impl PhysicalExpr for Extract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        let data_type = value.data_type();
        let array = match value {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        match data_type {
            DataType::Date32 => {
                let array = array.as_any().downcast_ref::<Date32Array>().unwrap();
                Ok(ColumnarValue::Array(Arc::new(hour(array)?)))
            }
            DataType::Date64 => {
                let array = array.as_any().downcast_ref::<Date64Array>().unwrap();
                Ok(ColumnarValue::Array(Arc::new(hour(array)?)))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                Ok(ColumnarValue::Array(Arc::new(hour(array)?)))
            }
            datatype => Err(DataFusionError::Internal(format!(
                "Extract does not support datatype {:?}",
                datatype
            ))),
        }
    }
}

/// Creates a expression Extract
pub fn extract(
    date_part: DatePart,
    expr: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(Extract::new(date_part, expr)))
}

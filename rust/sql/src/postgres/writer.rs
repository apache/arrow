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

use std::io::Write;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, DateUnit, Field, Schema, TimeUnit};
use arrow::error::{ArrowError, Result};
use arrow::{compute::cast, record_batch::RecordBatch};
use byteorder::{NetworkEndian, WriteBytesExt};
use postgres::{Client, CopyInWriter, NoTls};

use super::{Postgres, EPOCH_DAYS, EPOCH_MICROS, MAGIC};
use crate::SqlDataSink;

impl SqlDataSink for Postgres {
    fn create_table(
        connection: &str,
        table_name: &str,
        schema: &Arc<Schema>,
    ) -> Result<()> {
        let mut table = String::new();
        let field_len = schema.fields().len();
        for i in 0..field_len {
            table.push_str(
                format!(
                    "{} {}",
                    schema.field(i).name(),
                    get_postgres_type(schema.field(i))?
                )
                .as_str(),
            );
            if i + 1 < field_len {
                table.push_str(",\n");
            }
        }

        let mut client = Client::connect(connection, NoTls).unwrap();
        client
            .execute(format!("DROP TABLE IF EXISTS {}", table_name).as_str(), &[])
            .unwrap();
        client
            .execute(
                format!("CREATE TABLE IF NOT EXISTS {} ({})", table_name, table).as_str(),
                &[],
            )
            .unwrap();
        Ok(())
    }
    fn write_to_table(
        connection: &str,
        table_name: &str,
        batches: &[RecordBatch],
    ) -> Result<()> {
        if batches.is_empty() {
            return Err(ArrowError::IoError(
                "At least one batch should be provided to the SQL writer".to_string(),
            ));
        }

        let mut client = Client::connect(connection, NoTls).unwrap();
        let mut writer = get_binary_writer(&mut client, table_name)?;

        for batch in batches {
            // write out batch
            write_to_binary(&mut writer, batch)?;
        }
        writer.write_i16::<NetworkEndian>(-1)?;
        println!("Done writing values, finishing");

        writer.finish().unwrap();
        Ok(())
    }
}

fn get_postgres_type(field: &Field) -> Result<String> {
    let nullable = if !field.is_nullable() {
        " not null"
    } else {
        ""
    };
    let dtype = match field.data_type() {
        DataType::Boolean => "boolean",
        DataType::Int8 => {
            return Err(ArrowError::SqlError(
                "Int8 not mapped to any PostgreSQL type".to_string(),
            ))
        }
        DataType::Int16 => "smallint",
        DataType::Int32 => "integer",
        DataType::Int64 => "bigint",
        DataType::UInt8 => "character",
        DataType::UInt16 => "bigint",
        DataType::UInt32 => "bigint",
        DataType::UInt64 => "bigint",
        DataType::Float16 => {
            return Err(ArrowError::InvalidArgumentError(
                "Float16 not supported".to_string(),
            ))
        }
        DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::Timestamp(_, timezone) => match timezone {
            Some(_) => "timestamp with time zone",
            None => "timestamp",
        },
        DataType::Date32(_) | DataType::Date64(_) => "date",
        DataType::Time32(_) | DataType::Time64(_) => "time",
        DataType::Duration(_) => {
            return Err(ArrowError::SqlError(
                "Duration write not support not yet implemented".to_string(),
            ))
        }
        DataType::Interval(_) => {
            return Err(ArrowError::SqlError(
                "Interval write not support not yet implemented".to_string(),
            ))
        }
        DataType::Binary => "bytea",
        DataType::FixedSizeBinary(_) => {
            return Err(ArrowError::SqlError(
                "FixedSizeBinary write not support not yet implemented".to_string(),
            ))
        }
        DataType::Utf8 => "text",
        DataType::List(_) => {
            return Err(ArrowError::SqlError(
                "List write not support not yet implemented".to_string(),
            ))
        }
        DataType::FixedSizeList(_, _) => {
            return Err(ArrowError::SqlError(
                "FixedSizeList write not support not yet implemented".to_string(),
            ))
        }
        DataType::Struct(_) => {
            return Err(ArrowError::SqlError(
                "Struct write not support not yet implemented".to_string(),
            ))
        }
        DataType::Dictionary(_, _) => {
            return Err(ArrowError::SqlError(
                "Dictionary write not support not yet implemented".to_string(),
            ))
        }
        DataType::Null => {
            return Err(ArrowError::SqlError(
                "Null write not support not yet implemented".to_string(),
            ))
        }
        DataType::Union(_) => {
            return Err(ArrowError::SqlError(
                "Union type not yet supported".to_string(),
            ));
        }
        DataType::LargeBinary => {
            return Err(ArrowError::SqlError("Type not yet supported".to_string()));
        }
        DataType::LargeUtf8 => {
            return Err(ArrowError::SqlError("Type not yet supported".to_string()));
        }
        DataType::LargeList(_) => {
            return Err(ArrowError::SqlError("Type not yet supported".to_string()));
        }
        DataType::Decimal(_, _) => {
            return Err(ArrowError::SqlError("Type not yet supported".to_string()));
        }
    };
    Ok(format!("{} {}", dtype, nullable))
}

fn get_binary_writer<'a>(
    client: &'a mut Client,
    table_name: &str,
) -> Result<CopyInWriter<'a>> {
    Ok(client
        .copy_in(format!("COPY {} FROM stdin with (format binary)", table_name).as_str())
        .unwrap())
}

fn write_to_binary(writer: &mut CopyInWriter, batch: &RecordBatch) -> Result<u64> {
    // write header
    writer.write_all(MAGIC)?;
    // write flags
    writer.write_all(&[0; 4])?;
    // write header extension
    writer.write_all(&[0; 4])?;

    let column_len = batch.num_columns();

    // start writing rows
    for row in 0..batch.num_rows() {
        // write row/tuple length
        writer.write_i16::<NetworkEndian>(column_len as i16)?;

        for (arr, field) in batch.columns().iter().zip(batch.schema().fields().iter()) {
            match field.data_type() {
                arrow::datatypes::DataType::Boolean => {
                    let arr = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Int8 => {
                    let arr = arr.as_any().downcast_ref::<Int8Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Int16 => {
                    let arr = arr.as_any().downcast_ref::<Int16Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Int32 => {
                    let arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Int64 => {
                    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::UInt8 => {
                    let arr = arr.as_any().downcast_ref::<UInt8Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::UInt16 => {
                    return Err(ArrowError::SqlError(
                        "No PostgreSQL data type has been mapped to UInt16Type"
                            .to_string(),
                    ));
                }
                arrow::datatypes::DataType::UInt32 => {
                    let arr = arr.as_any().downcast_ref::<UInt32Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::UInt64 => {
                    let arr = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Float16 => {
                    return Err(ArrowError::SqlError(
                        "Float16 type not yet supported by Arrow".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Float32 => {
                    let arr = arr.as_any().downcast_ref::<Float32Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Float64 => {
                    let arr = arr.as_any().downcast_ref::<Float64Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Date32(_)
                | arrow::datatypes::DataType::Date64(_) => {
                    let arr = cast(arr, &DataType::Date32(DateUnit::Day))?;
                    let arr = arr.as_any().downcast_ref::<Date32Array>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Timestamp(_, _) => {
                    let arr =
                        cast(arr, &DataType::Timestamp(TimeUnit::Microsecond, None))?;
                    let arr = arr
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Time32(_)
                | arrow::datatypes::DataType::Time64(_) => {
                    let arr = cast(arr, &DataType::Time64(TimeUnit::Microsecond))?;
                    let arr = arr
                        .as_any()
                        .downcast_ref::<Time64MicrosecondArray>()
                        .unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::Duration(_) => {
                    return Err(ArrowError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer"
                            .to_string(),
                    ));
                }
                arrow::datatypes::DataType::Interval(_) => {
                    return Err(ArrowError::SqlError(
                        "Writing Intervals not yet implemented".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Binary => {
                    let arr = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::LargeBinary => {
                    return Err(ArrowError::SqlError(
                        "Type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::FixedSizeBinary(_) => {
                    return Err(ArrowError::SqlError(
                        "Type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Utf8 => {
                    let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                    arr.write_to_binary(writer, row)?;
                }
                arrow::datatypes::DataType::LargeUtf8 => {
                    return Err(ArrowError::SqlError(
                        "Type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::List(_)
                | arrow::datatypes::DataType::LargeList(_) => {
                    return Err(ArrowError::SqlError(
                        "Type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::FixedSizeList(_, _) => {
                    return Err(ArrowError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer"
                            .to_string(),
                    ));
                }
                arrow::datatypes::DataType::Struct(_) => {
                    return Err(ArrowError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer"
                            .to_string(),
                    ));
                }
                arrow::datatypes::DataType::Dictionary(_, _) => {
                    return Err(ArrowError::SqlError(
                        "Duration type not yet supported by PostgreSQL writer"
                            .to_string(),
                    ));
                }
                arrow::datatypes::DataType::Union(_) => {
                    return Err(ArrowError::SqlError(
                        "Union type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Null => {
                    // TODO: write it out as an all-null i8?
                    return Err(ArrowError::SqlError(
                        "Null type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
                arrow::datatypes::DataType::Decimal(_, _) => {
                    return Err(ArrowError::SqlError(
                        "Decimal type not yet supported by PostgreSQL writer".to_string(),
                    ));
                }
            }
        }
    }

    Ok(0)
}

/// A trait to write various supported array values to PostgreSQL's binary format
///
/// This assumes that nullness has been checked by the caller, and thus does not check this.
/// It is also the responsibility of the caller to ensure that the array has been converted to
///  a compatible array type for the Postgres column type
trait WriteToBinary {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()>;
}

fn write_null_to_binary<W: Write>(writer: &mut W) -> Result<()> {
    writer.write_i32::<NetworkEndian>(-1)?;
    Ok(())
}

fn write_length_to_binary<W: Write>(writer: &mut W, length: usize) -> Result<()> {
    writer.write_i32::<NetworkEndian>(length as i32)?;
    Ok(())
}

impl WriteToBinary for BooleanArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<u8>())?;
        if self.value(index) {
            writer.write_u8(1)
        } else {
            writer.write_u8(0)
        }?;
        Ok(())
    }
}

impl WriteToBinary for UInt8Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<u8>())?;
        writer.write_u8(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Int8Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i8>())?;
        writer.write_i8(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Int16Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i16>())?;
        writer.write_i16::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Int32Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i32>())?;
        writer.write_i32::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for UInt32Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<u32>())?;
        writer.write_u32::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for UInt64Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<u32>())?;
        writer.write_u64::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Int64Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i64>())?;
        writer.write_i64::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Float32Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<f32>())?;
        writer.write_f32::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for Float64Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<f64>())?;
        writer.write_f64::<NetworkEndian>(self.value(index))?;
        Ok(())
    }
}

impl WriteToBinary for TimestampMicrosecondArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i64>())?;
        writer.write_i64::<NetworkEndian>(self.value(index) - EPOCH_MICROS)?;
        Ok(())
    }
}

impl WriteToBinary for Date32Array {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i32>())?;
        writer.write_i32::<NetworkEndian>(self.value(index) - EPOCH_DAYS)?;
        Ok(())
    }
}

impl WriteToBinary for Time64MicrosecondArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        write_length_to_binary(writer, std::mem::size_of::<i32>())?;
        writer.write_i32::<NetworkEndian>(self.value(index) as i32)?;
        Ok(())
    }
}

impl WriteToBinary for StringArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        let value = self.value(index).as_bytes();
        dbg!(&value);
        write_length_to_binary(writer, value.len())?;
        writer.write_all(value)?;
        Ok(())
    }
}

impl WriteToBinary for BinaryArray {
    fn write_to_binary<W: Write>(&self, writer: &mut W, index: usize) -> Result<()> {
        let value = self.value(index);
        write_length_to_binary(writer, value.len())?;
        writer.write_all(value)?;
        Ok(())
    }
}

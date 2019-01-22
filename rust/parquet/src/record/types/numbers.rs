use std::{collections::HashMap, marker::PhantomData};

use crate::{
    column::reader::ColumnReader,
    data_type::{BoolType, DoubleType, FloatType, Int32Type, Int64Type},
    errors::ParquetError,
    record::{
        reader::{
            BoolReader, F32Reader, F64Reader, I32Reader, I64Reader, MapReader, TryIntoReader,
        },
        schemas::{
            BoolSchema, F32Schema, F64Schema, I16Schema, I32Schema, I64Schema, I8Schema, U16Schema,
            U32Schema, U64Schema, U8Schema,
        },
        triplet::TypedTripletIter,
        types::{downcast, Value},
        Deserialize,
    },
    schema::types::{ColumnDescPtr, ColumnPath, Type},
};

impl Deserialize for bool {
    type Reader = BoolReader;
    type Schema = BoolSchema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        BoolReader {
            column: TypedTripletIter::<BoolType>::new(
                curr_def_level,
                curr_rep_level,
                batch_size,
                col_reader,
            ),
        }
    }
}

impl Deserialize for i8 {
    type Reader = TryIntoReader<I32Reader, i8>;
    type Schema = I8Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        TryIntoReader(
            I32Reader {
                column: TypedTripletIter::<Int32Type>::new(
                    curr_def_level,
                    curr_rep_level,
                    batch_size,
                    col_reader,
                ),
            },
            PhantomData,
        )
    }
}
impl Deserialize for u8 {
    type Reader = TryIntoReader<I32Reader, u8>;
    type Schema = U8Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        TryIntoReader(
            I32Reader {
                column: TypedTripletIter::<Int32Type>::new(
                    curr_def_level,
                    curr_rep_level,
                    batch_size,
                    col_reader,
                ),
            },
            PhantomData,
        )
    }
}

impl Deserialize for i16 {
    type Reader = TryIntoReader<I32Reader, i16>;
    type Schema = I16Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        TryIntoReader(
            I32Reader {
                column: TypedTripletIter::<Int32Type>::new(
                    curr_def_level,
                    curr_rep_level,
                    batch_size,
                    col_reader,
                ),
            },
            PhantomData,
        )
    }
}
impl Deserialize for u16 {
    type Reader = TryIntoReader<I32Reader, u16>;
    type Schema = U16Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        TryIntoReader(
            I32Reader {
                column: TypedTripletIter::<Int32Type>::new(
                    curr_def_level,
                    curr_rep_level,
                    batch_size,
                    col_reader,
                ),
            },
            PhantomData,
        )
    }
}

impl Deserialize for i32 {
    type Reader = I32Reader;
    type Schema = I32Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        I32Reader {
            column: TypedTripletIter::<Int32Type>::new(
                curr_def_level,
                curr_rep_level,
                batch_size,
                col_reader,
            ),
        }
    }
}
impl Deserialize for u32 {
    // existential type Reader: Reader<Item = Self>;
    type Reader = MapReader<I32Reader, fn(i32) -> Result<Self, ParquetError>>;
    type Schema = U32Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        MapReader(
            I32Reader {
                column: TypedTripletIter::<Int32Type>::new(
                    curr_def_level,
                    curr_rep_level,
                    batch_size,
                    col_reader,
                ),
            },
            |x| Ok(x as u32),
        )
    }
}

impl Deserialize for i64 {
    type Reader = I64Reader;
    type Schema = I64Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        I64Reader {
            column: TypedTripletIter::<Int64Type>::new(
                curr_def_level,
                curr_rep_level,
                batch_size,
                col_reader,
            ),
        }
    }
}
impl Deserialize for u64 {
    // existential type Reader: Reader<Item = Self>;
    type Reader = MapReader<I64Reader, fn(i64) -> Result<Self, ParquetError>>;
    type Schema = U64Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        MapReader(
            I64Reader {
                column: TypedTripletIter::<Int64Type>::new(
                    curr_def_level,
                    curr_rep_level,
                    batch_size,
                    col_reader,
                ),
            },
            |x| Ok(x as u64),
        )
    }
}

impl Deserialize for f32 {
    type Reader = F32Reader;
    type Schema = F32Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        F32Reader {
            column: TypedTripletIter::<FloatType>::new(
                curr_def_level,
                curr_rep_level,
                batch_size,
                col_reader,
            ),
        }
    }
}
impl Deserialize for f64 {
    type Reader = F64Reader;
    type Schema = F64Schema;

    fn parse(schema: &Type) -> Result<(String, Self::Schema), ParquetError> {
        Value::parse(schema).and_then(downcast)
    }

    fn reader(
        _schema: &Self::Schema,
        path: &mut Vec<String>,
        curr_def_level: i16,
        curr_rep_level: i16,
        paths: &mut HashMap<ColumnPath, (ColumnDescPtr, ColumnReader)>,
        batch_size: usize,
    ) -> Self::Reader {
        let col_path = ColumnPath::new(path.to_vec());
        let (col_descr, col_reader) = paths.remove(&col_path).unwrap();
        assert_eq!(
            (curr_def_level, curr_rep_level),
            (col_descr.max_def_level(), col_descr.max_rep_level())
        );
        F64Reader {
            column: TypedTripletIter::<DoubleType>::new(
                curr_def_level,
                curr_rep_level,
                batch_size,
                col_reader,
            ),
        }
    }
}

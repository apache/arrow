use std::sync::{Mutex, Arc};
use std::thread;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use std::thread::JoinHandle;
use crate::execution::expression::CompiledExpr;

pub type Partition = Arc<Mutex<RBIterator>>;

pub trait RBIterator: Send {
    fn next(&mut self) -> Result<Option<RecordBatch>>;
}

struct MockPartition {
    index: usize,
    data: Vec<RecordBatch>
}

impl RBIterator for MockPartition {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        if self.index < self.data.len() {
            self.index += 1;
            Ok(Some(self.data[self.index-1].clone()))
        } else {
            Ok(None)
        }

    }
}

pub trait ExecutionPlan {
    fn execute(&mut self) -> Result<Vec<Partition>>;
}

struct FilterExec {
    input: Vec<Partition>,
    expr: CompiledExpr
}

impl ExecutionPlan for FilterExec {
    fn execute(&mut self) -> Result<Vec<Partition>> {

        let threads: Vec<JoinHandle<Partition>> = self.input.iter().map(|p| {
            let p = p.clone();
            thread::spawn(move || {
                p.clone()
            })
        }).collect();

        for t in threads {
            match t.join() {
                Ok(x) => {

                }
                _ => panic!()
            }
        }

        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{Field, DataType};
    use arrow::builder::UInt32Builder;

    #[test]
    fn test() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt32, false)
        ]));

        let mut array_builder = UInt32Builder::new(10);
        array_builder.append_value(123).unwrap();
        let array = array_builder.finish();

        let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
        let x = MockPartition {
            index: 0,
            data: vec![b]
        };

        let mut plan = FilterExec {
            input: vec![Arc::new(Mutex::new(x))],
            expr: CompiledExpr {
                name: "".to_string(),
                f: Arc::new(|b| {
                    panic!()
                }),
                t: DataType::Boolean
            }
        };

        plan.execute().unwrap();

    }

}

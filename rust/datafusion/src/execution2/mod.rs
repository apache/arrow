use std::sync::{Mutex, Arc};
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use std::thread;
use std::rc::Rc;
use std::cell::RefCell;
use std::iter::Iterator;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use std::thread::JoinHandle;
use crate::execution::datasource::DataSourceRelation;
use crate::execution::expression::CompiledExpr;
use crate::datasource::RecordBatchIterator;
use crate::datasource::parquet::ParquetTable;
use crate::datasource::datasource::TableProvider;

//pub trait Partition {
//    fn execute(&mut self) -> Rc<RefCell<Iterator<Item=Result<RecordBatch>>>>;
//}
//
//pub trait ExecutionPlan {
//    fn schema(&self) -> Arc<Schema>;
//    fn partitions(&self) -> Vec<Rc<RefCell<Partition>>>;
//}
//
//pub struct TableExec {
//    filenames: Vec<String>
//}
//
//impl ExecutionPlan for TableExec {
//
//    fn schema(&self) -> Arc<Schema> {
//        unimplemented!()
//    }
//
//    fn partitions(&self) -> Vec<Rc<RefCell<Partition>>> {
//        self.filenames.iter().map(|f|
//            Rc::new(RefCell::new(TablePartition { filename: f.to_string() })) as Rc<RefCell<Partition>>
//        ).collect()
//    }
//}
//
//pub struct TablePartition {
//    filename: String
//}
//
//impl Partition for TablePartition {
//    fn execute(&mut self) -> Rc<RefCell<Iterator<Item=Result<RecordBatch>>>> {
//        Rc::new(RefCell::new(TablePartitionIterator::new(&self.filename)))
//    }
//}
//
//pub struct TablePartitionIterator {
//    request_tx: Sender<()>,
//    response_rx: Receiver<RecordBatch>,
//}
//
//impl TablePartitionIterator {
//
//    fn new(filename: &str) -> Self {
//        let (request_tx, request_rx): (Sender<()>, Receiver<()>) = mpsc::channel();
//        let (response_tx, response_rx): (Sender<RecordBatch>, Receiver<RecordBatch>) = mpsc::channel();
//
//        let filename = filename.to_string();
//        thread::spawn(move || {
//            let table = ParquetTable::try_new(&filename).unwrap();
//            let partitions = table.scan(&None, 1024).unwrap();
//            let partition = partitions[0].clone();
//            loop {
//                let x = request_rx.recv().unwrap();
//                let batch = partition.lock().unwrap().next().unwrap().unwrap();
//                response_tx.send(batch).unwrap();
//            }
//        });
//
//        Self { request_tx, response_rx }
//    }
//
//}
//impl Iterator for TablePartitionIterator {
//    type Item = Result<RecordBatch>;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        self.request_tx.send(()).unwrap();
//        Some(Ok(self.response_rx.recv().unwrap()))
//    }
//}

//struct FilterExec {
//    input: Vec<Partition>,
//    expr: CompiledExpr
//}
//
//impl ExecutionPlan for FilterExec {
//
//    fn execute(&mut self) -> Result<Vec<Partition>> {
//
//        let threads: Vec<JoinHandle<Partition>> = self.input.iter().map(|p| {
//            let p = p.clone();
//            thread::spawn(move || {
//                p.clone()
//            })
//        }).collect();
//
//        let mut result = vec![];
//        for t in threads {
//            match t.join() {
//                Ok(x) => {
//                    result.push(x);
//                }
//                _ => panic!()
//            }
//        }
//
//        Ok(result)
//    }
//}

#[cfg(test)]
mod test {
//    use super::*;
//    use arrow::datatypes::{Field, DataType};
//    use arrow::builder::UInt32Builder;
//    use core::borrow::BorrowMut;
//    use crate::execution2::Partition;

//    #[test]
//    fn test() {
//        let schema = Arc::new(Schema::new(vec![
//            Field::new("a", DataType::UInt32, false)
//        ]));
//
//        let mut array_builder = UInt32Builder::new(10);
//        array_builder.append_value(123).unwrap();
//        let array = array_builder.finish();
//
//        let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
//        let x = MockPartition {
//            index: 0,
//            data: vec![b]
//        };
//
//        let mut plan = FilterExec {
//            input: vec![Arc::new(Mutex::new(x))],
//            expr: CompiledExpr {
//                name: "".to_string(),
//                f: Arc::new(|b| {
//                    panic!()
//                }),
//                t: DataType::Boolean
//            }
//        };
//
//        plan.execute().unwrap();
//
//    }

//    #[test]
//    fn thread_ds() {
//
//        use super::Partition;
//
//        let p = TableExec {
//            filenames: vec![
//                "/home/andy/git/andygrove/arrow/cpp/submodules/parquet-testing/data/alltypes_plain.parquet".to_string(),
//                "/home/andy/git/andygrove/arrow/cpp/submodules/parquet-testing/data/alltypes_plain.parquet".to_string()
//            ]
//        };
//
//        for i in 0..1 {
//            let partitions = p.partitions();
//            let mut xx = partitions[0].clone();
//            xx.borrow_mut().execute();


//            let yy = xx.borrow_mut();
//            let batch = yy.borrow_mut().next().unwrap().unwrap();
//            println!("batch has {} rows", batch.num_rows());
//        }

//    }

}

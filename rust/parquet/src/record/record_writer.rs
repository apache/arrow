use super::super::file::writer::RowGroupWriter;

pub trait RecordWriter<T> {
  fn write_to_row_group(&self, row_group_writer: &mut Box<RowGroupWriter>);
}
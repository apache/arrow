// impl Deserialize for parquet::data_type::Decimal {
//  type Schema = DecimalSchema;
// type Reader = Reader;
// fn placeholder() -> Self::Schema {
//
// }
// fn parse(schema: &Type) -> Result<(String,Self::Schema),ParquetError> {
//  unimplemented!()
// }
//  fn render(name: &str, schema: &Self::Schema) -> Type {
//    Type::primitive_type_builder(name, PhysicalType::DOUBLE)
//  .with_repetition(Repetition::REQUIRED)
//  .with_logical_type(LogicalType::NONE)
//  .with_length(-1)
//  .with_precision(-1)
//  .with_scale(-1)
//  .build().unwrap()
// Type::PrimitiveType {
//      basic_info: BasicTypeInfo {
//        name: String::from(schema),
//        repetition: Some(Repetition::REQUIRED),
//        logical_type: LogicalType::DECIMAL,
//        id: None,
//      }
//      physical_type: PhysicalType::
//  }
// }
// struct DecimalSchema {
//  scale: u32,
//  precision: u32,
// }

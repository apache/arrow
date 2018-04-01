#include <Rcpp.h>
#include "arrow_types.h"

// [[Rcpp::plugins(cpp11)]]

using namespace Rcpp ;

template <typename... String >
xptr_DataType metadata( const std::shared_ptr<arrow::DataType>& ptr, String... strings ){
  xptr_DataType res( new std::shared_ptr<arrow::DataType>(ptr) ) ;
  res.attr("class") = CharacterVector::create( ptr->name(), strings... ) ;
  return res ;
}

xptr_DataType metadata_integer( const std::shared_ptr<arrow::DataType>& ptr ){
  return metadata( ptr, "arrow::Integer", "arrow::Number", "arrow::PrimitiveCType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

//' @export
// [[Rcpp::export]]
xptr_DataType int8(){ return metadata_integer(arrow::int8()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType int16(){ return metadata_integer(arrow::int16()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType int32(){ return metadata_integer(arrow::int32()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType int64(){ return metadata_integer(arrow::int64()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType uint8(){ return metadata_integer(arrow::uint8()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType uint16(){ return metadata_integer(arrow::uint16()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType uint32(){ return metadata_integer(arrow::uint32()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType uint64(){ return metadata_integer(arrow::uint64()) ; }

xptr_DataType metadata_float( const std::shared_ptr<arrow::DataType>& ptr ){
  return metadata( ptr, "arrow::FloatingPoint", "arrow::Number", "arrow::PrimitiveCType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

//' @export
// [[Rcpp::export]]
xptr_DataType float16(){ return metadata_float(arrow::float16()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType float32(){ return metadata_float(arrow::float32()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType float64(){ return metadata_float(arrow::float64()) ; }

//' @export
// [[Rcpp::export]]
xptr_DataType boolean(){
  return metadata( arrow::boolean(), "arrow::BooleanType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

//' @export
// [[Rcpp::export]]
xptr_DataType utf8(){
  return metadata( arrow::utf8(), "arrow::StringType", "arrow::BinaryType", "arrow::DataType" ) ;
}

// binary ?

xptr_DataType metadata_date( const std::shared_ptr<arrow::DataType>& ptr ){
  return metadata( ptr, "arrow::DateType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

//' @export
// [[Rcpp::export]]
xptr_DataType date32(){
  return metadata_date( arrow::date32() ) ;
}

//' @export
// [[Rcpp::export]]
xptr_DataType date64(){
  return metadata_date(arrow::date64() ) ;
}

//' @export
// [[Rcpp::export]]
xptr_DataType null(){
  return metadata( arrow::null(), "arrow::NullType", "arrow::DataType" ) ;
}

//' @export
// [[Rcpp::export]]
xptr_DataType decimal_type(int32_t precision, int32_t scale){
  return metadata( arrow::decimal(precision, scale) , "arrow::NullType", "arrow::DataType" ) ;
}

//' @export
// [[Rcpp::export]]
xptr_DataType fixed_size_binary(int32_t byte_width){
  return metadata( arrow::fixed_size_binary(byte_width), "arrow::FixedSizeBinaryType", "arrow::FixedWidthType", "arrow::DataType") ;
}

namespace Rcpp {
  template <>
  arrow::TimeUnit::type as<arrow::TimeUnit::type>( SEXP x ){
    if( !Rf_inherits(x, "arrow::TimeUnit::type") ) stop("incompatible") ;
    return static_cast<arrow::TimeUnit::type>(as<int>(x)) ;
  }
}

// [[Rcpp::export]]
xptr_DataType timestamp1(arrow::TimeUnit::type unit){
  return metadata( arrow::timestamp(unit),
    "arrow::TimestampType", "arrow::FixedWidthType", "arrow::DataType"
  ) ;
}

// [[Rcpp::export]]
xptr_DataType timestamp2(arrow::TimeUnit::type unit, const std::string& timezone ){
  return metadata( arrow::timestamp(unit, timezone),
    "arrow::TimestampType", "arrow::FixedWidthType", "arrow::DataType"
  ) ;
}

//' @export
// [[Rcpp::export(name="time32")]]
xptr_DataType time32_(arrow::TimeUnit::type unit){
  return metadata(
    arrow::time32(unit),
    "arrow::Time32Type", "arrow::TimeType", "arrow::FixedWidthType", "arrow::DataType"
  ) ;
}

//' @export
// [[Rcpp::export(name="time64")]]
xptr_DataType time64_(arrow::TimeUnit::type unit){
  return metadata(
    arrow::time64(unit),
    "arrow::Time64Type", "arrow::TimeType", "arrow::FixedWidthType", "arrow::DataType"
  ) ;
}

//' @export
// [[Rcpp::export]]
SEXP list_( SEXP x ){
  if( Rf_inherits(x, "arrow::Field")){
    return metadata(
      arrow::list(*xptr_Field(x)),
      "arrow::ListType", "arrow::NestedType", "arrow::DataType"
    ) ;
  }

  if( Rf_inherits(x, "arrow::DataType") ){
    return metadata(
      arrow::list(*xptr_DataType(x)),
      "arrow::ListType", "arrow::NestedType", "arrow::DataType"
    ) ;
  }

  stop("incompatible") ;
  return R_NilValue ;
}

//' @export
// [[Rcpp::export]]
xptr_DataType struct_( ListOf<xptr_Field> fields ){

  int n = fields.size() ;
  std::vector<std::shared_ptr<arrow::Field>> vec_fields ;
  for(int i=0; i<n; i++){
    vec_fields.emplace_back( *fields[i] ) ;
  }

  std::shared_ptr<arrow::DataType> s( arrow::struct_(vec_fields) ) ;
  return metadata(s, "arrow::StructType", "arrow::NestedType", "arrow::DataType") ;

}

// [[Rcpp::export]]
std::string DataType_ToString( xptr_DataType type){
  std::shared_ptr<arrow::DataType> ptr(*type) ;
  return ptr->ToString() ;
}

// [[Rcpp::export]]
xptr_Schema schema_( ListOf<xptr_Field> fields ){

  int n = fields.size() ;
  std::vector<std::shared_ptr<arrow::Field>> vec_fields ;
  for(int i=0; i<n; i++){
    vec_fields.emplace_back( *fields[i] ) ;
  }

  xptr_Schema s( new std::shared_ptr<arrow::Schema>( arrow::schema(vec_fields) ) );
  s.attr("class") = CharacterVector::create( "arrow::Schema" ) ;
  return s ;
}

// [[Rcpp::export]]
std::string Schema_ToString( xptr_Schema type){
  std::shared_ptr<arrow::Schema> ptr(*type) ;
  return ptr->ToString() ;
}

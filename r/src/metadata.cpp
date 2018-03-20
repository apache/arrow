#include <Rcpp.h>
#include "rrrow_types.h"

// [[Rcpp::plugins(cpp11)]]

using namespace Rcpp ;

template <typename Fun, typename... String >
xptr_DataType metadata( Fun fun, String... strings ){
  auto ptr = fun() ;
  xptr_DataType res( new std::shared_ptr<arrow::DataType>(ptr) ) ;
  res.attr("class") = CharacterVector::create( ptr->name(), strings... ) ;
  return res ;
}

xptr_DataType metadata_integer( std::shared_ptr<arrow::DataType> (*fun)() ){
  return metadata( fun, "arrow::Integer", "arrow::Number", "arrow::PrimitiveCType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

// [[Rcpp::export]]
xptr_DataType int8(){ return metadata_integer(arrow::int8) ; }

// [[Rcpp::export]]
xptr_DataType int16(){ return metadata_integer(arrow::int16) ; }

// [[Rcpp::export]]
xptr_DataType int32(){ return metadata_integer(arrow::int32) ; }

// [[Rcpp::export]]
xptr_DataType int64(){ return metadata_integer(arrow::int64) ; }

// [[Rcpp::export]]
xptr_DataType uint8(){ return metadata_integer(arrow::uint8) ; }

// [[Rcpp::export]]
xptr_DataType uint16(){ return metadata_integer(arrow::uint16) ; }

// [[Rcpp::export]]
xptr_DataType uint32(){ return metadata_integer(arrow::uint32) ; }

// [[Rcpp::export]]
xptr_DataType uint64(){ return metadata_integer(arrow::uint64) ; }

xptr_DataType metadata_float( std::shared_ptr<arrow::DataType> (*fun)()){
  return metadata( fun, "arrow::FloatingPoint", "arrow::Number", "arrow::PrimitiveCType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

// [[Rcpp::export]]
xptr_DataType float16(){ return metadata_float(arrow::float16) ; }

// [[Rcpp::export]]
xptr_DataType float32(){ return metadata_float(arrow::float32) ; }

// [[Rcpp::export]]
xptr_DataType float64(){ return metadata_float(arrow::float64) ; }

// [[Rcpp::export]]
xptr_DataType boolean(){
  return metadata( arrow::boolean, "arrow::BooleanType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

// [[Rcpp::export]]
xptr_DataType utf8(){
  return metadata( arrow::utf8, "arrow::StringType", "arrow::BinaryType", "arrow::DataType" ) ;
}

// binary ?

xptr_DataType metadata_date( std::shared_ptr<arrow::DataType> (*fun)() ){
  return metadata( fun, "arrow::DateType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

// [[Rcpp::export]]
xptr_DataType date32(){
  return metadata_date( arrow::date32 ) ;
}

// [[Rcpp::export]]
xptr_DataType date64(){
  return metadata_date(arrow::date64 ) ;
}

// [[Rcpp::export]]
xptr_DataType null(){
  return metadata( arrow::null, "arrow::NullType", "arrow::DataType" ) ;
}

// [[Rcpp::export]]
xptr_DataType decimal_type(int32_t precision, int32_t scale){
  return metadata( std::bind(arrow::decimal, precision, scale) , "arrow::NullType", "arrow::DataType" ) ;
}

// [[Rcpp::export]]
xptr_DataType fixed_size_binary(int32_t byte_width){
  return metadata( std::bind(arrow::fixed_size_binary, byte_width), "arrow::FixedSizeBinaryType", "arrow::FixedWidthType", "arrow::DataType") ;
}

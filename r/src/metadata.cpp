#include <Rcpp.h>
#include "rrrow_types.h"

// [[Rcpp::plugins(cpp11)]]

using namespace Rcpp ;

template <typename std::shared_ptr<arrow::DataType> (*fun)(), typename... String >
xptr_DataType metadata( String... strings ){
  auto ptr = fun() ;
  xptr_DataType res( new std::shared_ptr<arrow::DataType>(ptr) ) ;
  res.attr("class") = CharacterVector::create( ptr->name(), strings... ) ;
  return res ;
}

template <typename std::shared_ptr<arrow::DataType> (*fun)() >
xptr_DataType metadata_integer( ){
  return metadata<fun>( "arrow::Integer", "arrow::Number", "arrow::PrimitiveCType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

// [[Rcpp::export]]
xptr_DataType int8(){ return metadata_integer<arrow::int8>() ; }

// [[Rcpp::export]]
xptr_DataType int16(){ return metadata_integer<arrow::int16>() ; }

// [[Rcpp::export]]
xptr_DataType int32(){ return metadata_integer<arrow::int32>() ; }

// [[Rcpp::export]]
xptr_DataType int64(){ return metadata_integer<arrow::int64>() ; }

// [[Rcpp::export]]
xptr_DataType uint8(){ return metadata_integer<arrow::uint8>() ; }

// [[Rcpp::export]]
xptr_DataType uint16(){ return metadata_integer<arrow::uint16>() ; }

// [[Rcpp::export]]
xptr_DataType uint32(){ return metadata_integer<arrow::uint32>() ; }

// [[Rcpp::export]]
xptr_DataType uint64(){ return metadata_integer<arrow::uint64>() ; }

template <typename std::shared_ptr<arrow::DataType> (*fun)() >
xptr_DataType metadata_float( ){
  return metadata<fun>( "arrow::FloatingPoint", "arrow::Number", "arrow::PrimitiveCType", "arrow::FixedWidthType", "arrow::DataType" ) ;
}

// [[Rcpp::export]]
xptr_DataType float16(){ return metadata_float<arrow::float16>() ; }

// [[Rcpp::export]]
xptr_DataType float32(){ return metadata_float<arrow::float32>() ; }

// [[Rcpp::export]]
xptr_DataType float64(){ return metadata_float<arrow::float64>() ; }


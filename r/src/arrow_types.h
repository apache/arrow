#pragma once

#include <Rcpp.h>
#undef Free
#include <arrow/api.h>
#include <arrow/type.h>

using xptr_DataType  = Rcpp::XPtr<std::shared_ptr<arrow::DataType>> ;
using xptr_Field     = Rcpp::XPtr<std::shared_ptr<arrow::Field>> ;
using xptr_Schema    = Rcpp::XPtr<std::shared_ptr<arrow::Schema>> ;
using xptr_ArrayBuilder = Rcpp::XPtr<std::unique_ptr<arrow::ArrayBuilder>> ;

namespace Rcpp{
  template <>
  arrow::TimeUnit::type as<arrow::TimeUnit::type>( SEXP ) ;
}

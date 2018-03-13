#pragma once

#include <Rcpp.h>
#undef Free
#include <arrow/api.h>
#include <arrow/type.h>

using array_ptr = Rcpp::XPtr<std::shared_ptr<arrow::Array>> ;
using type_ptr  = Rcpp::XPtr<std::shared_ptr<arrow::DataType>> ;

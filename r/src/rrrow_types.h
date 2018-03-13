#pragma once

#include <Rcpp.h>
#undef Free
#include <arrow/api.h>
#include <arrow/type.h>

using xptr_DataType  = Rcpp::XPtr<std::shared_ptr<arrow::DataType>> ;

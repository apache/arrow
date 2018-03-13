#pragma once

#include <Rcpp.h>
#undef Free
#include <arrow/api.h>

typedef Rcpp::XPtr<std::shared_ptr<arrow::Array>> array_ptr;

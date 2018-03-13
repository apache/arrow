#include <Rcpp.h>
#include "rrrow_types.h"

// [[Rcpp::plugins(cpp11)]]

using namespace Rcpp ;

// [[Rcpp::export]]
type_ptr int32(){
  auto ptr = arrow::int32();
  return type_ptr( new std::shared_ptr<arrow::DataType>(ptr) ) ;
}

#include <Rcpp.h>
#include "rrrow_types.h"

// [[Rcpp::plugins(cpp11)]]

using namespace Rcpp ;

//' @export
// [[Rcpp::export]]
xptr_Field field( const std::string& name, xptr_DataType type, bool nullable = true){
  auto ptr = arrow::field(name, *type, nullable) ;
  xptr_Field res( new std::shared_ptr<arrow::Field>(ptr) ) ;
  res.attr("name")  = ptr->name() ;
  res.attr("class") = CharacterVector::create( "arrow::Field" ) ;
  return res ;
}

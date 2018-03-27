#include <Rcpp.h>
#include "rrrow_types.h"

// [[Rcpp::plugins(cpp11)]]

using namespace Rcpp ;

//' @export
// [[Rcpp::export]]
xptr_ArrayBuilder ArrayBuilder( xptr_DataType xptr_type ){
  if( !Rf_inherits(xptr_type, "arrow::DataType") )
    stop("incompatible") ;

  std::shared_ptr<arrow::DataType>& type = *xptr_type ;

  auto memory_pool = arrow::default_memory_pool();
  std::unique_ptr<arrow::ArrayBuilder>* arrow_builder = new std::unique_ptr<arrow::ArrayBuilder> ;
  auto status = arrow::MakeBuilder( memory_pool, type, arrow_builder) ;

  xptr_ArrayBuilder res(arrow_builder) ;
  res.attr("class") = CharacterVector::create( DEMANGLE(**arrow_builder), "arrow::ArrayBuilder") ;
  return res ;

}

// [[Rcpp::export]]
int ArrayBuilder__num_children( xptr_ArrayBuilder xptr_type ){
  return  (*xptr_type)->num_children() ;
}

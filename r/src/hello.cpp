#include <Rcpp.h>
#include "rrrow_types.h"

using namespace Rcpp ;
using namespace arrow ;

// [[Rcpp::export]]
int test(){
  arrow::Int32Builder builder ;

  builder.Append(1);
  builder.Append(2);
  builder.Append(3);
  builder.AppendNull();
  builder.Append(5);
  builder.Append(6);
  builder.Append(7);

  return builder.length() ;
}

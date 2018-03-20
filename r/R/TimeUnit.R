TimeUnit <- structure(
  list( SECOND = 0L, MILLI = 1L, MICRO = 2L, NANO = 3L),
  class = c( "arrow::TimeUnit::type", "arrow-enum" )
)

timestamp <- function(unit, timezone){
  if( missing(timezone)){
    timestamp1(unit)
  } else {
    timestamp2(unit, timezone)
  }
}

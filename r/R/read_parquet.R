#' Read parquet file from disk
#' 
#' @param files a vector of filenames
#' @export
read_parquet = function(files) {
  tables = lapply(files, function(f) {
    return (as_tibble(shared_ptr(`arrow::Table`, read_parquet_file(f))))
  })
  do.call('rbind', tables)
}

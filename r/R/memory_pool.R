#' @include R6.R

`arrow::MemoryPool` <- R6Class("arrow::MemoryPool",
  inherit = `arrow::Object`,
  public = list(
    initialize = function(xp){
      if(!missing(xp)) self$set_pointer(xp)
    },
    # TODO: Allocate
    # TODO: Reallocate
    # TODO: Free
    bytes_allocated = function() MemoryPool_bytes_allocated(self),
    max_memory = function() MemoryPool_max_memory(self)
  )
)

default_memory_pool <- function() {
  `arrow::MemoryPool`$new(MemoryPool_default())
}

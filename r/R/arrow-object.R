# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#' @include enums.R
ArrowObject <- R6Class("ArrowObject",
  public = list(
    initialize = function(xp) self$set_pointer(xp),
    pointer = function() get(".:xp:.", envir = self),
    `.:xp:.` = NULL,
    set_pointer = function(xp) {
      if (!inherits(xp, "externalptr")) {
        stop(
          class(self)[1], "$new() requires a pointer as input: ",
          "did you mean $create() instead?",
          call. = FALSE
        )
      }
      assign(".:xp:.", xp, envir = self)
    },
    class_title = function() {
      if (".class_title" %in% ls(self, all.names = TRUE)) {
        # Allow subclasses to override just printing the class name first
        class_title <- self$.class_title()
      } else {
        class_title <- class(self)[[1]]
      }
    },
    print = function(...) {
      cat(self$class_title(), "\n", sep = "")
      if (!is.null(self$ToString)) {
        cat(self$ToString(), "\n", sep = "")
      }
      invisible(self)
    },
    .unsafe_delete = function() {
      # The best we can do in a generic way is to set the underlying
      # pointer to NULL. Subclasses specialize this so that we can actually
      # call the underlying shared pointer's reset() method for the
      # shared_ptr<SubclassType> in C++.
      self$`.:xp:.` <- NULL

      # Return NULL, because keeping this R6 object in scope is not a good idea.
      # This syntax would allow the rare use that has to actually do this to
      # do `object <- object$.unsafe_delete()` and reduce the chance that an
      # IDE like RStudio will try try to call other methods which will error
      invisible(NULL)
    }
  )
)

#' @export
`!=.ArrowObject` <- function(lhs, rhs) !(lhs == rhs) # nolint

#' @export
`==.ArrowObject` <- function(x, y) { # nolint
  x$Equals(y)
}

#' @export
all.equal.ArrowObject <- function(target, current, ..., check.attributes = TRUE) {
  target$Equals(current, check_metadata = check.attributes)
}

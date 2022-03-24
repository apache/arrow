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

#' Load a Python Flight server
#'
#' @param name string Python module name
#' @param path file system path where the Python module is found. Default is
#' to look in the `inst/` directory for included modules.
#' @export
#' @examplesIf FALSE
#' load_flight_server("demo_flight_server")
load_flight_server <- function(name, path = system.file(package = "arrow")) {
  reticulate::import_from_path(name, path)
}

#' Connect to a Flight server
#'
#' @param host string hostname to connect to
#' @param port integer port to connect on
#' @param scheme URL scheme, default is "grpc+tcp"
#' @return A `pyarrow.flight.FlightClient`.
#' @export
flight_connect <- function(host = "localhost", port, scheme = "grpc+tcp") {
  pa <- reticulate::import("pyarrow")
  location <- paste0(scheme, "://", host, ":", port)
  pa$flight$FlightClient(location)
}

#' Send data to a Flight server
#'
#' @param client `pyarrow.flight.FlightClient`, as returned by [flight_connect()]
#' @param data `data.frame`, [RecordBatch], or [Table] to upload
#' @param path string identifier to store the data under
#' @param overwrite logical: if `path` exists on `client` already, should we
#' replace it with the contents of `data`? Default is `TRUE`; if `FALSE` and
#' `path` exists, the function will error.
#' @return `client`, invisibly.
#' @export
flight_put <- function(client, data, path, overwrite = TRUE) {
  assert_is(data, c("data.frame", "Table", "RecordBatch"))

  if (!overwrite && flight_path_exists(client, path)) {
    stop(path, " exists.", call. = FALSE)
  }
  if (is.data.frame(data)) {
    data <- Table$create(data)
  }

  py_data <- reticulate::r_to_py(data)
  writer <- client$do_put(descriptor_for_path(path), py_data$schema)[[1]]
  if (inherits(data, "RecordBatch")) {
    writer$write_batch(py_data)
  } else {
    writer$write_table(py_data)
  }
  writer$close()
  invisible(client)
}

#' Get data from a Flight server
#'
#' @param client `pyarrow.flight.FlightClient`, as returned by [flight_connect()]
#' @param path string identifier under which data is stored
#' @return A [Table]
#' @export
flight_get <- function(client, path) {
  reader <- flight_reader(client, path)
  reader$read_all()
}

# TODO: could use this as a RecordBatch iterator, call $read_chunk() on this
flight_reader <- function(client, path) {
  info <- client$get_flight_info(descriptor_for_path(path))
  # Hack: assume a single ticket, on the same server as client is already connected
  ticket <- info$endpoints[[1]]$ticket
  client$do_get(ticket)
}

descriptor_for_path <- function(path) {
  pa <- reticulate::import("pyarrow")
  pa$flight$FlightDescriptor$for_path(path)
}

#' See available resources on a Flight server
#'
#' @inheritParams flight_get
#' @return `list_flights()` returns a character vector of paths.
#' `flight_path_exists()` returns a logical value, the equivalent of `path %in% list_flights()`
#' @export
list_flights <- function(client) {
  generator <- client$list_flights()
  out <- reticulate::iterate(generator, function(x) as.character(x$descriptor$path[[1]]))
  out
}

#' @rdname list_flights
#' @export
flight_path_exists <- function(client, path) {
  it_exists <- tryCatch(
    expr = {
      client$get_flight_info(descriptor_for_path(path))
      TRUE
    },
    error = function(e) {
      msg <- conditionMessage(e)
      if (!any(grepl("ArrowKeyError", msg))) {
        # Raise an error if this fails for any reason other than not found
        stop(e)
      }
      FALSE
    }
  )
}

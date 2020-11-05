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
#' @param data `data.frame` or [RecordBatch] to upload
#' @param path string identifier to store the data under
#' @return `client`, invisibly.
#' @export
push_data <- function(client, data, path) {
  if (inherits(data, "data.frame")) {
    data <- record_batch(data)
  }
  # TODO: this is only RecordBatch; handle Table
  py_data <- reticulate::r_to_py(data)
  writer <- client$do_put(descriptor_for_path(path), py_data$schema)[[1]]
  writer$write_batch(py_data)
  writer$close()
  invisible(client)
}

#' Get data from a Flight server
#'
#' @param client `pyarrow.flight.FlightClient`, as returned by [flight_connect()]
#' @param path string identifier under which the data is stored
#' @return A [RecordBatch]
#' @export
flight_get <- function(client, path) {
  info <- client$get_flight_info(descriptor_for_path(path))
  # Hack: assume a single ticket, on the same server as client is already connected
  ticket <- info$endpoints[[1]]$ticket
  reader <- client$do_get(ticket)
  # Next hack: assume a single record batch
  # TODO: read_all() instead? Or read all chunks and build Table in R?
  chunk <- reader$read_chunk()
  # Drop $app_metadata and just return the data
  chunk$data
}

descriptor_for_path <- function(path) {
  pa <- reticulate::import("pyarrow")
  pa$flight$FlightDescriptor$for_path(path)
}

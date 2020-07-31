#' Connect to a Flight server
#'
#' @param host string hostname to connect to
#' @param port integer port to connect on
#' @param scheme URL scheme, default is "grpc+tcp"
#' @return A `pyarrow.flight.FlightClient`.
#' @importFrom reticulate import
#' @export
flight_connect <- function(host = "localhost", port, scheme = "grpc+tcp") {
  pa <- import("pyarrow")
  location <- paste0(scheme, "://", host, ":", port)
  pa$flight$FlightClient(location)
}

#' Send data to a Flight server
#'
#' @param client `pyarrow.flight.FlightClient`, as returned by [flight_connect()]
#' @param data `data.frame` or [arrow::RecordBatch] to upload
#' @param path string identifier to store the data under
#' @return `client`, invisibly.
#' @importFrom arrow record_batch
#' @importFrom reticulate r_to_py
#' @export
push_data <- function(client, data, path) {
  if (inherits(data, "data.frame")) {
    data <- record_batch(data)
  }
  # TODO: this is only RecordBatch; handle Table
  py_data <- r_to_py(data)
  writer <- client$do_put(descriptor_for_path(path), py_data$schema)[[1]]
  writer$write_batch(py_data)
  writer$close()
  invisible(client)
}

#' Get data from a Flight server
#'
#' @param client `pyarrow.flight.FlightClient`, as returned by [flight_connect()]
#' @param path string identifier under which the data is stored
#' @return An [arrow::RecordBatch]
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
  pa <- import("pyarrow")
  pa$flight$FlightDescriptor$for_path(path)
}

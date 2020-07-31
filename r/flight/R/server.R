#' Load a Python Flight server
#'
#' @param name string Python module name
#' @param path file system path where the Python module is found. Default is
#' to look in the `inst/` directory for included modules.
#' @importFrom reticulate import_from_path
#' @export
load_flight_server <- function(name, path = system.file(package = "flight")) {
  import_from_path(name, path)
}

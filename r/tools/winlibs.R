# Download static arrow from rwinlib
VERSION <- commandArgs(TRUE)
if(!file.exists(sprintf("../windows/arrow-%s/include/arrow/api.h", VERSION))){
  if(getRversion() < "3.3.0") setInternet2()
  download.file(sprintf("https://github.com/rwinlib/arrow/archive/v%s.zip", VERSION), "lib.zip", quiet = TRUE)
  dir.create("../windows", showWarnings = FALSE)
  unzip("lib.zip", exdir = "../windows")
  unlink("lib.zip")
}

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

VERSION=$(shell grep ^Version DESCRIPTION | sed s/Version:\ //)
ARROW_R_DEV="TRUE"

doc:
	R --slave -e 'devtools::document(); rmarkdown::render("README.Rmd")'
	-git add --all man/*.Rd

test:
	export ARROW_R_DEV=$(ARROW_R_DEV) && R CMD INSTALL --install-tests .
	export NOT_CRAN=true && R --slave -e 'library(testthat); setwd(file.path(.libPaths()[1], "arrow", "tests")); system.time(test_check("arrow", filter="${file}", reporter=ifelse(nchar("${r}"), "${r}", "summary")))'

deps:
	R --slave -e 'lib <- Sys.getenv("R_LIB", .libPaths()[1]); install.packages("devtools", repo="https://cloud.r-project.org", lib=lib); devtools::install_dev_deps(lib=lib)'

build: doc
	cp ../NOTICE.txt inst/NOTICE.txt
	R CMD build .

check: build
	-export _R_CHECK_CRAN_INCOMING_REMOTE_=FALSE && export ARROW_R_DEV=$(ARROW_R_DEV) && R CMD check --as-cran arrow_$(VERSION).tar.gz
	rm -rf arrow.Rcheck/

clean:
	-rm src/*.o
	-rm src/*.so
	-rm src/*.dll
	-rm src/Makevars
	-rm src/Makevars.win
	-rm -rf arrow.Rcheck/

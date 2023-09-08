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
ARROW_LARGE_MEMORY_TESTS=$(ARROW_R_DEV)

style:
	R -s -e 'setwd(".."); if (requireNamespace("styler")) styler::style_file(setdiff(system("git diff --name-only | grep r/.*R$$", intern = TRUE), file.path("r", source("r/.styler_excludes.R")$$value)))'

style-all:
	R -s -e 'styler::style_file(setdiff(dir(pattern = "R$$", recursive = TRUE), source(".styler_excludes.R")$$value))'

doc: style
	R -s -f data-raw/docgen.R
	R -s -e 'roxygen2::roxygenize()'
	-git add --all man/*.Rd

test:
	export NOT_CRAN=true && export ARROW_R_DEV=$(ARROW_R_DEV) && R CMD INSTALL --install-tests --no-test-load --no-docs --no-help --no-byte-compile .
	export NOT_CRAN=true && export ARROW_R_DEV=$(ARROW_R_DEV) && export AWS_EC2_METADATA_DISABLED=TRUE && export ARROW_LARGE_MEMORY_TESTS=$(ARROW_LARGE_MEMORY_TESTS) && R -s -e 'library(testthat); setwd(file.path(.libPaths()[1], "arrow", "tests")); system.time(test_check("arrow", filter="${file}", reporter=ifelse(nchar("${r}"), "${r}", "summary")))'

deps:
	R -s -e 'lib <- Sys.getenv("R_LIB", .libPaths()[1]); install.packages("devtools", repo="https://cloud.r-project.org", lib=lib); devtools::install_dev_deps(lib=lib)'

# Note: files in tools are available at build time, but not at run time. The thirdparty
# cmake expects .env, NOTICE.txt, and LICENSE.txt to be available one level up from cpp/
# we must rename .env to dotenv and then replace references to it in cpp/CMakeLists.txt
sync-cpp:
	cp ../NOTICE.txt inst/NOTICE.txt
	rsync --archive --delete --exclude 'apidoc' --exclude 'build' --exclude 'build-support/boost_*' --exclude 'examples' --exclude 'src/gandiva' --exclude 'src/jni' --exclude 'src/skyhook' --exclude 'submodules' --exclude '**/*_test.cc' ../cpp tools/
	cp -p ../.env tools/dotenv
	cp -p ../NOTICE.txt tools/
	cp -p ../LICENSE.txt tools/
	sed -i"" -e "s/\.env/dotenv/g" tools/cpp/CMakeLists.txt

build: doc sync-cpp
	R CMD build ${args} .

check: build
	-export _R_CHECK_CRAN_INCOMING_REMOTE_=FALSE && export ARROW_R_DEV=$(ARROW_R_DEV) && export _R_CHECK_TESTS_NLINES_=0 && R CMD check --as-cran --run-donttest arrow_$(VERSION).tar.gz
	rm -rf arrow.Rcheck/

release: build
	-export _R_CHECK_TESTS_NLINES_=0 && R CMD check --as-cran --run-donttest arrow_$(VERSION).tar.gz
	rm -rf arrow.Rcheck/

clean:
	-rm src/*.o
	-rm src/*.so
	-rm src/*.dll
	-rm src/Makevars
	-rm src/Makevars.win
	-rm -rf arrow.Rcheck/
	-rm -rf libarrow/
	-rm -rf tools/cpp/ tools/.env tools/NOTICE.txt tools/LICENSE.txt
	-find . -name "*.orig" -delete

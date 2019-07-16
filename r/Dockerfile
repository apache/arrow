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

FROM arrow:cpp

# Configure
ENV CC=gcc \
    CXX=g++

# r-base includes tzdata. Get around interactive stop in that package
ENV DEBIAN_FRONTEND=noninteractive
# workaround for install_github GitHub API rate limit
ENV CI=true

# Build R
# [1] https://www.digitalocean.com/community/tutorials/how-to-install-r-on-ubuntu-18-04
# [2] https://linuxize.com/post/how-to-install-r-on-ubuntu-18-04/#installing-r-packages-from-cran
RUN apt-get update -y && \
    apt-get install -y \
        apt-transport-https \
        software-properties-common && \
    apt-key adv \
        --keyserver keyserver.ubuntu.com \
        --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/' && \
    apt-get install -y r-base && \
    # system libs needed by core R packages
    apt-get install -y \
            libgit2-dev \
            libssl-dev && \
    # install clang to mirror what was done on Travis
    apt-get install -y \
            clang \
            clang-format \
            clang-tidy && \
    # R CMD CHECK --as-cran needs pdflatex to build the package manual
    apt-get install -y \
            texlive-latex-base && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# So that arrowExports.* files are generated
ENV ARROW_R_DEV=TRUE

# Tell R where it can find the source code for arrow
ENV PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:/build/cpp/src/arrow:/opt/conda/lib/pkgconfig
ENV LD_LIBRARY_PATH=/opt/conda/lib/:/build/cpp/src/arrow:/arrow/r/src

RUN Rscript -e "install.packages('devtools', repos = 'http://cran.rstudio.com')" && \
    Rscript -e "devtools::install_github('romainfrancois/decor')" && \
    Rscript -e "install.packages(c( \
        'Rcpp', 'dplyr', 'stringr', 'glue', 'vctrs', \
        'purrr', \
        'assertthat', \
        'fs', \
        'tibble', \
        'crayon', \
        'testthat', \
        'bit64', \
        'hms', \
        'lubridate'), \
        repos = 'https://cran.rstudio.com')"


# build, install, test R package
CMD ["/bin/bash", "-c", "/arrow/ci/docker_build_cpp.sh && \
    /arrow/ci/docker_build_r.sh"]

#!/bin/bash

Rscript -e "
options(repos = 'https://cloud.r-project.org/')
if (!require('assertthat'))  install.packages('assertthat')
if (!require('bit64'))  install.packages('bit64')
if (!require('purrr'))  install.packages('purrr')
if (!require('R6'))  install.packages('R6')
if (!require('rlang'))  install.packages('rlang')
if (!require('tidyselect'))  install.packages('tidyselect')
if (!require('vctrs'))  install.packages('vctrs')
if (!require('cpp11'))  install.packages('cpp11')
"

make clean && R CMD INSTALL . && make test


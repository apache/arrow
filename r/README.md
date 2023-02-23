# arrow <img src="https://arrow.apache.org/img/arrow-logo_hex_black-txt_white-bg.png" align="right" alt="" width="120" />

[![cran](https://www.r-pkg.org/badges/version-last-release/arrow)](https://cran.r-project.org/package=arrow)
[![CI](https://github.com/apache/arrow/workflows/R/badge.svg?event=push)](https://github.com/apache/arrow/actions?query=workflow%3AR+branch%3Amain+event%3Apush)
[![conda-forge](https://img.shields.io/conda/vn/conda-forge/r-arrow.svg)](https://anaconda.org/conda-forge/r-arrow)

[Apache Arrow](https://arrow.apache.org/) is a cross-language
development platform for in-memory and larger-than-memory data. It specifies a standardized
language-independent columnar memory format for flat and hierarchical
data, organized for efficient analytic operations on modern hardware. It
also provides computational libraries and zero-copy streaming, messaging,
and interprocess communication.

The arrow R package exposes an interface to the Arrow C++ library,
enabling access to many of its features in R. It provides low-level
access to the Arrow C++ library API and higher-level access through a
`{dplyr}` backend and familiar R functions.

## What can the arrow package do?

The arrow package provides functionality for a wide range of data analysis
tasks. It allows users to read and write data in a variety formats:

-   Read and write Parquet files, an efficient and widely used columnar format
-   Read and write Arrow (formerly known as Feather) files, a format optimized for speed and
    interoperability
-   Read and write CSV files with excellent speed and efficiency
-   Read and write multi-file and larger-than-memory datasets
-   Read JSON files

It provides data analysis tools for both in-memory and larger-than-memory data sets

-   Analyze and process larger-than-memory datasets
-   Manipulate and analyze Arrow data with dplyr verbs

It provides access to remote filesystems and servers

-   Read and write files in Amazon S3 and Google Cloud Storage buckets
-   Connect to Arrow Flight servers to transport large datasets over networks  
    
Additional features include:

-   Zero-copy data sharing between R and Python
-   Fine control over column types to work seamlessly
    with databases and data warehouses
-   Support for compression codecs including Snappy, gzip, Brotli,
    Zstandard, LZ4, LZO, and bzip2
-   Access and manipulate Arrow objects through low-level bindings
    to the C++ library
-   Toolkit for building connectors to other applications
    and services that use Arrow

## Installation

Most R users will probably want to install the latest release of arrow 
from CRAN:

``` r
install.packages("arrow")
```

Alternatively, if you are using conda you can install arrow from conda-forge:

``` shell
conda install -c conda-forge --strict-channel-priority r-arrow
```

In most cases installing the latest release should work without 
requiring any additional system dependencies, especially if you are using 
Window or a Mac. For those users, CRAN hosts binary packages that contain 
the Arrow C++ library upon which the arrow package relies, and no 
additional steps should be required.

There are some special cases to note:

- On Linux the installation process can sometimes be more involved because 
CRAN does not host binaries for Linux. For more information please see the [installation guide](https://arrow.apache.org/docs/r/articles/install.html).

- If you are compiling arrow from source, please note that as of version 
10.0.0, arrow requires C++17 to build. This has implications on Windows and
CentOS 7. For Windows users it means you need to be running an R version of 
4.0 or later. On CentOS 7, it means you need to install a newer compiler 
than the default system compiler gcc 4.8. See the [installation details article](https://arrow.apache.org/docs/r/articles/developers/install_details.html) for guidance. Note that 
this does not affect users who are installing a binary version of the package.

- Development versions of arrow are released nightly. Most users will not 
need to install nightly builds, but if you do please see the article on [installing nightly builds](https://arrow.apache.org/docs/r/articles/install_nightly.html) for more information.

## Arrow resources 

In addition to the official [Arrow R package documentation](https://arrow.apache.org/docs/r/), the [Arrow for R cheatsheet](https://github.com/apache/arrow/blob/-/r/cheatsheet/arrow-cheatsheet.pdf), and the [Apache Arrow R Cookbook](https://arrow.apache.org/cookbook/r/index.html) are useful resources for getting started with arrow.

## Getting help

If you encounter a bug, please file an issue with a minimal reproducible
example on [GitHub issues](https://github.com/apache/arrow/issues).
Log in to your GitHub account, click on **New issue** and select the type of
issue you want to create. Add a meaningful title prefixed with **`[R]`**
followed by a space, the issue summary and select component **R** from the
dropdown list. For more information, see the **Report bugs and propose
features** section of the [Contributing to Apache
Arrow](https://arrow.apache.org/docs/developers/contributing.html) page
in the Arrow developer documentation.

We welcome questions, discussion, and contributions from users of the
arrow package. For information about mailing lists and other venues
for engaging with the Arrow developer and user communities, please see
the [Apache Arrow Community](https://arrow.apache.org/community/) page.

Please note that all participation in the Apache Arrow project is 
governed by the Apache Software Foundation's [code of
conduct](https://www.apache.org/foundation/policies/conduct.html).

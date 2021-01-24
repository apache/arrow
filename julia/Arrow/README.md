# Arrow

[![docs](https://img.shields.io/badge/docs-latest-blue&logo=julia)](https://arrow.juliadata.org/dev/)
[![CI](https://github.com/JuliaData/Arrow.jl/workflows/CI/badge.svg)](https://github.com/JuliaData/Arrow.jl/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/JuliaData/Arrow.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaData/Arrow.jl)

[![deps](https://juliahub.com/docs/Arrow/deps.svg)](https://juliahub.com/ui/Packages/Arrow/QnF3w?t=2)
[![version](https://juliahub.com/docs/Arrow/version.svg)](https://juliahub.com/ui/Packages/Arrow/QnF3w)
[![pkgeval](https://juliahub.com/docs/Arrow/pkgeval.svg)](https://juliahub.com/ui/Packages/Arrow/QnF3w)

This is a pure Julia implementation of the [Apache Arrow](https://arrow.apache.org) data standard.  This package provides Julia `AbstractVector` objects for
referencing data that conforms to the Arrow standard.  This allows users to seamlessly interface Arrow formatted data with a great deal of existing Julia code.

Please see this [document](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout) for a description of the Arrow memory layout.

## Installation

The package can be installed by typing in the following in a Julia REPL:

```julia
julia> using Pkg; Pkg.add(url="https://github.com/apache/arrow", subdir="julia/Arrow", rev="apache-arrow-3.0.0")
```

or to use the non-official-apache code that will sometimes include bugfix patches between apache releases, you can do:

```julia
julia> using Pkg; Pkg.add("Arrow")
```

## Difference between this code and the JuliaData/Arrow.jl repository

This code is officially part of the apache/arrow repository and as such follows the regulated release cadence of the entire project, following standard community
voting protocols. The JuliaData/Arrow.jl repository can be viewed as a sort of "dev" or "latest" branch of this code that may release more frequently, but without following
official apache release guidelines. The two repositories are synced, however, so any bugfix patches in JuliaData will be upstreamed to apache/arrow for each release.

## Format Support

This implementation supports the 1.0 version of the specification, including support for:
  * All primitive data types
  * All nested data types
  * Dictionary encodings and messages
  * Extension types
  * Streaming, file, record batch, and replacement and isdelta dictionary messages

It currently doesn't include support for:
  * Tensors or sparse tensors
  * Flight RPC
  * C data interface

Third-party data formats:
  * csv and parquet support via the existing CSV.jl and Parquet.jl packages
  * Other Tables.jl-compatible packages automatically supported (DataFrames.jl, JSONTables.jl, JuliaDB.jl, SQLite.jl, MySQL.jl, JDBC.jl, ODBC.jl, XLSX.jl, etc.)
  * No current Julia packages support ORC or Avro data formats

See the [full documentation](https://arrow.juliadata.org/dev/) for details on reading and writing arrow data.

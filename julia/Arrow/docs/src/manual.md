# User Manual

The goal of this documentation is to provide a brief introduction to the arrow data format, then provide a walk-through of the functionality provided in the Arrow.jl Julia package, with an aim to expose a little of the machinery "under the hood" to help explain how things work and how that influences real-world use-cases for the arrow data format.

The best place to learn about the Apache arrow project is [the website itself](https://arrow.apache.org/), specifically the data format [specification](https://arrow.apache.org/docs/format/Columnar.html). Put briefly, the arrow project provides a formal specification for how columnar, "table" data can be laid out efficiently in memory to standardize and maximize the ability to share data across languages/platforms. In the current [apache/arrow GitHub repository](https://github.com/apache/arrow), language implementations exist for C++, Java, Go, Javascript, Rust, to name a few. Other database vendors and data processing frameworks/applications have also built support for the arrow format, allowing for a wide breadth of possibility for applications to "speak the data language" of arrow.

The [Arrow.jl](https://github.com/JuliaData/Arrow.jl) Julia package is another implementation, allowing the ability to both read and write data in the arrow format. As a data format, arrow specifies an exact memory layout to be used for columnar table data, and as such, "reading" involves custom Julia objects ([`Arrow.Table`](@ref) and [`Arrow.Stream`](@ref)), which read the *metadata* of an "arrow memory blob", then *wrap* the array data contained therein, having learned the type and size, amongst other properties, from the metadata. Let's take a closer look at what this "reading" of arrow memory really means/looks like.


## Reading arrow data

After installing the Arrow.jl Julia package (via `] add Arrow`), and if you have some arrow data, let's say a file named `data.arrow` generated from the [`pyarrow`](https://arrow.apache.org/docs/python/) library (a Python library for interfacing with arrow data), you can then read that arrow data into a Julia session by doing:

```julia
using Arrow

table = Arrow.Table("data.arrow")
```

### `Arrow.Table`

The type of `table` in this example will be an `Arrow.Table`. When "reading" the arrow data, `Arrow.Table` first ["mmapped"](https://en.wikipedia.org/wiki/Mmap) the `data.arrow` file, which is an important technique for dealing with data larger than available RAM on a system. By "mmapping" a file, the OS doesn't actually load the entire file contents into RAM at the same time, but file contents are "swapped" into RAM as different regions of a file are requested. Once "mmapped", `Arrow.Table` then inspected the metadata in the file to determine the number of columns, their names and types, at which byte offset each column begins in the file data, and even how many "batches" are included in this file (arrow tables may be partitioned into one ore more "record batches" each containing portions of the data). Armed with all the appropriate metadata, `Arrow.Table` then created custom array objects ([`ArrowVector`](@ref)), which act as "views" into the raw arrow memory bytes. This is a significant point in that no extra memory is allocated for "data" when reading arrow data. This is in contrast to if we wanted to read the data of a csv file as columns into Julia structures; we would need to allocate those array structures ourselves, then parse the file, "filling in" each element of the array with the data we parsed from the file. Arrow data, on the other hand, is *already laid out in memory or on disk* in a binary format, and as long as we have the metadata to interpret the raw bytes, we can figure out whether to treat those bytes as a `Vector{Float64}`, etc. A sample of the kinds of arrow array types you might see when deserializing arrow data, include:

* [`Arrow.Primitive`](@ref): the most common array type for simple, fixed-size elements like integers, floats, time types, and decimals
* [`Arrow.List`](@ref): an array type where its own elements are also arrays of some kind, like string columns, where each element can be thought of as an array of characters
* [`Arrow.FixedSizeList`](@ref): similar to the `List` type, but where each array element has a fixed number of elements itself; you can think of this like a `Vector{NTuple{N, T}}`, where `N` is the fixed-size width
* [`Arrow.Map`](@ref): an array type where each element is like a Julia `Dict`; a list of key value pairs like a `Vector{Dict}`
* [`Arrow.Struct`](@ref): an array type where each element is an instance of a custom struct, i.e. an ordered collection of named & typed fields, kind of like a `Vector{NamedTuple}`
* [`Arrow.DenseUnion`](@ref): an array type where elements may be of several different types, stored compactly; can be thought of like `Vector{Union{A, B}}`
* [`Arrow.SparseUnion`](@ref): another array type where elements may be of several different types, but stored as if made up of identically lengthed child arrays for each possible type (less memory efficient than `DenseUnion`)
* [`Arrow.DictEncoded`](@ref): a special array type where values are "dictionary encoded", meaning the list of unique, possible values for an array are stored internally in an "encoding pool", whereas each stored element of the array is just an integer "code" to index into the encoding pool for the actual value.

And while these custom array types do subtype `AbstractArray`, there is only limited support for `setindex!`. Remember, these arrays are "views" into the raw arrow bytes, so for array types other than `Arrow.Primitive`, it gets pretty tricky to allow manipulating those raw arrow bytes. Nevetheless, it's as simple as calling `copy(x)` where `x` is any `ArrowVector` type, and a normal Julia `Vector` type will be fully materialized (which would then allow mutating/manipulating values).

So, what can you do with an `Arrow.Table` full of data? Quite a bit actually!

Because `Arrow.Table` implements the [Tables.jl](https://juliadata.github.io/Tables.jl/stable/) interface, it opens up a world of integrations for using arrow data. A few examples include:

* `df = DataFrame(Arrow.Table(file))`: Build a [`DataFrame`](https://juliadata.github.io/DataFrames.jl/stable/), using the arrow vectors themselves; this allows utilizing a host of DataFrames.jl functionality directly on arrow data; grouping, joining, selecting, etc.
* `Tables.datavaluerows(Arrow.Table(file)) |> @map(...) |> @filter(...) |> DataFrame`: use [`Query.jl`'s](https://www.queryverse.org/Query.jl/stable/standalonequerycommands/) row-processing utilities to map, group, filter, mutate, etc. directly over arrow data.
* `Arrow.Table(file) |> SQLite.load!(db, "arrow_table")`: load arrow data directly into an sqlite database/table, where sql queries can be executed on the data
* `Arrow.Table(file) |> CSV.write("arrow.csv")`: write arrow data out to a csv file

A full list of Julia packages leveraging the Tables.jl inteface can be found [here](https://github.com/JuliaData/Tables.jl/blob/master/INTEGRATIONS.md).

Apart from letting other packages have all the fun, an `Arrow.Table` itself can be plenty useful. For example, with `tbl = Arrow.Table(file)`:
* `tbl[1]`: retrieve the first column via indexing; the number of columns can be queried via `length(tbl)`
* `tbl[:col1]` or `tbl.col1`: retrieve the column named `col1`, either via indexing with the column name given as a `Symbol`, or via "dot-access"
* `for col in tbl`: iterate through columns in the table
* `AbstractDict` methods like `haskey(tbl, :col1)`, `get(tbl, :col1, nothing)`, `keys(tbl)`, or `values(tbl)`

### Arrow types

In the arrow data format, specific logical types are supported, a list of which can be found [here](https://arrow.apache.org/docs/status.html#data-types). These include booleans, integers of various bit widths, floats, decimals, time types, and binary/string. While most of these map naturally to types builtin to Julia itself, there are a few cases where the definitions are slightly different, and in these cases, by default, they are converted to more "friendly" Julia types (this auto conversion can be avoided by passing `convert=false` to `Arrow.Table`, like `Arrow.Table(file; convert=false)`). Examples of arrow to julia type mappings include:

* `Date`, `Time`, `Timestamp`, and `Duration` all have natural Julia defintions in `Dates.Date`, `Dates.Time`, `TimeZones.ZonedDateTime`, and `Dates.Period` subtypes, respectively. 
* `Char` and `Symbol` Julia types are mapped to arrow string types, with additional metadata of the original Julia type; this allows deserializing directly to `Char` and `Symbol` in Julia, while other language implementations will see these columns as just strings
* `Decimal128` and `Decimal256` have no corresponding builtin Julia types, so they're deserialized using a compatible type definition in Arrow.jl itself: `Arrow.Decimal`

Note that when `convert=false` is passed, data will be returned in Arrow.jl-defined types that exactly match the arrow definitions of those types; the authoritative source for how each type represents its data can be found in the arrow [`Schema.fbs`](https://github.com/apache/arrow/blob/master/format/Schema.fbs) file.

#### Custom types

To support writing your custom Julia struct, Arrow.jl utilizes the format's mechanism for "extension types" by storing
the Julia type name in the field metadata. To "hook in" to this machinery, custom types can just call
`Arrow.ArrowTypes.registertype!(T, T)`, where `T` is the custom struct type. For example:

```julia
using Arrow

struct Person
    id::Int
    name::String
end

Arrow.ArrowTypes.registertype!(Person, Person)

table = (col1=[Person(1, "Bob"), Person(2, "Jane")],)
io = IOBuffer()
Arrow.write(io, table)
seekstart(io)
table2 = Arrow.Table(io)
```

In this example, we're writing our `table`, which is a NamedTuple with one column named `col1`, which has two
elements which are instances of our custom `Person` struct. We call `Arrow.Arrowtypes.registertype!` so that
Arrow.jl knows how to serialize our `Person` struct. We then read the table back in using `Arrow.Table` and
the table we get back will be an `Arrow.Table`, with a single `Arrow.Struct` column with element type `Person`.

Note that without calling `Arrow.Arrowtypes.registertype!`, we may get into a weird limbo state where we've written
our table with `Person` structs out as a table, but when reading back in, Arrow.jl doesn't know what a `Person` is;
deserialization won't fail, but we'll just get a `Namedtuple{(:id, :name), Tuple{Int, String}}` back instead of `Person`.

!!! warning

    If `Arrow.ArrowTypes.registertype!` is called in a downstream package, e.g. to register a custom type defined in
    that package, it must be called from the `__init__` function of the package's top-level module
    (see the [Julia docs](https://docs.julialang.org/en/v1/manual/modules/#Module-initialization-and-precompilation)
    for more on `__init__` functions). Otherwise, the type will only be registered during the precompilation phase,
    but that state will be lost afterwards (and in particular, the type will not be registered when the package is loaded).

### `Arrow.Stream`

In addition to `Arrow.Table`, the Arrow.jl package also provides `Arrow.Stream` for processing arrow data. While `Arrow.Table` will iterate all record batches in an arrow file/stream, concatenating columns, `Arrow.Stream` provides a way to *iterate* through record batches, one at a time. Each iteration yields an `Arrow.Table` instance, with columns/data for a single record batch. This allows, if so desired, "batch processing" of arrow data, one record batch at a time, instead of creating a single long table via `Arrow.Table`.

### Table and column metadata

The arrow format allows attaching arbitrary metadata in the form of a `Dict{String, String}` to tables and individual columns. The Arrow.jl package supports retrieving serialized metadata by calling `Arrow.getmetadata(table)` or `Arrow.getmetadata(column)`.

## Writing arrow data

Ok, so that's a pretty good rundown of *reading* arrow data, but how do you *produce* arrow data? Enter `Arrow.write`.

### `Arrow.write`

With `Arrow.write`, you provide either an `io::IO` argument or `file::String` to write the arrow data to, as well as a Tables.jl-compatible source that contains the data to be written.

What are some examples of Tables.jl-compatible sources? A few examples include:
* `Arrow.write(io, df::DataFrame)`: A `DataFrame` is a collection of indexable columns
* `Arrow.write(io, CSV.File(file))`: read data from a csv file and write out to arrow format
* `Arrow.write(io, DBInterface.execute(db, sql_query))`: Execute an SQL query against a database via the [`DBInterface.jl`](https://github.com/JuliaDatabases/DBInterface.jl) interface, and write the query resultset out directly in the arrow format. Packages that implement DBInterface include [SQLite.jl](https://juliadatabases.github.io/SQLite.jl/stable/), [MySQL.jl](https://juliadatabases.github.io/MySQL.jl/dev/), and [ODBC.jl](http://juliadatabases.github.io/ODBC.jl/latest/). 
* `df |> @map(...) |> Arrow.write(io)`: Write the results of a [Query.jl](https://www.queryverse.org/Query.jl/stable/) chain of operations directly out as arrow data
* `jsontable(json) |> Arrow.write(io)`: Treat a json array of objects or object of arrays as a "table" and write it out as arrow data using the [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl) package
* `Arrow.write(io, (col1=data1, col2=data2, ...))`: a `NamedTuple` of `AbstractVector`s or an `AbstractVector` of `NamedTuple`s are both considered tables by default, so they can be quickly constructed for easy writing of arrow data if you already have columns of data

And these are just a few examples of the numerous [integrations](https://github.com/JuliaData/Tables.jl/blob/master/INTEGRATIONS.md).

In addition to just writing out a single "table" of data as a single arrow record batch, `Arrow.write` also supports writing out multiple record batches when the input supports the `Tables.partitions` functionality. One immediate, though perhaps not incredibly useful example, is `Arrow.Stream`. `Arrow.Stream` implements `Tables.partitions` in that it iterates "tables" (specifically `Arrow.Table`), and as such, `Arrow.write` will iterate an `Arrow.Stream`, and write out each `Arrow.Table` as a separate record batch. Another important point for why this example works is because an `Arrow.Stream` iterates `Arrow.Table`s that all have the same schema. This is important because when writing arrow data, a "schema" message is always written first, with all subsequent record batches written with data matching the initial schema.

In addition to inputs that support `Tables.partitions`, note that the Tables.jl itself provides the `Tables.partitioner` function, which allows providing your own separate instances of similarly-schema-ed tables as "partitions", like:

```julia
# treat 2 separate NamedTuples of vectors with same schema as 1 table, 2 partitions
tbl_parts = Tables.partitioner([(col1=data1, col2=data2), (col1=data3, col2=data4)])
Arrow.write(io, tbl_parts)

# treat an array of csv files with same schema where each file is a partition
# in this form, a function `CSV.File` is applied to each element of 2nd argument
csv_parts = Tables.partitioner(CSV.File, csv_files)
Arrow.write(io, csv_parts)
```

### Multithreaded writing

By default, `Arrow.write` will use multiple threads to write multiple
record batches simultaneously (e.g. if julia is started with `julia -t 8` or the `JULIA_NUM_THREADS` environment variable is set).

### Compression

Compression is supported when writing via the `compress` keyword argument. Possible values include `:lz4`, `:zstd`, or your own initialized `LZ4FrameCompressor` or `ZstdCompressor` objects; will cause all buffers in each record batch to use the respective compression encoding or compressor.

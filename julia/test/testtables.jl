testtables = [
  (
    "basic",
    (col1=Int64[1,2,3,4,5,6,7,8,9,10],),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "missing values",
    (col1=Union{Int64, Missing}[1,2,3,4,5,6,7,8,9,missing],),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "primitive types",
    (
      col1=[missing, missing, missing, missing],
      col2=Union{UInt8, Missing}[0, 1, 2, missing],
      col3=Union{UInt16, Missing}[0, 1, 2, missing],
      col4=Union{UInt32, Missing}[0, 1, 2, missing],
      col5=Union{UInt64, Missing}[0, 1, 2, missing],
      col6=Union{Int8, Missing}[0, 1, 2, missing],
      col7=Union{Int16, Missing}[0, 1, 2, missing],
      col8=Union{Int32, Missing}[0, 1, 2, missing],
      col9=Union{Int64, Missing}[0, 1, 2, missing],
      col10=Union{Float16, Missing}[0, 1, 2, missing],
      col11=Union{Float32, Missing}[0, 1, 2, missing],
      col12=Union{Float64, Missing}[0, 1, 2, missing],
      col13=[true, false, true, missing],
    ),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "arrow date/time types",
    (
      col14=[zero(Arrow.Decimal{Int32(2), Int32(2)}), zero(Arrow.Decimal{Int32(2), Int32(2)}), zero(Arrow.Decimal{Int32(2), Int32(2)}), missing],
      col15=[zero(Arrow.Date{Arrow.Meta.DateUnit.DAY, Int32}), zero(Arrow.Date{Arrow.Meta.DateUnit.DAY, Int32}), zero(Arrow.Date{Arrow.Meta.DateUnit.DAY, Int32}), missing],
      col16=[zero(Arrow.Time{Arrow.Meta.TimeUnit.SECOND, Int32}), zero(Arrow.Time{Arrow.Meta.TimeUnit.SECOND, Int32}), zero(Arrow.Time{Arrow.Meta.TimeUnit.SECOND, Int32}), missing],
      col17=[zero(Arrow.Timestamp{Arrow.Meta.TimeUnit.SECOND, nothing}), zero(Arrow.Timestamp{Arrow.Meta.TimeUnit.SECOND, nothing}), zero(Arrow.Timestamp{Arrow.Meta.TimeUnit.SECOND, nothing}), missing],
      col18=[zero(Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH, Int32}), zero(Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH, Int32}), zero(Arrow.Interval{Arrow.Meta.IntervalUnit.YEAR_MONTH, Int32}), missing],
      col19=[zero(Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}), zero(Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}), zero(Arrow.Duration{Arrow.Meta.TimeUnit.SECOND}), missing],
    ),
    NamedTuple(),
    (convert=false,),
    nothing
  ),
  (
    "list types",
    (
      col1=Union{String, Missing}["hey", "there", "sailor", missing],
      col2=Union{Vector{UInt8}, Missing}[b"hey", b"there", b"sailor", missing],
      col3=Union{Vector{Int64}, Missing}[Int64[1], Int64[2], Int64[3], missing],
      col4=Union{NTuple{2, Vector{Int64}},Missing}[(Int64[1], Int64[2]), missing, missing, (Int64[3], Int64[4])],
      col5=Union{NTuple{2, UInt8}, Missing}[(0x01, 0x02), (0x03, 0x04), missing, (0x05, 0x06)],
      col6=NamedTuple{(:a, :b), Tuple{Int64, String}}[(a=Int64(1), b="hey"), (a=Int64(2), b="there"), (a=Int64(3), b="sailor"), (a=Int64(4), b="jo-bob")],
    ),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "unions",
    (
      col1=Arrow.DenseUnionVector( Union{Int64, Float64, Missing}[1, 2.0, 3, 4.0, missing]),
      col2=Arrow.SparseUnionVector(Union{Int64, Float64, Missing}[1, 2.0, 3, 4.0, missing]),
    ),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "dict encodings",
    (
      col1=Arrow.DictEncode(Int64[4, 5, 6]),
    ),
    NamedTuple(),
    NamedTuple(),
    function (tt)
      col1 = copy(tt.col1)
      @test typeof(col1) == PooledVector{Int64, Int8, Vector{Int8}}
    end
  ),
  (
    "more dict encodings",
    (
      col1=Arrow.DictEncode(NamedTuple{(:a, :b), Tuple{Int64, Union{String, Missing}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(3), b="sailor"), (a=Int64(4), b="jo-bob")]),
    ),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "PooledArray",
    (
      col1=PooledArray([4,5,6,6]),
    ),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "auto-converting types",
    (
      col1=[Date(2001, 1, 2), Date(2010, 10, 10), Date(2020, 12, 1)],
      col2=[Time(1, 1, 2), Time(13, 10, 10), Time(22, 12, 1)],
      col3=[DateTime(2001, 1, 2), DateTime(2010, 10, 10), DateTime(2020, 12, 1)]
    ),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "Map",
    (
      col1=[Dict(Int32(1) => Float32(3.14)), missing],
    ),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "non-standard types",
    (
      col1=[:hey, :there, :sailor],
      col2=['a', 'b', 'c'],
      col3=Arrow.DictEncode(['a', 'a', 'b'])
    ),
    NamedTuple(),
    NamedTuple(),
    nothing
  ),
  (
    "large lists",
    (
      col1=Union{String, Missing}["hey", "there", "sailor", missing],
      col2=Union{Vector{UInt8}, Missing}[b"hey", b"there", b"sailor", missing],
      col3=Union{Vector{Int64}, Missing}[Int64[1], Int64[2], Int64[3], missing],
      col4=Union{NTuple{2, Vector{Int64}},Missing}[(Int64[1], Int64[2]), missing, missing, (Int64[3], Int64[4])],
      col5=Union{NTuple{2, UInt8}, Missing}[(0x01, 0x02), (0x03, 0x04), missing, (0x05, 0x06)],
      col6=NamedTuple{(:a, :b), Tuple{Int64, String}}[(a=Int64(1), b="hey"), (a=Int64(2), b="there"), (a=Int64(3), b="sailor"), (a=Int64(4), b="jo-bob")],
    ),
    (largelists=true,),
    NamedTuple(),
    nothing
  ),
  (
    "dictencode keyword",
    (
      col1=Int64[1,2,3,4],
      col2=Union{String, Missing}["hey", "there", "sailor", missing],
      col3=Arrow.DictEncode(NamedTuple{(:a, :b), Tuple{Int64, Union{String, Missing}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(3), b="sailor"), (a=Int64(4), b="jo-bob")]),
      col4=[:a, :b, :c, missing],
      col5=[Date(2020, 1, 1) for x = 1:4]
    ),
    (dictencode=true,),
    NamedTuple(),
    nothing
  ),
  (
    "nesteddictencode keyword",
    (
      col1=Int64[1,2,3,4],
      col2=Union{String, Missing}["hey", "there", "sailor", missing],
      col3=Arrow.DictEncode(NamedTuple{(:a, :b), Tuple{Int64, Union{String, Missing}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(3), b="sailor"), (a=Int64(4), b="jo-bob")]),
    ),
    (dictencode=true, dictencodenested=true,),
    NamedTuple(),
    nothing
  ),
  (
    "Julia unions",
    (
      col1=Union{Int, String}[1, "hey", 2, "ho"],
      col2=Union{Char, NamedTuple{(:a,), Tuple{Symbol}}}['a', (a=:hey,), 'b', (a=:ho,)],
    ),
    (denseunions=false,),
    NamedTuple(),
    nothing
  ),
];

function testtable(nm, t, writekw, readkw, extratests)
  println("testing: $nm")
  io = IOBuffer()
  Arrow.write(io, t; writekw...)
  seekstart(io)
  tt = Arrow.Table(io; readkw...)
  @test length(tt) == length(t)
  @test all(isequal.(values(t), values(tt)))
  extratests !== nothing && extratests(tt)
  seekstart(io)
  str = Arrow.Stream(io; readkw...)
  tt = first(str)
  @test length(tt) == length(t)
  @test all(isequal.(values(t), values(tt)))
  # compressed
  io = IOBuffer()
  Arrow.write(io, t; compress=((:lz4, :zstd)[rand(1:2)]), writekw...)
  seekstart(io)
  tt = Arrow.Table(io; readkw...)
  @test length(tt) == length(t)
  @test all(isequal.(values(t), values(tt)))
  extratests !== nothing && extratests(tt)
  seekstart(io)
  str = Arrow.Stream(io; readkw...)
  tt = first(str)
  @test length(tt) == length(t)
  @test all(isequal.(values(t), values(tt)))
  # file
  io = IOBuffer()
  Arrow.write(io, t; file=true, writekw...)
  seekstart(io)
  tt = Arrow.Table(io; readkw...)
  @test length(tt) == length(t)
  @test all(isequal.(values(t), values(tt)))
  extratests !== nothing && extratests(tt)
  seekstart(io)
  str = Arrow.Stream(io; readkw...)
  tt = first(str)
  @test length(tt) == length(t)
  @test all(isequal.(values(t), values(tt)))
  return
end
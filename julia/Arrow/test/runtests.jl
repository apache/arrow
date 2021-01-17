# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

using Test, Arrow, Tables, Dates, PooledArrays, TimeZones, UUIDs

include(joinpath(dirname(pathof(Arrow)), "../test/testtables.jl"))
include(joinpath(dirname(pathof(Arrow)), "../test/integrationtest.jl"))
include(joinpath(dirname(pathof(Arrow)), "../test/dates.jl"))

struct CustomStruct
    x::Int
    y::Float64
    z::String
end

@testset "Arrow" begin

@testset "table roundtrips" begin

for case in testtables
    testtable(case...)
end

end # @testset "table roundtrips"

@testset "arrow json integration tests" begin

for file in readdir(joinpath(dirname(pathof(Arrow)), "../test/arrowjson"))
    jsonfile = joinpath(joinpath(dirname(pathof(Arrow)), "../test/arrowjson"), file)
    println("integration test for $jsonfile")
    df = ArrowJSON.parsefile(jsonfile);
    io = IOBuffer()
    Arrow.write(io, df)
    seekstart(io)
    tbl = Arrow.Table(io; convert=false);
    @test isequal(df, tbl)
end

end # @testset "arrow json integration tests"

@testset "misc" begin

# multiple record batches
t = Tables.partitioner(((col1=Union{Int64, Missing}[1,2,3,4,5,6,7,8,9,missing],), (col1=Union{Int64, Missing}[1,2,3,4,5,6,7,8,9,missing],)))
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == 1
@test isequal(tt.col1, vcat([1,2,3,4,5,6,7,8,9,missing], [1,2,3,4,5,6,7,8,9,missing]))
@test eltype(tt.col1) === Union{Int64, Missing}

# Arrow.Stream
seekstart(io)
str = Arrow.Stream(io)
state = iterate(str)
@test state !== nothing
tt, st = state
@test length(tt) == 1
@test isequal(tt.col1, [1,2,3,4,5,6,7,8,9,missing])

state = iterate(str, st)
@test state !== nothing
tt, st = state
@test length(tt) == 1
@test isequal(tt.col1, [1,2,3,4,5,6,7,8,9,missing])

@test iterate(str, st) === nothing

# dictionary batch isDelta
t = (
    col1=Int64[1,2,3,4],
    col2=Union{String, Missing}["hey", "there", "sailor", missing],
    col3=NamedTuple{(:a, :b), Tuple{Int64, Union{Missing, NamedTuple{(:c,), Tuple{String}}}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(3), b=(c="sailor",)), (a=Int64(4), b=(c="jo-bob",))]
)
t2 = (
    col1=Int64[1,2,5,6],
    col2=Union{String, Missing}["hey", "there", "sailor2", missing],
    col3=NamedTuple{(:a, :b), Tuple{Int64, Union{Missing, NamedTuple{(:c,), Tuple{String}}}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(5), b=(c="sailor2",)), (a=Int64(4), b=(c="jo-bob",))]
)
tt = Tables.partitioner((t, t2))
io = IOBuffer()
Arrow.write(io, tt; dictencode=true, dictencodenested=true)
seekstart(io)
tt = Arrow.Table(io)
@test tt.col1 == [1,2,3,4,1,2,5,6]
@test isequal(tt.col2, ["hey", "there", "sailor", missing, "hey", "there", "sailor2", missing])
@test isequal(tt.col3, vcat(NamedTuple{(:a, :b), Tuple{Int64, Union{Missing, NamedTuple{(:c,), Tuple{String}}}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(3), b=(c="sailor",)), (a=Int64(4), b=(c="jo-bob",))], NamedTuple{(:a, :b), Tuple{Int64, Union{Missing, NamedTuple{(:c,), Tuple{String}}}}}[(a=Int64(1), b=missing), (a=Int64(1), b=missing), (a=Int64(5), b=(c="sailor2",)), (a=Int64(4), b=(c="jo-bob",))]))

t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
meta = Dict("key1" => "value1", "key2" => "value2")
Arrow.setmetadata!(t, meta)
meta2 = Dict("colkey1" => "colvalue1", "colkey2" => "colvalue2")
Arrow.setmetadata!(t.col1, meta2)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test tt.col1 == t.col1
@test eltype(tt.col1) === Int64
@test Arrow.getmetadata(tt) == meta
@test Arrow.getmetadata(tt.col1) == meta2

# custom compressors
lz4 = Arrow.CodecLz4.LZ4FrameCompressor(; compressionlevel=8)
Arrow.CodecLz4.TranscodingStreams.initialize(lz4)
t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
io = IOBuffer()
Arrow.write(io, t; compress=lz4)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

zstd = Arrow.CodecZstd.ZstdCompressor(; level=8)
Arrow.CodecZstd.TranscodingStreams.initialize(zstd)
t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
io = IOBuffer()
Arrow.write(io, t; compress=zstd)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# custom alignment
t = (col1=Int64[1,2,3,4,5,6,7,8,9,10],)
io = IOBuffer()
Arrow.write(io, t; alignment=64)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# 53
s = "a" ^ 100
t = (a=[SubString(s, 1:10), SubString(s, 11:20)],)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test tt.a == ["aaaaaaaaaa", "aaaaaaaaaa"]

# 49
@test_throws ArgumentError Arrow.Table("file_that_doesnt_exist")

# 52
t = (a=Arrow.DictEncode(string.(1:129)),)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)

# 60: unequal column lengths
io = IOBuffer()
@test_throws ArgumentError Arrow.write(io, (a = Int[], b = ["asd"], c=collect(1:100)))

# nullability of custom extension types
t = (a=['a', missing],)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test isequal(tt.a, ['a', missing])

# automatic custom struct serialization/deserialization
t = (col1=[CustomStruct(1, 2.3, "hey"), CustomStruct(4, 5.6, "there")],)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# 76
t = (col1=NamedTuple{(:a,),Tuple{Union{Int,String}}}[(a=1,), (a="x",)],)
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test length(tt) == length(t)
@test all(isequal.(values(t), values(tt)))

# 89 - test deprecation path for old UUID autoconversion
u = 0x6036fcbd20664bd8a65cdfa25434513f
@test Arrow.ArrowTypes.arrowconvert(UUID, (value=u,)) === UUID(u)

# 98
t = (a = [Nanosecond(0), Nanosecond(1)], b = [uuid4(), uuid4()], c = [missing, Nanosecond(1)])
io = IOBuffer()
Arrow.write(io, t)
seekstart(io)
tt = Arrow.Table(io)
@test copy(tt.a) isa Vector{Nanosecond}
@test copy(tt.b) isa Vector{UUID}
@test copy(tt.c) isa Vector{Union{Missing,Nanosecond}}

# copy on DictEncoding w/ missing values
x = PooledArray(["hey", missing])
x2 = Arrow.toarrowvector(x)
@test isequal(copy(x2), x)

end # @testset "misc"

end

using Arrow
using Compat, Compat.Test, Compat.Random, Compat.Dates
using CategoricalArrays

if VERSION < v"0.7.0-"
    using Missings
end

const ≅ = isequal

const SEED = 999

const N_OUTER = 4
const N_PAD_TESTS = 32
const N_WRITE_TESTS = 16
const N_DATE_TESTS = 32
const N_IDX_CHECK = 32
const MAX_IDX_LEN = 32
const MAX_VECTOR_LENGTH = 256
const MAX_STRING_LENGTH = 32

const PRIMITIVE_ELTYPES = [Float32, Float64, Int32, Int64, UInt16]
const OFFSET_ELTYPES = [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64]

Compat.Random.seed!(SEED)

include("testutils.jl")


include("indexing.jl")
include("writing.jl")


# used in locate testing
struct LocateTest1 end
struct LocateTest2 end
struct LocateTest3 end

@testset "locate" begin
    v = rand(Float32, rand(1:MAX_VECTOR_LENGTH))
    p = Primitive(v)

    Locate.length(::LocateTest1) = length(v)
    Locate.values(::LocateTest1) = 1
    Locate.valueslength(::LocateTest1) = length(v)

    data = rawpadded(p, values)

    pp = locate(data, Float32, LocateTest1())
    @test typeof(pp) == Primitive{Float32}
    @test p[:] == pp[:]


    v = Union{Int64,Missing}[2,3,missing,7,11]
    p = NullablePrimitive(v)

    Locate.length(::LocateTest2) = length(v)
    Locate.bitmask(::LocateTest2) = 1
    Locate.values(::LocateTest2) = 9
    Locate.valueslength(::LocateTest2) = length(v)

    data = rawpadded(p, bitmask, values)

    pp = locate(data, Union{Int64,Missing}, LocateTest2())
    @test typeof(pp) == NullablePrimitive{Int64}
    @test p[:] ≅ pp[:]

    v = String["fire", "walk", "with", "me"]
    p = List(v)

    Locate.length(::LocateTest3) = length(v)
    Locate.offsets(::LocateTest3) = 1
    Locate.values(::LocateTest3) = padding((length(v)+1)*sizeof(Arrow.DefaultOffset)) + 1
    Locate.valueslength(::LocateTest3) = 14

    data = rawpadded(p, offsets, values)

    pp = locate(data, String, LocateTest3())
    @test typeof(pp) == List{String,Arrow.DefaultOffset,Primitive{UInt8}}
    @test p[:] == pp[:]
end


@testset "DateTime" begin
    for i ∈ 1:N_DATE_TESTS
        d = Date(rand(0:4000), rand(1:12), rand(1:20))
        ad = convert(Arrow.Datestamp, d)
        @test convert(Date, ad) == d

        dt = DateTime(rand(0:4000), rand(1:12), rand(1:20), rand(0:23), rand(0:59), rand(0:59))
        adt = convert(Arrow.Timestamp, dt)
        @test convert(DateTime, adt) == dt

        t = Time(rand(0:23), rand(0:59), rand(0:59))
        at = convert(Arrow.TimeOfDay, t)
        @test convert(Time, at) == t
    end
end


@testset "arrowformat_construct" begin
    len = rand(1:MAX_VECTOR_LENGTH)
    randstring_() = randstring(rand(1:MAX_STRING_LENGTH))


    p = arrowformat(rand(Float64, len))
    @test typeof(p) == Primitive{Float64}

    mask = rand(Bool, len)

    v = Union{Float32,Missing}[mask[i] ? rand(Float32) : missing for i ∈ 1:len]
    p = arrowformat(v)
    @test typeof(p) == NullablePrimitive{Float32}

    v = String[randstring_() for i ∈ 1:len]
    p = arrowformat(v)
    @test typeof(p) == List{String,Arrow.DefaultOffset,Primitive{UInt8}}

    v = Union{String,Missing}[mask[i] ? randstring_() : missing for i ∈ 1:len]
    p = arrowformat(v)
    @test typeof(p) == NullableList{String,Arrow.DefaultOffset,Primitive{UInt8}}

    v = rand(Bool, len)
    p = arrowformat(v)
    @test typeof(p) == BitPrimitive

    v = Union{Bool,Missing}[mask[i] ? rand(Bool) : missing for i ∈ 1:len]
    p = arrowformat(v)
    @test typeof(p) == NullableBitPrimitive

    v = Date[Date(1), Date(2)]
    p = arrowformat(v)
    @test typeof(p) == Primitive{Arrow.Datestamp}

    v = DateTime[DateTime(0), DateTime(1)]
    p = arrowformat(v)
    @test typeof(p) == Primitive{Arrow.Timestamp{Millisecond}}

    v = Time[Time(0), Time(1)]
    p = arrowformat(v)
    @test typeof(p) == Primitive{Arrow.TimeOfDay{Nanosecond,Int64}}

    v = Union{Date,Missing}[Date(1), missing]
    p = arrowformat(v)
    @test typeof(p) == NullablePrimitive{Arrow.Datestamp}

    v = Union{DateTime,Missing}[DateTime(0), missing]
    p = arrowformat(v)
    @test typeof(p) == NullablePrimitive{Arrow.Timestamp{Millisecond}}

    v = Union{Time,Missing}[Time(0), missing]
    p = arrowformat(v)
    @test typeof(p) == NullablePrimitive{Arrow.TimeOfDay{Nanosecond,Int64}}

    v = categorical(["a", "b", "c"])
    p = arrowformat(v)
    @test typeof(p) == DictEncoding{String,Primitive{Int32},List{String,Arrow.DefaultOffset,Primitive{UInt8}}}
end

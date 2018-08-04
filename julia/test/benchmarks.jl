using Arrow
using BenchmarkTools

const L = 10^7


# TODO obviously these are not real benchmarks
# so far this is just a space to screw around and check basic performance


function wrap(::Type{T}, len::Integer, v::Vector{UInt8}) where T
    ptr = convert(Ptr{T}, pointer(v))
    unsafe_wrap(Array, ptr, len)
end


function randmissings(::Type{T}, len::Integer) where T

end


function benches1()
    A = convert(Vector{UInt8}, reinterpret(UInt8, rand(Int64, L)))

    @info("performing wrap benchmark...")
    global b_wrap = @benchmark wrap(Int64, $L, $A)

    @info("performing wrap benchmark with missings...")
    global b_wrap_missing = @benchmark convert(Vector{Union{Int,Missing}}, wrap(Int, $L, $A))
    
    @info("performing reinterpret benchmark...")
    global b_reint = @benchmark reinterpret(Int64, $A)
    

    p = Primitive{Int64}(A, 1, L)

    bmask = zeros(UInt8, Arrow.bitmaskbytes(L))
    A2 = vcat(bmask, A)
    println(pointer(A2))
    p2 = NullablePrimitive{Int64}(A2, 1, length(bmask)+1, L)
    println(Arrow.valuespointer(p2))
    
    @info("performing Arrow benchmark...")
    global b_arrow = @benchmark Arrow.getindex($p, 1:($L-1))

    @info("performing Arrow nullable benchmark...")
    global b_arrow2 = @benchmark Arrow.getindex($p2, 1:($L-1))

    global b_idx = @benchmark Arrow.rawvalueindex($p, 1:length($p))
end


function benches2()
    A = String[randstring(rand(4:12)) for i ∈ 1:L]

    l = NullableList(A)

    info("performing Arrow benchmark...")
    global b_arrow = @benchmark Arrow.getindex($l, 1:$L)
end

v = ["a", "ab", "abc"]
l = List(v)


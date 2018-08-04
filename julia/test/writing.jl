

@testset "wrinting_padding" begin
    for i ∈ 1:N_PAD_TESTS
        base = rand(0:256)*Arrow.ALIGNMENT
        r = rand(1:(Arrow.ALIGNMENT-1))
        n = base + r
        @test padding(base) == base
        @test padding(n) == base + Arrow.ALIGNMENT
    end
end


@testset "writing_Primitive" begin
    for i ∈ 1:N_WRITE_TESTS
        dtype = rand(PRIMITIVE_ELTYPES)
        v = reinterpret(UInt8, rand(dtype, rand(1:MAX_VECTOR_LENGTH)))
        p = Primitive(v)
        pad = zeros(UInt8, padding(length(v)) - length(v))
        buff = vcat(convert(Vector{UInt8}, v), pad)
        io = IOBuffer()
        @test writepadded(io, p, values) % 8 == 0
        @test rawpadded(p, values) == buff
        @test take!(io) == buff
    end
end


@testset "writing_NullablePrimitive" begin
    for i ∈ 1:N_WRITE_TESTS
        v, buff = rand_nullableprimitive_buffer2()
        p = NullablePrimitive(v)
        io = IOBuffer()
        @test writepadded(io, p, bitmask, values) % Arrow.ALIGNMENT == 0
        @test rawpadded(p, bitmask, values) == buff
        @test take!(io) == buff
    end
end


@testset "writing_List" begin
    for i ∈ 1:N_WRITE_TESTS
        len = rand(1:MAX_VECTOR_LENGTH)
        offstype = rand([Int32,Int64])
        strs = String[randstring(rand(0:MAX_STRING_LENGTH)) for i ∈ 1:(len-1)]
        # won't work if all strings are empty
        push!(strs, "a")
        offs = Arrow.offsets(offstype, UInt8, strs)
        offs_reint = reinterpret(UInt8, offs)
        vals = convert(Vector{UInt8}, codeunits(reduce(*, strs)))
        valpad = makepad(length(vals))
        buff = vcat(offs_reint, makepad(length(offs_reint)), vals, valpad)
        l = List{String,offstype}(UInt8, strs)
        io = IOBuffer()
        @test writepadded(io, l, offsets, values) % Arrow.ALIGNMENT == 0
        @test rawpadded(l, offsets, values) == buff
        @test take!(io) == buff
    end
end

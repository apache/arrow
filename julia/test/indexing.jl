

@testset "indexing_Primitive_buffer" begin
    for i ∈ 1:N_OUTER
        len, dtype, lpad, b, v = rand_primitive_buffer()
        p = Primitive{dtype}(b, lpad+1, len)
        # integer indices
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] == v[k]
        end
        # AbstractVector{<:Integer} indices
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] == v[idx]
        end
        @test p[:] == v
    end
end


@testset "indexing_Primitive_construct" begin
    for i ∈ 1:N_OUTER
        dtype = rand(PRIMITIVE_ELTYPES)
        len = rand(1:MAX_VECTOR_LENGTH)
        v = rand(dtype, len)
        p = Primitive(v)
        # integer indices
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] == v[k]
        end
        # AbstractVector{<:Integer} indices
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] == v[idx]
        end
        @test p[:] == v
    end
end


@testset "indexing_Primitive_setindex" begin
    for i ∈ 1:N_OUTER
        dtype = rand(PRIMITIVE_ELTYPES)
        len = rand(1:MAX_VECTOR_LENGTH)
        v = rand(dtype, len)
        p = Primitive(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            x = rand(dtype)
            p[k] = x
            @test p[k] == x
        end
        for j ∈ 1:N_IDX_CHECK
            idx = unique(rand(1:len, rand(1:MAX_IDX_LEN)))
            x = rand(dtype, length(idx))
            p[idx] = x
            @test p[idx] == x
        end
        # contiguous
        for j ∈ 1:N_IDX_CHECK
            a, b = extrema(rand(1:len, 2))
            idx = a:b
            x = rand(dtype, length(idx))
            p[idx] = x
            @test p[idx] == x
        end
    end
end


@testset "indexing_NullablePrimitive_buffer" begin
    for i ∈ 1:N_OUTER
        len, dtype, bmask, lpad, b, v = rand_nullableprimitive_buffer()
        p = NullablePrimitive{dtype}(b, 1+lpad, 1+bmask+lpad, len)
        # integer indices
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test (ismissing(p[k]) && ismissing(v[k])) || (p[k] == v[k])
        end
        # AbstractVector{<:Integer} indices
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            sp = p[idx]
            sv = v[idx]
            @test sp ≅ sv
        end
        @test p[:] ≅ v
    end
end


@testset "indexing_NullablePrimitive_construct" begin
    for i ∈ 1:N_OUTER
        dtype = rand(PRIMITIVE_ELTYPES)
        len = rand(1:MAX_VECTOR_LENGTH)
        v = randmissings(dtype, len)
        p = NullablePrimitive(v)
        # integer indices
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] ≅ v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] ≅ v[idx]
        end
        @test p[:] ≅ v
    end
end


@testset "indexing_NullablePrimitive_setindex" begin
    for i ∈ 1:N_OUTER
        dtype = rand(PRIMITIVE_ELTYPES)
        len = rand(1:MAX_VECTOR_LENGTH)
        v = randmissings(dtype, len)
        p = NullablePrimitive(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            x = randmissing(dtype)
            p[k] = x
            @test p[k] ≅ x
        end
        for j ∈ 1:N_IDX_CHECK
            idx = unique(rand(1:len, rand(1:MAX_IDX_LEN)))
            x = randmissings(dtype, length(idx))
            p[idx] = x
            @test p[idx] ≅ x
        end
        # contiguous
        for j ∈ 1:N_IDX_CHECK
            a, b = extrema(rand(1:len, 2))
            idx = a:b
            x = rand(dtype, length(idx))
            p[idx] = x
            @test p[idx] ≅ x
        end
    end
end


@testset "indexing_List_buffer" begin
    len  = 5
    offstype = rand(OFFSET_ELTYPES)
    offs = convert(Vector{offstype}, [0,7,15,18,24,30])
    vals = convert(Vector{UInt8}, codeunits("fireα walk∀ with🐱 me🐟"))
    valspad = zeros(UInt8, Arrow.paddinglength(length(vals)))
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, convert(Vector{UInt8}, reinterpret(UInt8, offs)), vals, valspad, rpad)
    l = List{String,offstype}(b, 1+length(lpad), 1+length(lpad)+sizeof(offstype)*length(offs),
                              len, UInt8, length(vals))
    @test offsets(l)[:] == offs
    @test values(l)[:] == vals
    @test l[1] == "fireα "
    @test l[2] == "walk∀ "
    @test l[3] == "wit"
    @test l[4] == "h🐱 "
    @test l[5] == "me🐟"
    @test l[[1,3,5]] == ["fireα ", "wit", "me🐟"]
    @test l[[false,true,false,true,false]] == ["walk∀ ", "h🐱 "]
    @test l[:] == ["fireα ", "walk∀ ", "wit", "h🐱 ", "me🐟"]
end


@testset "indexing_List_construct" begin
    for i ∈ 1:N_OUTER
        len = rand(1:MAX_VECTOR_LENGTH)
        v = String[randstring(rand(0:MAX_STRING_LENGTH)) for i ∈ 1:len]
        l = List(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test l[k] == v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test l[idx] == v[idx]
        end
        @test l[:] == v
    end
end


@testset "indexing_NullableList_buffer" begin
    len = 7
    offstype = rand(OFFSET_ELTYPES)
    offs = convert(Vector{offstype}, [0,4,9,9,14,14,17,21])
    offs = convert(Vector{offstype}, [0,9,17,17,28,28,31,35])
    vals = convert(Vector{UInt8}, codeunits("kirk 🚀η spockbones, 💀ncc1701"))
    valspad = zeros(UInt8, Arrow.paddinglength(length(vals)))
    pres = Bool[true,true,false,true,false,true,true]
    mask = Arrow.bitpackpadded(pres)
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, mask, convert(Vector{UInt8}, reinterpret(UInt8, offs)), vals, valspad, rpad)
    l = NullableList{String,offstype}(b, 1+length(lpad), 1+length(lpad)+length(mask),
                                      1+length(lpad)+length(mask)+sizeof(offstype)*length(offs),
                                      len, UInt8, length(vals))
    @test offsets(l)[:] == offs
    @test values(l)[:] == vals
    @test l[1] == "kirk 🚀"
    @test l[2] == "η spock"
    @test ismissing(l[3])
    @test l[4] == "bones, 💀"
    @test ismissing(l[5])
    @test l[6] == "ncc"
    @test l[7] == "1701"
    @test l[[2,5,4]] ≅ ["η spock", missing, "bones, 💀"]
    @test l[[true,false,true,false,false,false,false]] ≅ ["kirk 🚀", missing]
    @test l[:] ≅ ["kirk 🚀", "η spock", missing, "bones, 💀", missing, "ncc", "1701"]
end


@testset "indexing_NullableList_construct" begin
    for i ∈ 1:N_OUTER
        len = rand(1:MAX_VECTOR_LENGTH)
        v = Union{String,Missing}[rand(Bool) ? missing : randstring(rand(0:MAX_STRING_LENGTH))
                                  for i ∈ 1:(len-1)]
        # won't work if all strings are empty or missing
        push!(v, "a")
        l = NullableList(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test l[k] ≅ v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test l[idx] ≅ v[idx]
        end
        @test l[:] ≅ v
    end
end


@testset "indexing_BitPrimitive_buffer" begin
    len = 10
    bits = vcat(UInt8[0xfd,0x02], zeros(UInt8, 6))
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, bits, rpad)
    v = Bool[true,false,true,true,true,true,true,true,false,true]
    p = BitPrimitive(b, 1+length(lpad), 10)
    for j ∈ 1:N_IDX_CHECK
        k = rand(1:len)
        @test p[k] == v[k]
    end
    for j ∈ 1:N_IDX_CHECK
        idx = rand(1:len, rand(1:MAX_IDX_LEN))
        @test p[idx] == v[idx]
    end
    @test p[:] == v
end


@testset "indexing_BitPrimitive_construct" begin
    for i ∈ 1:N_OUTER
        len = rand(1:MAX_VECTOR_LENGTH)
        v = rand(Bool, len)
        p = BitPrimitive(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] == v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] == v[idx]
        end
        @test p[:] == v
    end
end


@testset "indexing_NullableBitPrimitive_buffer" begin
    len = 7
    bits = vcat(UInt8[0x4f], zeros(UInt8, 7))
    bmask = vcat(UInt8[0x79], zeros(UInt8, 7))
    lpad = randpad()
    rpad = randpad()
    b = vcat(lpad, bmask, bits, rpad)
    v = [true,missing,missing,true,false,false,true]
    p = NullableBitPrimitive(b, 1+length(lpad), 1+length(lpad)+length(bmask), length(v))
    for j ∈ 1:N_IDX_CHECK
        k = rand(1:len)
        @test p[k] ≅ v[k]
    end
    for j ∈ 1:N_IDX_CHECK
        idx = rand(1:len, rand(1:MAX_IDX_LEN))
        @test p[idx] ≅ v[idx]
    end
    @test p[:] ≅ v
end


@testset "indexing_NullableBitPrimitive_construct" begin
    for i ∈ 1:N_OUTER
        len = rand(1:MAX_VECTOR_LENGTH)
        pres = rand(Bool, len)
        vals = rand(Bool, len)
        v = Union{Bool,Missing}[pres[i] ? missing : vals[i] for i ∈ 1:len]
        p = NullableBitPrimitive(v)
        for j ∈ 1:N_IDX_CHECK
            k = rand(1:len)
            @test p[k] ≅ v[k]
        end
        for j ∈ 1:N_IDX_CHECK
            idx = rand(1:len, rand(1:MAX_IDX_LEN))
            @test p[idx] ≅ v[idx]
        end
        @test p[:] ≅ v
    end
end


@testset "indexing_DictEncoding_buffer" begin
    len = 7
    refs = Primitive(Int32[0,1,2,1,0,3,2])
    data = List(["fire", "walk", "with", "me"])
    d = DictEncoding(refs, data)
    @test references(d) == refs
    @test levels(d) == data
    @test d[1] == "fire"
    @test d[2] == "walk"
    @test d[3] == "with"
    @test d[4] == "walk"
    @test d[5] == "fire"
    @test d[6] == "me"
    @test d[7] == "with"
    @test d[[1,4,3,6]] == ["fire", "walk", "with", "me"]
    @test d[[true,false,false,false,false,false,true]] == ["fire", "with"]
    @test d[:] == ["fire", "walk", "with", "walk", "fire", "me", "with"]
end


@testset "indexing_DictEncoding_construct" begin
    v = [-999, missing, 55, -999, 42]
    d = DictEncoding(categorical(v))
    pool = CategoricalPool{Int,Int32}([-999, 55, 42])
    ref = CategoricalArray{Union{Int,Missing},1}(Int32[1,0,2,1,3], pool)
    @test typeof(d.refs) == NullablePrimitive{Int32}
    @test typeof(d.pool) == Primitive{Int64}
    @test d[1] == -999
    @test ismissing(d[2])
    @test d[3] == 55
    @test d[4] == -999
    @test d[5] == 42
    @test d[[1,3,5]] ≅ v[[1,3,5]]
    @test d[[false,true,false,true,false]] ≅ v[[false,true,false,true,false]]
    @test d[1:end] ≅ v
    @test categorical(d) ≅ ref
end

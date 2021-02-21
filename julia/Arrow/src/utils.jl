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

# Determines the total number of bytes needed to store `n` bytes with padding.
# Note that the Arrow standard requires buffers to be aligned to 8-byte boundaries.
padding(n::Integer, alignment) = ((n + alignment - 1) ÷ alignment) * alignment

paddinglength(n::Integer, alignment) = padding(n, alignment) - n

function writezeros(io::IO, n::Integer)
    s = 0
    for i ∈ 1:n
        s += Base.write(io, 0x00)
    end
    s
end

# efficient writing of arrays
writearray(io, col) = writearray(io, maybemissing(eltype(col)), col)

function writearray(io::IO, ::Type{T}, col) where {T}
    if col isa Vector{T}
        n = Base.write(io, col)
    elseif isbitstype(T) && (col isa Vector{Union{T, Missing}} || col isa SentinelVector{T, T, Missing, Vector{T}})
        # need to write the non-selector bytes of isbits Union Arrays
        n = Base.unsafe_write(io, pointer(col), sizeof(T) * length(col))
    elseif col isa ChainedVector
        n = 0
        for A in col.arrays
            n += writearray(io, T, A)
        end
    else
        n = 0
        data = Vector{UInt8}(undef, sizeof(col))
        buf = IOBuffer(data; write=true)
        for x in col
            n += Base.write(buf, coalesce(x, ArrowTypes.default(T)))
        end
        n = Base.write(io, take!(buf))
    end
    return n
end

getbit(v::UInt8, n::Integer) = Bool((v & 0x02^(n - 1)) >> (n - 1))

function setbit(v::UInt8, b::Bool, n::Integer)
    if b
        v | 0x02^(n - 1)
    else
        v & (0xff ⊻ 0x02^(n - 1))
    end
end

# Determines the number of bytes used by `n` bits, optionally with padding.
function bitpackedbytes(n::Integer, alignment)
    ℓ = cld(n, 8)
    return ℓ + paddinglength(ℓ, alignment)
end

# count # of missing elements in an iterable
nullcount(col) = count(ismissing, col)

# like startswith/endswith for strings, but on byte buffers
function _startswith(a::AbstractVector{UInt8}, pos::Integer, b::AbstractVector{UInt8})
    for i = 1:length(b)
        @inbounds check = a[pos + i - 1] == b[i]
        check || return false
    end
    return true
end

function _endswith(a::AbstractVector{UInt8}, endpos::Integer, b::AbstractVector{UInt8})
    aoff = endpos - length(b) + 1
    for i = 1:length(b)
        @inbounds check = a[aoff] == b[i]
        check || return false
        aoff += 1
    end
    return true
end

# read a single element from a byte vector
# copied from read(::IOBuffer, T) in Base
function readbuffer(t::AbstractVector{UInt8}, pos::Integer, ::Type{T}) where {T}
    GC.@preserve t begin
        ptr::Ptr{T} = pointer(t, pos)
        x = unsafe_load(ptr)
    end
end

# given a number of unique values; what dict encoding _index_ type is most appropriate
encodingtype(n) = n < div(typemax(Int8), 2) ? Int8 : n < div(typemax(Int16), 2) ? Int16 : n < div(typemax(Int32), 2) ? Int32 : Int64

# lazily call convert(T, x) on getindex for each x in data
struct Converter{T, A} <: AbstractVector{T}
    data::A
end

converter(::Type{T}, x::A) where {T, A} = Converter{eltype(A) >: Missing ? Union{T, Missing} : T, A}(x)
converter(::Type{T}, x::ChainedVector{A}) where {T, A} = ChainedVector([converter(T, x) for x in x.arrays])

Base.IndexStyle(::Type{<:Converter}) = Base.IndexLinear()
Base.size(x::Converter) = (length(x.data),)
Base.eltype(x::Converter{T, A}) where {T, A} = T
Base.getindex(x::Converter{T}, i::Int) where {T} = ArrowTypes.arrowconvert(T, getindex(x.data, i))

maybemissing(::Type{T}) where {T} = T === Missing ? Missing : Base.nonmissingtype(T)

function getfooter(filebytes)
    len = readbuffer(filebytes, length(filebytes) - 9, Int32)
    FlatBuffers.getrootas(Meta.Footer, filebytes[end-(9 + len):end-10], 0)
end

function getrb(filebytes)
    f = getfooter(filebytes)
    rb = f.recordBatches[1]
    return filebytes[rb.offset+1:(rb.offset+1+rb.metaDataLength)]
    # FlatBuffers.getrootas(Meta.Message, filebytes, rb.offset)
end

function readmessage(filebytes, off=9)
    @assert readbuffer(filebytes, off, UInt32) === 0xFFFFFFFF
    len = readbuffer(filebytes, off + 4, Int32)

    FlatBuffers.getrootas(Meta.Message, filebytes, off + 8)
end

# a custom Channel type that only allows put!-ing objects in a specific, monotonically increasing order
struct OrderedChannel{T}
    chan::Channel{T}
    cond::Threads.Condition
    i::Ref{Int}
end

OrderedChannel{T}(sz) where {T} = OrderedChannel{T}(Channel{T}(sz), Threads.Condition(), Ref(1))
Base.iterate(ch::OrderedChannel, st...) = iterate(ch.chan, st...)

macro lock(obj, expr)
    esc(quote
        lock($obj)
        try
            $expr
        finally
            unlock($obj)
        end
    end)
end

# when put!-ing an object, operation may have to wait until other tasks have put their
# objects to ensure the channel is ordered correctly
function Base.put!(ch::OrderedChannel{T}, x::T, i::Integer, incr::Bool=false) where {T}
    @lock ch.cond begin
        while ch.i[] < i
            # channel index too early, need to wait for other tasks to put their objects first
            wait(ch.cond)
        end
        # now it's our turn
        put!(ch.chan, x)
        if incr
            ch.i[] += 1
        end
        # wake up tasks that may be waiting to put their objects
        notify(ch.cond)
    end
    return
end

function Base.close(ch::OrderedChannel)
    @lock ch.cond begin
        # just need to ensure any tasks waiting to put their tasks have had a chance to put
        while Base.n_waiters(ch.cond) > 0
            wait(ch.cond)
        end
        close(ch.chan)
    end
    return
end

struct Lockable{T}
    x::T
    lock::ReentrantLock
end

Lockable(x::T) where {T} = Lockable{T}(x, ReentrantLock())

Base.lock(x::Lockable) = lock(x.lock)
Base.unlock(x::Lockable) = unlock(x.lock)

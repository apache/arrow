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

const fileIdentifierLength = 4

"""
Scalar
A Union of the Julia types `T <: Number` that are allowed in FlatBuffers schema
"""
const Scalar = Union{Bool,
Int8, Int16, Int32, Int64,
UInt8, UInt16, UInt32, UInt64,
Float32, Float64, Enum}

"""
Builder is a state machine for creating FlatBuffer objects.
Use a Builder to construct object(s) starting from leaf nodes.

A Builder constructs byte buffers in a last-first manner for simplicity and
performance.
"""
mutable struct Builder
    bytes::Vector{UInt8}
    minalign::Int
    vtable::Vector{UOffsetT}
    objectend::UOffsetT
    vtables::Vector{UOffsetT}
    head::UOffsetT
    nested::Bool
    finished::Bool
    sharedstrings::Dict{String, UOffsetT}
end

bytes(b::Builder) = getfield(b, :bytes)

Builder(size=0) = Builder(zeros(UInt8, size), 1, UOffsetT[], UOffsetT(0), UOffsetT[], UOffsetT(size), false, false, Dict{String, UOffsetT}())

function reset!(b::Builder)
    empty!(b.bytes)
    empty!(b.vtable)
    emtpy!(b.vtables)
    empty!(b.sharedstrings)
    b.minalign = 1
    b.nested = false
    b.finished = false
    b.head = 0
    return
end

Base.write(sink::Builder, o, x::Union{Bool,UInt8}) = sink.bytes[o+1] = UInt8(x)
function Base.write(sink::Builder, off, x::T) where {T}
    off += 1
    for (i, ind) = enumerate(off:(off + sizeof(T) - 1))
        sink.bytes[ind] = (x >> ((i-1) * 8)) % UInt8
    end
end
Base.write(b::Builder, o, x::Float32) = write(b, o, reinterpret(UInt32, x))
Base.write(b::Builder, o, x::Float64) = write(b, o, reinterpret(UInt64, x))
Base.write(b::Builder, o, x::Enum) = write(b, o, basetype(x)(x))

"""
`finishedbytes` returns a pointer to the written data in the byte buffer.
Panics if the builder is not in a finished state (which is caused by calling
`finish!()`).
"""
function finishedbytes(b::Builder)
    assertfinished(b)
    return view(b.bytes, (b.head + 1):length(b.bytes))
end

function startobject!(b::Builder, numfields)
    assertnotnested(b)
    b.nested = true
    resize!(b.vtable, numfields)
    fill!(b.vtable, 0)
    b.objectend = offset(b)
    return
end

"""
WriteVtable serializes the vtable for the current object, if applicable.

Before writing out the vtable, this checks pre-existing vtables for equality
to this one. If an equal vtable is found, point the object to the existing
vtable and return.

Because vtable values are sensitive to alignment of object data, not all
logically-equal vtables will be deduplicated.

A vtable has the following format:
<VOffsetT: size of the vtable in bytes, including this value>
<VOffsetT: size of the object in bytes, including the vtable offset>
<VOffsetT: offset for a field> * N, where N is the number of fields in
the schema for this type. Includes deprecated fields.
Thus, a vtable is made of 2 + N elements, each SizeVOffsetT bytes wide.

An object has the following format:
<SOffsetT: offset to this object's vtable (may be negative)>
<byte: data>+
"""
function writevtable!(b::Builder)
    # Prepend a zero scalar to the object. Later in this function we'll
    # write an offset here that points to the object's vtable:
    prepend!(b, SOffsetT(0))

    objectOffset = offset(b)
    existingVtable = UOffsetT(0)

    # Trim vtable of trailing zeroes.
    i = findlast(!iszero, b.vtable)
    if i !== nothing
        resize!(b.vtable, i)
    end
    
    # Search backwards through existing vtables, because similar vtables
    # are likely to have been recently appended. See
    # BenchmarkVtableDeduplication for a case in which this heuristic
    # saves about 30% of the time used in writing objects with duplicate
    # tables.
    for i = length(b.vtables):-1:1
        # Find the other vtable, which is associated with `i`:
        vt2Offset = b.vtables[i]
        vt2Start = length(b.bytes) - vt2Offset
        vt2Len = readbuffer(b.bytes, vt2Start, VOffsetT)

        metadata = VtableMetadataFields * sizeof(VOffsetT)
        vt2End = vt2Start + vt2Len
        vt2 = view(b.bytes, (vt2Start + metadata + 1):vt2End) #TODO: might need a +1 on the start of range here

        # Compare the other vtable to the one under consideration.
        # If they are equal, store the offset and break:
        if vtableEqual(b.vtable, objectOffset, vt2)
            existingVtable = vt2Offset
            break
        end
    end

    if existingVtable == 0
        # Did not find a vtable, so write this one to the buffer.

        # Write out the current vtable in reverse , because
        # serialization occurs in last-first order:
        for i = length(b.vtable):-1:1
            off::UOffsetT = 0
            if b.vtable[i] != 0
                # Forward reference to field;
                # use 32bit number to assert no overflow:
                off = objectOffset - b.vtable[i]
            end
            prepend!(b, VOffsetT(off))
        end

        # The two metadata fields are written last.

        # First, store the object bytesize:
        objectSize = objectOffset - b.objectend
        prepend!(b, VOffsetT(objectSize))

        # Second, store the vtable bytesize:
        vbytes = (length(b.vtable) + VtableMetadataFields) * sizeof(VOffsetT)
        prepend!(b, VOffsetT(vbytes))

        # Next, write the offset to the new vtable in the
        # already-allocated SOffsetT at the beginning of this object:
        objectStart = SOffsetT(length(b.bytes) - objectOffset)
        write(b, objectStart, SOffsetT(offset(b) - objectOffset))

        # Finally, store this vtable in memory for future
        # deduplication:
        push!(b.vtables, offset(b))
    else
        # Found a duplicate vtable.

        objectStart = SOffsetT(length(b.bytes) - objectOffset)
        b.head = objectStart

        # Write the offset to the found vtable in the
        # already-allocated SOffsetT at the beginning of this object:
        write(b, b.head, SOffsetT(existingVtable) - SOffsetT(objectOffset))
    end

    empty!(b.vtable)
    return objectOffset
end

"""
`endobject` writes data necessary to finish object construction.
"""
function endobject!(b::Builder)
    assertnested(b)
    n = writevtable!(b)
    b.nested = false
    return n
end

offset(b::Builder) = UOffsetT(length(b.bytes) - b.head)

pad!(b::Builder, n) = foreach(x->place!(b, 0x00), 1:n)

"""
`prep!` prepares to write an element of `size` after `additionalbytes`
have been written, e.g. if you write a string, you need to align such
the int length field is aligned to sizeof(Int32), and the string data follows it
directly.
If all you need to do is align, `additionalbytes` will be 0.
"""
function prep!(b::Builder, size, additionalbytes)
    # Track the biggest thing we've ever aligned to.
    if size > b.minalign
        b.minalign = size
    end
    # Find the amount of alignment needed such that `size` is properly
    # aligned after `additionalBytes`:
    alignsize = xor(Int(-1), (length(b.bytes) - b.head) + additionalbytes) + 1
    alignsize &= (size - 1)

    # Reallocate the buffer if needed:
    totalsize = alignsize + size + additionalbytes
    if b.head <= totalsize
        len = length(b.bytes)
        prepend!(b.bytes, zeros(UInt8, totalsize))
        b.head += length(b.bytes) - len
    end
    pad!(b, alignsize)
    return
end

function Base.prepend!(b::Builder, x::T) where {T}
    prep!(b, sizeof(T), 0)
    place!(b, x)
    return
end

function prependoffset!(b::Builder, off)
    prep!(b, sizeof(Int32), 0) # Ensure alignment is already done.
    if !(off <= offset(b))
        throw(ArgumentError("unreachable: $off <= $(offset(b))"))
    end
    place!(b, SOffsetT(offset(b) - off + sizeof(SOffsetT)))
    return
end

function prependoffsetslot!(b::Builder, o::Int, x::T, d) where {T}
    if x != T(d)
        prependoffset!(b, x)
        slot!(b, o)
    end
    return
end

"""
`startvector` initializes bookkeeping for writing a new vector.

A vector has the following format:
<UOffsetT: number of elements in this vector>
<T: data>+, where T is the type of elements of this vector.
"""
function startvector!(b::Builder, elemSize, numElems, alignment)
    assertnotnested(b)
    b.nested = true
    prep!(b, sizeof(UInt32), elemSize * numElems)
    prep!(b, alignment, elemSize * numElems)
    return offset(b)
end

"""
`endvector` writes data necessary to finish vector construction.
"""
function endvector!(b::Builder, vectorNumElems)
    assertnested(b)
    place!(b, UOffsetT(vectorNumElems))
    b.nested = false
    return offset(b)
end

function createsharedstring!(b::Builder, s::AbstractString)
    get!(b.sharedstrings, s) do
        createstring!(b, s)
    end
end

"""
`createstring!` writes a null-terminated string as a vector.
"""
function createstring!(b::Builder, s::Union{AbstractString, AbstractVector{UInt8}})
    assertnotnested(b)
    b.nested = true
    s = codeunits(s)
    prep!(b, sizeof(UInt32), sizeof(s) + 1)
    place!(b, UInt8(0))

    l = sizeof(s)

    b.head -= l
    copyto!(b.bytes, b.head+1, s, 1, l)
    return endvector!(b, sizeof(s))
end

createbytevector(b::Builder, v) = createstring!(b, v)

function assertnested(b::Builder)
    # If you get this assert, you're in an object while trying to write
    # data that belongs outside of an object.
    # To fix this, write non-inline data (like vectors) before creating
    # objects.
    if !b.nested
        throw(ArgumentError("Incorrect creation order: must be inside object."))
    end
    return
end

function assertnotnested(b::Builder)
    # If you hit this, you're trying to construct a Table/Vector/String
    # during the construction of its parent table (between the MyTableBuilder
    # and builder.Finish()).
    # Move the creation of these view-objects to above the MyTableBuilder to
    # not get this assert.
    # Ignoring this assert may appear to work in simple cases, but the reason
    # it is here is that storing objects in-line may cause vtable offsets
    # to not fit anymore. It also leads to vtable duplication.
    if b.nested
        throw(ArgumentError("Incorrect creation order: object must not be nested."))
    end
    return
end

function assertfinished(b::Builder)
    # If you get this assert, you're attempting to get access a buffer
    # which hasn't been finished yet. Be sure to call builder.Finish()
    # with your root table.
    # If you really need to access an unfinished buffer, use the bytes
    # buffer directly.
    if !b.finished
        throw(ArgumentError("Incorrect use of FinishedBytes(): must call 'Finish' first."))
    end
end

"""
`prependslot!` prepends a `T` onto the object at vtable slot `o`.
If value `x` equals default `d`, then the slot will be set to zero and no
other data will be written.
"""
function prependslot!(b::Builder, o::Int, x::T, d, sh=false) where {T <: Scalar}
    if x != T(d)
        prepend!(b, x)
        slot!(b, o)
    end
    return
end

"""
`prependstructslot!` prepends a struct onto the object at vtable slot `o`.
Structs are stored inline, so nothing additional is being added.
In generated code, `d` is always 0.
"""
function prependstructslot!(b::Builder, voffset, x, d)
    if x != d
        assertnested(b)
        if x != offset(b)
            throw(ArgumentError("inline data write outside of object"))
        end
        slot!(b, voffset)
    end
    return
end

"""
`slot!` sets the vtable key `voffset` to the current location in the buffer.
"""
function slot!(b::Builder, slotnum)
    b.vtable[slotnum + 1] = offset(b)
end

# FinishWithFileIdentifier finalizes a buffer, pointing to the given `rootTable`.
# as well as applys a file identifier
function finishwithfileidentifier(b::Builder, rootTable, fid)
    if length(fid) != fileIdentifierLength
        error("incorrect file identifier length")
    end
    # In order to add a file identifier to the flatbuffer message, we need
    # to prepare an alignment and file identifier length
    prep!(b, b.minalign, sizeof(Int32) + fileIdentifierLength)
    for i = fileIdentifierLength:-1:1
        # place the file identifier
        place!(b, fid[i])
    end
    # finish
    finish!(b, rootTable)
end

"""
`finish!` finalizes a buffer, pointing to the given `rootTable`.
"""
function finish!(b::Builder, rootTable)
    assertnotnested(b)
    prep!(b, b.minalign, sizeof(UOffsetT))
    prependoffset!(b, UOffsetT(rootTable))
    b.finished = true
    return
end

"vtableEqual compares an unwritten vtable to a written vtable."
function vtableEqual(a::Vector{UOffsetT}, objectStart, b::AbstractVector{UInt8})
    if length(a) * sizeof(VOffsetT) != length(b)
        return false
    end

    for i = 0:(length(a)-1)
        x = read(IOBuffer(view(b, (i * sizeof(VOffsetT) + 1):length(b))), VOffsetT)

        # Skip vtable entries that indicate a default value.
        x == 0 && a[i+1] == 0 && continue

        y = objectStart - a[i+1]
        x != y && return false
    end
    return true
end

"""
`place!` prepends a `T` to the Builder, without checking for space.
"""
function place!(b::Builder, x::T) where {T}
    b.head -= sizeof(T)
    write(b, b.head, x)
    return
end

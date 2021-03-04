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

module FlatBuffers

const UOffsetT = UInt32
const SOffsetT = Int32
const VOffsetT = UInt16
const VtableMetadataFields = 2

basetype(::Enum) = UInt8

function readbuffer(t::AbstractVector{UInt8}, pos::Integer, ::Type{Bool})
    @inbounds b = t[pos + 1]
    return b === 0x01
end

function readbuffer(t::AbstractVector{UInt8}, pos::Integer, ::Type{T}) where {T}
    GC.@preserve t begin
        ptr = convert(Ptr{T}, pointer(t, pos + 1))
        x = unsafe_load(ptr)
    end
end

include("builder.jl")
include("table.jl")

function Base.show(io::IO, x::TableOrStruct)
    print(io, "$(typeof(x))")
    if isempty(propertynames(x))
        print(io, "()")
    else
        show(io, NamedTuple{propertynames(x)}(Tuple(getproperty(x, y) for y in propertynames(x))))
    end
end

abstract type ScopedEnum{T<:Integer} <: Enum{T} end

macro scopedenum(T, syms...)
    if isempty(syms)
        throw(ArgumentError("no arguments given for ScopedEnum $T"))
    end
    basetype = Int32
    typename = T
    if isa(T, Expr) && T.head === :(::) && length(T.args) == 2 && isa(T.args[1], Symbol)
        typename = T.args[1]
        basetype = Core.eval(__module__, T.args[2])
        if !isa(basetype, DataType) || !(basetype <: Integer) || !isbitstype(basetype)
            throw(ArgumentError("invalid base type for ScopedEnum $typename, $T=::$basetype; base type must be an integer primitive type"))
        end
    elseif !isa(T, Symbol)
        throw(ArgumentError("invalid type expression for ScopedEnum $T"))
    end
    values = basetype[]
    seen = Set{Symbol}()
    namemap = Dict{basetype,Symbol}()
    lo = hi = 0
    i = zero(basetype)
    hasexpr = false

    if length(syms) == 1 && syms[1] isa Expr && syms[1].head === :block
        syms = syms[1].args
    end
    for s in syms
        s isa LineNumberNode && continue
        if isa(s, Symbol)
            if i == typemin(basetype) && !isempty(values)
                throw(ArgumentError("overflow in value \"$s\" of ScopedEnum $typename"))
            end
        elseif isa(s, Expr) &&
               (s.head === :(=) || s.head === :kw) &&
               length(s.args) == 2 && isa(s.args[1], Symbol)
            i = Core.eval(__module__, s.args[2]) # allow exprs, e.g. uint128"1"
            if !isa(i, Integer)
                throw(ArgumentError("invalid value for ScopedEnum $typename, $s; values must be integers"))
            end
            i = convert(basetype, i)
            s = s.args[1]
            hasexpr = true
        else
            throw(ArgumentError(string("invalid argument for ScopedEnum ", typename, ": ", s)))
        end
        if !Base.isidentifier(s)
            throw(ArgumentError("invalid name for ScopedEnum $typename; \"$s\" is not a valid identifier"))
        end
        if hasexpr && haskey(namemap, i)
            throw(ArgumentError("both $s and $(namemap[i]) have value $i in ScopedEnum $typename; values must be unique"))
        end
        namemap[i] = s
        push!(values, i)
        if s in seen
            throw(ArgumentError("name \"$s\" in ScopedEnum $typename is not unique"))
        end
        push!(seen, s)
        if length(values) == 1
            lo = hi = i
        else
            lo = min(lo, i)
            hi = max(hi, i)
        end
        i += oneunit(i)
    end
    defs = Expr(:block)
    if isa(typename, Symbol)
        for (i, sym) in namemap
            push!(defs.args, :(const $(esc(sym)) = $(esc(typename))($i)))
        end
    end
    mod = Symbol(typename, "Module")
    syms = Tuple(Base.values(namemap))
    blk = quote
        module $(esc(mod))
            export $(esc(typename))
            # enum definition
            primitive type $(esc(typename)) <: ScopedEnum{$(basetype)} $(sizeof(basetype) * 8) end
            function $(esc(typename))(x::Integer)
                $(Base.Enums.membershiptest(:x, values)) || Base.Enums.enum_argument_error($(Expr(:quote, typename)), x)
                return Core.bitcast($(esc(typename)), convert($(basetype), x))
            end
            if isdefined(Base.Enums, :namemap)
                Base.Enums.namemap(::Type{$(esc(typename))}) = $(esc(namemap))
            end
            Base.getproperty(::Type{$(esc(typename))}, sym::Symbol) = sym in $syms ? getfield($(esc(mod)), sym) : getfield($(esc(typename)), sym)
            Base.typemin(x::Type{$(esc(typename))}) = $(esc(typename))($lo)
            Base.typemax(x::Type{$(esc(typename))}) = $(esc(typename))($hi)
            let insts = (Any[ $(esc(typename))(v) for v in $values ]...,)
                Base.instances(::Type{$(esc(typename))}) = insts
            end
            FlatBuffers.basetype(::$(esc(typename))) = $(basetype)
            FlatBuffers.basetype(::Type{$(esc(typename))}) = $(basetype)
            $defs
        end
    end
    push!(blk.args, :nothing)
    blk.head = :toplevel
    push!(blk.args, :(using .$mod))
    return blk
end

end # module

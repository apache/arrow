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

struct FieldNode <: FlatBuffers.Struct
    bytes::Vector{UInt8}
    pos::Base.Int
end

FlatBuffers.structsizeof(::Base.Type{FieldNode}) = 16

Base.propertynames(x::FieldNode) = (:length, :null_count)

function Base.getproperty(x::FieldNode, field::Symbol)
    if field === :length
        return FlatBuffers.get(x, FlatBuffers.pos(x), Int64)
    elseif field === :null_count
        return FlatBuffers.get(x, FlatBuffers.pos(x) + 8, Int64)
    end
    return nothing
end

function createFieldNode(b::FlatBuffers.Builder, length::Int64, nullCount::Int64)
    FlatBuffers.prep!(b, 8, 16)
    prepend!(b, nullCount)
    prepend!(b, length)
    return FlatBuffers.offset(b)
end

FlatBuffers.@scopedenum CompressionType::Int8 LZ4_FRAME ZSTD

FlatBuffers.@scopedenum BodyCompressionMethod::Int8 BUFFER

struct BodyCompression <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::BodyCompression) = (:codec, :method)

function Base.getproperty(x::BodyCompression, field::Symbol)
    if field === :codec
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), CompressionType)
        return CompressionType.LZ4_FRAME
    elseif field === :method
        o = FlatBuffers.offset(x, 6)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), BodyCompressionMethod)
        return BodyCompressionMethod.BUFFER
    end
    return nothing
end

bodyCompressionStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 2)
bodyCompressionAddCodec(b::FlatBuffers.Builder, codec::CompressionType) = FlatBuffers.prependslot!(b, 0, codec, 0)
bodyCompressionAddMethod(b::FlatBuffers.Builder, method::BodyCompressionMethod) = FlatBuffers.prependslot!(b, 1, method, 0)
bodyCompressionEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct RecordBatch <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::RecordBatch) = (:length, :nodes, :buffers, :compression)

function Base.getproperty(x::RecordBatch, field::Symbol)
    if field === :length
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int64)
    elseif field === :nodes
        o = FlatBuffers.offset(x, 6)
        if o != 0
            return FlatBuffers.Array{FieldNode}(x, o)
        end
    elseif field === :buffers
        o = FlatBuffers.offset(x, 8)
        if o != 0
            return FlatBuffers.Array{Buffer}(x, o)
        end
    elseif field === :compression
        o = FlatBuffers.offset(x, 10)
        if o != 0
            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))
            return FlatBuffers.init(BodyCompression, FlatBuffers.bytes(x), y)
        end
    end
    return nothing
end

recordBatchStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 4)
recordBatchAddLength(b::FlatBuffers.Builder, length::Int64) = FlatBuffers.prependslot!(b, 0, length, 0)
recordBatchAddNodes(b::FlatBuffers.Builder, nodes::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 1, nodes, 0)
recordBatchStartNodesVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 16, numelems, 8)
recordBatchAddBuffers(b::FlatBuffers.Builder, buffers::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 2, buffers, 0)
recordBatchStartBuffersVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 16, numelems, 8)
recordBatchAddCompression(b::FlatBuffers.Builder, c::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 3, c, 0)
recordBatchEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct DictionaryBatch <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::DictionaryBatch) = (:id, :data, :isDelta)

function Base.getproperty(x::DictionaryBatch, field::Symbol)
    if field === :id
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int64)
        return Int64(0)
    elseif field === :data
        o = FlatBuffers.offset(x, 6)
        if o != 0
            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))
            return FlatBuffers.init(RecordBatch, FlatBuffers.bytes(x), y)
        end
    elseif field === :isDelta
        o = FlatBuffers.offset(x, 8)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Base.Bool)
        return false
    end
    return nothing
end

dictionaryBatchStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 3)
dictionaryBatchAddId(b::FlatBuffers.Builder, id::Int64) = FlatBuffers.prependslot!(b, 0, id, 0)
dictionaryBatchAddData(b::FlatBuffers.Builder, data::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 1, data, 0)
dictionaryBatchAddIsDelta(b::FlatBuffers.Builder, isdelta::Base.Bool) = FlatBuffers.prependslot!(b, 2, isdelta, false)
dictionaryBatchEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

function MessageHeader(b::UInt8)
    b == 1 && return Schema
    b == 2 && return DictionaryBatch
    b == 3 && return RecordBatch
    # b == 4 && return Tensor
    # b == 5 && return SparseTensor
    return nothing
end

function MessageHeader(::Base.Type{T})::Int16 where {T}
    T == Schema && return 1
    T == DictionaryBatch && return 2
    T == RecordBatch && return 3
    # T == Tensor && return 4
    # T == SparseTensor && return 5
    return 0
end

struct Message <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Message) = (:version, :header, :bodyLength, :custom_metadata)

function Base.getproperty(x::Message, field::Symbol)
    if field === :version
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), MetadataVersion)
    elseif field === :header
        o = FlatBuffers.offset(x, 6)
        if o != 0
            T = MessageHeader(FlatBuffers.get(x, o + FlatBuffers.pos(x), UInt8))
            o = FlatBuffers.offset(x, 8)
            pos = FlatBuffers.union(x, o)
            if o != 0
                return FlatBuffers.init(T, FlatBuffers.bytes(x), pos)
            end
        end
    elseif field === :bodyLength
        o = FlatBuffers.offset(x, 10)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int64)
        return Int64(0)
    elseif field === :custom_metadata
        o = FlatBuffers.offset(x, 12)
        if o != 0
            return FlatBuffers.Array{KeyValue}(x, o)
        end
    end
    return nothing
end

messageStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 5)
messageAddVersion(b::FlatBuffers.Builder, version::MetadataVersion) = FlatBuffers.prependslot!(b, 0, version, 0)
messageAddHeaderType(b::FlatBuffers.Builder, ::Core.Type{T}) where {T} = FlatBuffers.prependslot!(b, 1, MessageHeader(T), 0)
messageAddHeader(b::FlatBuffers.Builder, header::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 2, header, 0)
messageAddBodyLength(b::FlatBuffers.Builder, bodyLength::Int64) = FlatBuffers.prependslot!(b, 3, bodyLength, 0)
messageAddCustomMetadata(b::FlatBuffers.Builder, meta::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 4, meta, 0)
messageStartCustomMetadataVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 4, numelems, 4)
messageEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)
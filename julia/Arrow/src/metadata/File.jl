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

struct Footer <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Footer) = (:version, :schema, :dictionaries, :recordBatches, :custom_metadata)

function Base.getproperty(x::Footer, field::Symbol)
    if field === :version
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), MetadataVersion)
        return MetadataVersion.V1
    elseif field === :schema
        o = FlatBuffers.offset(x, 6)
        if o != 0
            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))
            return FlatBuffers.init(Schema, FlatBuffers.bytes(x), y)
        end
    elseif field === :dictionaries
        o = FlatBuffers.offset(x, 8)
        if o != 0
            return FlatBuffers.Array{Block}(x, o)
        end
    elseif field === :recordBatches
        o = FlatBuffers.offset(x, 10)
        if o != 0
            return FlatBuffers.Array{Block}(x, o)
        end
    elseif field === :custom_metadata
        o = FlatBuffers.offset(x, 12)
        if o != 0
            return FlatBuffers.Array{KeyValue}(x, o)
        end
    end
    return nothing
end

footerStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 4)
footerAddVersion(b::FlatBuffers.Builder, version::MetadataVersion) = FlatBuffers.prependslot!(b, 0, version, 0)
footerAddSchema(b::FlatBuffers.Builder, schema::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 1, schema, 0)
footerAddDictionaries(b::FlatBuffers.Builder, dictionaries::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 2, dictionaries, 0)
footerStartDictionariesVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 24, numelems, 8)
footerAddRecordBatches(b::FlatBuffers.Builder, recordbatches::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 3, recordbatches, 0)
footerStartRecordBatchesVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 24, numelems, 8)
footerEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Block <: FlatBuffers.Struct
    bytes::Vector{UInt8}
    pos::Base.Int
end

FlatBuffers.structsizeof(::Base.Type{Block}) = 24

Base.propertynames(x::Block) = (:offset, :metaDataLength, :bodyLength)

function Base.getproperty(x::Block, field::Symbol)
    if field === :offset
        return FlatBuffers.get(x, FlatBuffers.pos(x), Int64)
    elseif field === :metaDataLength
        return FlatBuffers.get(x, FlatBuffers.pos(x) + 8, Int32)
    elseif field === :bodyLength
        return FlatBuffers.get(x, FlatBuffers.pos(x) + 16, Int64)
    end
    return nothing
end

function createBlock(b::FlatBuffers.Builder, offset::Int64, metadatalength::Int32, bodylength::Int64)
    FlatBuffers.prep!(b, 8, 24)
    prepend!(b, bodylength)
    FlatBuffers.pad!(b, 4)
    prepend!(b, metadatalength)
    prepend!(b, offset)
    return FlatBuffers.offset(b)
end
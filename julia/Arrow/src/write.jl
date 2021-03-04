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

const OBJ_METADATA = IdDict{Any, Dict{String, String}}()

"""
    Arrow.setmetadata!(x, metadata::Dict{String, String})

Set the metadata for any object, provided as a `Dict{String, String}`.
Metadata attached to a table or column will be serialized when written
as a stream or file.
"""
function setmetadata!(x, meta::Dict{String, String})
    OBJ_METADATA[x] = meta
    return
end

"""
    Arrow.getmetadata(x) => Dict{String, String}

Retrieve any metadata (as a `Dict{String, String}`) attached to an object.

Metadata may be attached to any object via [`Arrow.setmetadata!`](@ref),
or deserialized via the arrow format directly (the format allows attaching metadata
to table, column, and other objects).
"""
getmetadata(x, default=nothing) = get(OBJ_METADATA, x, default)

"""
    Arrow.write(io::IO, tbl)
    Arrow.write(file::String, tbl)
    tbl |> Arrow.write(io_or_file)

Write any [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible `tbl` out as arrow formatted data.
Providing an `io::IO` argument will cause the data to be written to it
in the ["streaming" format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format), unless `file=true` keyword argument is passed.
Providing a `file::String` argument will result in the ["file" format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format) being written.

Multiple record batches will be written based on the number of
`Tables.partitions(tbl)` that are provided; by default, this is just
one for a given table, but some table sources support automatic
partitioning. Note you can turn multiple table objects into partitions
by doing `Tables.partitioner([tbl1, tbl2, ...])`, but note that
each table must have the exact same `Tables.Schema`.

By default, `Arrow.write` will use multiple threads to write multiple
record batches simultaneously (e.g. if julia is started with `julia -t 8` or the `JULIA_NUM_THREADS` environment variable is set).

Supported keyword arguments to `Arrow.write` include:
  * `compress`: possible values include `:lz4`, `:zstd`, or your own initialized `LZ4FrameCompressor` or `ZstdCompressor` objects; will cause all buffers in each record batch to use the respective compression encoding
  * `alignment::Int=8`: specify the number of bytes to align buffers to when written in messages; strongly recommended to only use alignment values of 8 or 64 for modern memory cache line optimization
  * `dictencode::Bool=false`: whether all columns should use dictionary encoding when being written; to dict encode specific columns, wrap the column/array in `Arrow.DictEncode(col)`
  * `dictencodenested::Bool=false`: whether nested data type columns should also dict encode nested arrays/buffers; other language implementations [may not support this](https://arrow.apache.org/docs/status.html)
  * `denseunions::Bool=true`: whether Julia `Vector{<:Union}` arrays should be written using the dense union layout; passing `false` will result in the sparse union layout
  * `largelists::Bool=false`: causes list column types to be written with Int64 offset arrays; mainly for testing purposes; by default, Int64 offsets will be used only if needed
  * `file::Bool=false`: if a an `io` argument is being written to, passing `file=true` will cause the arrow file format to be written instead of just IPC streaming
"""
function write end

write(io_or_file; kw...) = x -> write(io_or_file, x; kw...)

function write(file::String, tbl; largelists::Bool=false, compress::Union{Nothing, Symbol, LZ4FrameCompressor, ZstdCompressor}=nothing, denseunions::Bool=true, dictencode::Bool=false, dictencodenested::Bool=false, alignment::Int=8)
    open(file, "w") do io
        write(io, tbl, true, largelists, compress, denseunions, dictencode, dictencodenested, alignment)
    end
    return file
end

function write(io::IO, tbl; largelists::Bool=false, compress::Union{Nothing, Symbol, LZ4FrameCompressor, ZstdCompressor}=nothing, denseunions::Bool=true, dictencode::Bool=false, dictencodenested::Bool=false, alignment::Int=8, file::Bool=false)
    return write(io, tbl, file, largelists, compress, denseunions, dictencode, dictencodenested, alignment)
end

function write(io, source, writetofile, largelists, compress, denseunions, dictencode, dictencodenested, alignment)
    if compress === :lz4
        compress = LZ4_FRAME_COMPRESSOR[]
    elseif compress === :zstd
        compress = ZSTD_COMPRESSOR[]
    elseif compress isa Symbol
        throw(ArgumentError("unsupported compress keyword argument value: $compress. Valid values include `:lz4` or `:zstd`"))
    end
    if writetofile
        @debug 1 "starting write of arrow formatted file"
        Base.write(io, "ARROW1\0\0")
    end
    msgs = OrderedChannel{Message}(Inf)
    # build messages
    sch = Ref{Tables.Schema}()
    firstcols = Ref{Any}()
    dictencodings = Dict{Int64, Any}() # Lockable{DictEncoding}
    blocks = (Block[], Block[])
    # start message writing from channel
    tsk = Threads.@spawn for msg in msgs
        Base.write(io, msg, blocks, sch, alignment)
    end
    @sync for (i, tbl) in enumerate(Tables.partitions(source))
        @debug 1 "processing table partition i = $i"
        if i == 1
            cols = toarrowtable(tbl, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested)
            sch[] = Tables.schema(cols)
            firstcols[] = cols
            put!(msgs, makeschemamsg(sch[], cols), i)
            if !isempty(dictencodings)
                des = sort!(collect(dictencodings); by=x->x.first, rev=true)
                for (id, delock) in des
                    # assign dict encoding ids
                    de = delock.x
                    dictsch = Tables.Schema((:col,), (eltype(de.data),))
                    put!(msgs, makedictionarybatchmsg(dictsch, (col=de.data,), id, false, alignment), i)
                end
            end
            put!(msgs, makerecordbatchmsg(sch[], cols, alignment), i, true)
        else
            Threads.@spawn begin
                cols = toarrowtable(tbl, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested)
                if !isempty(cols.dictencodingdeltas)
                    for de in cols.dictencodingdeltas
                        dictsch = Tables.Schema((:col,), (eltype(de.data),))
                        put!(msgs, makedictionarybatchmsg(dictsch, (col=de.data,), de.id, true, alignment), i)
                    end
                end
                put!(msgs, makerecordbatchmsg(sch[], cols, alignment), i, true)
            end
        end
    end
    # close our message-writing channel, no further put!-ing is allowed
    close(msgs)
    # now wait for our message-writing task to finish writing
    wait(tsk)
    # write empty message
    if !writetofile
        Base.write(io, Message(UInt8[], nothing, 0, true, false), blocks, sch, alignment)
    end
    if writetofile
        b = FlatBuffers.Builder(1024)
        schfoot = makeschema(b, sch[], firstcols[])
        if !isempty(blocks[1])
            N = length(blocks[1])
            Meta.footerStartRecordBatchesVector(b, N)
            for blk in Iterators.reverse(blocks[1])
                Meta.createBlock(b, blk.offset, blk.metaDataLength, blk.bodyLength)
            end
            recordbatches = FlatBuffers.endvector!(b, N)
        else
            recordbatches = FlatBuffers.UOffsetT(0)
        end
        if !isempty(blocks[2])
            N = length(blocks[2])
            Meta.footerStartDictionariesVector(b, N)
            for blk in Iterators.reverse(blocks[2])
                Meta.createBlock(b, blk.offset, blk.metaDataLength, blk.bodyLength)
            end
            dicts = FlatBuffers.endvector!(b, N)
        else
            dicts = FlatBuffers.UOffsetT(0)
        end
        Meta.footerStart(b)
        Meta.footerAddVersion(b, Meta.MetadataVersion.V4)
        Meta.footerAddSchema(b, schfoot)
        Meta.footerAddDictionaries(b, dicts)
        Meta.footerAddRecordBatches(b, recordbatches)
        foot = Meta.footerEnd(b)
        FlatBuffers.finish!(b, foot)
        footer = FlatBuffers.finishedbytes(b)
        Base.write(io, footer)
        Base.write(io, Int32(length(footer)))
        Base.write(io, "ARROW1")
    end
    return io
end

struct ToArrowTable
    sch::Tables.Schema
    cols::Vector{Any}
    metadata::Union{Nothing, Dict{String, String}}
    dictencodingdeltas::Vector{DictEncoding}
end

function toarrowtable(x, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested)
    @debug 1 "converting input table to arrow formatted columns"
    cols = Tables.columns(x)
    meta = getmetadata(cols)
    sch = Tables.schema(cols)
    types = collect(sch.types)
    N = length(types)
    newcols = Vector{Any}(undef, N)
    newtypes = Vector{Type}(undef, N)
    dictencodingdeltas = DictEncoding[]
    Tables.eachcolumn(sch, cols) do col, i, nm
        newcol = toarrowvector(col, i, dictencodings, dictencodingdeltas; compression=compress, largelists=largelists, denseunions=denseunions, dictencode=dictencode, dictencodenested=dictencodenested)
        newtypes[i] = eltype(newcol)
        newcols[i] = newcol
    end
    minlen, maxlen = extrema(length, newcols)
    minlen == maxlen || throw(ArgumentError("columns with unequal lengths detected: $minlen < $maxlen"))
    return ToArrowTable(Tables.Schema(sch.names, newtypes), newcols, meta, dictencodingdeltas)
end

Tables.columns(x::ToArrowTable) = x
Tables.rowcount(x::ToArrowTable) = length(x.cols) == 0 ? 0 : length(x.cols[1])
Tables.schema(x::ToArrowTable) = x.sch
Tables.columnnames(x::ToArrowTable) = x.sch.names
Tables.getcolumn(x::ToArrowTable, i::Int) = x.cols[i]

struct Message
    msgflatbuf
    columns
    bodylen
    isrecordbatch::Bool
    blockmsg::Bool
end

struct Block
    offset::Int64
    metaDataLength::Int32
    bodyLength::Int64
end

function Base.write(io::IO, msg::Message, blocks, sch, alignment)
    metalen = padding(length(msg.msgflatbuf), alignment)
    @debug 1 "writing message: metalen = $metalen, bodylen = $(msg.bodylen), isrecordbatch = $(msg.isrecordbatch)"
    if msg.blockmsg
        push!(blocks[msg.isrecordbatch ? 1 : 2], Block(position(io), metalen + 8, msg.bodylen))
    end
    # now write the final message spec out
    # continuation byte
    n = Base.write(io, 0xFFFFFFFF)
    # metadata length
    n += Base.write(io, Int32(metalen))
    # message flatbuffer
    n += Base.write(io, msg.msgflatbuf)
    n += writezeros(io, paddinglength(length(msg.msgflatbuf), alignment))
    # message body
    if msg.columns !== nothing
        # write out buffers
        for col in Tables.Columns(msg.columns)
            writebuffer(io, col, alignment)
        end
    end
    return n
end

function makemessage(b, headerType, header, columns=nothing, bodylen=0)
    # write the message flatbuffer object
    Meta.messageStart(b)
    Meta.messageAddVersion(b, Meta.MetadataVersion.V5)
    Meta.messageAddHeaderType(b, headerType)
    Meta.messageAddHeader(b, header)
    Meta.messageAddBodyLength(b, Int64(bodylen))
    # Meta.messageAddCustomMetadata(b, meta)
    # Meta.messageStartCustomMetadataVector(b, num_meta_elems)
    msg = Meta.messageEnd(b)
    FlatBuffers.finish!(b, msg)
    return Message(FlatBuffers.finishedbytes(b), columns, bodylen, headerType == Meta.RecordBatch, headerType == Meta.RecordBatch || headerType == Meta.DictionaryBatch)
end

function makeschema(b, sch::Tables.Schema{names}, columns) where {names}
    # build Field objects
    N = length(names)
    fieldoffsets = [fieldoffset(b, names[i], columns.cols[i]) for i = 1:N]
    Meta.schemaStartFieldsVector(b, N)
    for off in Iterators.reverse(fieldoffsets)
        FlatBuffers.prependoffset!(b, off)
    end
    fields = FlatBuffers.endvector!(b, N)
    if columns.metadata !== nothing
        kvs = columns.metadata
        kvoffs = Vector{FlatBuffers.UOffsetT}(undef, length(kvs))
        for (i, (k, v)) in enumerate(kvs)
            koff = FlatBuffers.createstring!(b, String(k))
            voff = FlatBuffers.createstring!(b, String(v))
            Meta.keyValueStart(b)
            Meta.keyValueAddKey(b, koff)
            Meta.keyValueAddValue(b, voff)
            kvoffs[i] = Meta.keyValueEnd(b)
        end
        Meta.schemaStartCustomMetadataVector(b, length(kvs))
        for off in Iterators.reverse(kvoffs)
            FlatBuffers.prependoffset!(b, off)
        end
        meta = FlatBuffers.endvector!(b, length(kvs))
    else
        meta = FlatBuffers.UOffsetT(0)
    end
    # write schema object
    Meta.schemaStart(b)
    Meta.schemaAddEndianness(b, Meta.Endianness.Little)
    Meta.schemaAddFields(b, fields)
    Meta.schemaAddCustomMetadata(b, meta)
    return Meta.schemaEnd(b)
end

function makeschemamsg(sch::Tables.Schema, columns)
    @debug 1 "building schema message: sch = $sch"
    b = FlatBuffers.Builder(1024)
    schema = makeschema(b, sch, columns)
    return makemessage(b, Meta.Schema, schema)
end

function fieldoffset(b, name, col)
    nameoff = FlatBuffers.createstring!(b, String(name))
    T = eltype(col)
    nullable = T >: Missing
    # check for custom metadata
    if getmetadata(col) !== nothing
        kvs = getmetadata(col)
        kvoffs = Vector{FlatBuffers.UOffsetT}(undef, length(kvs))
        for (i, (k, v)) in enumerate(kvs)
            koff = FlatBuffers.createstring!(b, String(k))
            voff = FlatBuffers.createstring!(b, String(v))
            Meta.keyValueStart(b)
            Meta.keyValueAddKey(b, koff)
            Meta.keyValueAddValue(b, voff)
            kvoffs[i] = Meta.keyValueEnd(b)
        end
        Meta.fieldStartCustomMetadataVector(b, length(kvs))
        for off in Iterators.reverse(kvoffs)
            FlatBuffers.prependoffset!(b, off)
        end
        meta = FlatBuffers.endvector!(b, length(kvs))
    else
        meta = FlatBuffers.UOffsetT(0)
    end
    # build dictionary
    if isdictencoded(col)
        encodingtype = indtype(col)
        IT, inttype, _ = arrowtype(b, encodingtype)
        Meta.dictionaryEncodingStart(b)
        Meta.dictionaryEncodingAddId(b, Int64(getid(col)))
        Meta.dictionaryEncodingAddIndexType(b, inttype)
        # TODO: support isOrdered?
        Meta.dictionaryEncodingAddIsOrdered(b, false)
        dict = Meta.dictionaryEncodingEnd(b)
    else
        dict = FlatBuffers.UOffsetT(0)
    end
    type, typeoff, children = arrowtype(b, col)
    if children !== nothing
        Meta.fieldStartChildrenVector(b, length(children))
        for off in Iterators.reverse(children)
            FlatBuffers.prependoffset!(b, off)
        end
        children = FlatBuffers.endvector!(b, length(children))
    else
        Meta.fieldStartChildrenVector(b, 0)
        children = FlatBuffers.endvector!(b, 0)
    end
    # build field object
    if isdictencoded(col)
        @debug 1 "building field: name = $name, nullable = $nullable, T = $T, type = $type, inttype = $IT, dictionary id = $(getid(col))"
    else
        @debug 1 "building field: name = $name, nullable = $nullable, T = $T, type = $type"
    end
    Meta.fieldStart(b)
    Meta.fieldAddName(b, nameoff)
    Meta.fieldAddNullable(b, nullable)
    Meta.fieldAddTypeType(b, type)
    Meta.fieldAddType(b, typeoff)
    Meta.fieldAddDictionary(b, dict)
    Meta.fieldAddChildren(b, children)
    Meta.fieldAddCustomMetadata(b, meta)
    return Meta.fieldEnd(b)
end

struct FieldNode
    length::Int64
    null_count::Int64
end

struct Buffer
    offset::Int64
    length::Int64
end

function makerecordbatchmsg(sch::Tables.Schema{names, types}, columns, alignment) where {names, types}
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns, alignment)
    return makemessage(b, Meta.RecordBatch, recordbatch, columns, bodylen)
end

function makerecordbatch(b, sch::Tables.Schema{names, types}, columns, alignment) where {names, types}
    nrows = Tables.rowcount(columns)
    
    compress = nothing
    fieldnodes = FieldNode[]
    fieldbuffers = Buffer[]
    bufferoffset = 0
    for col in Tables.Columns(columns)
        if col isa Compressed
            compress = compressiontype(col)
        end
        bufferoffset = makenodesbuffers!(col, fieldnodes, fieldbuffers, bufferoffset, alignment)
    end
    @debug 1 "building record batch message: nrows = $nrows, sch = $sch, compress = $compress"

    # write field nodes objects
    FN = length(fieldnodes)
    Meta.recordBatchStartNodesVector(b, FN)
    for fn in Iterators.reverse(fieldnodes)
        Meta.createFieldNode(b, fn.length, fn.null_count)
    end
    nodes = FlatBuffers.endvector!(b, FN)

    # write buffer objects
    bodylen = 0
    BN = length(fieldbuffers)
    Meta.recordBatchStartBuffersVector(b, BN)
    for buf in Iterators.reverse(fieldbuffers)
        Meta.createBuffer(b, buf.offset, buf.length)
        bodylen += padding(buf.length, alignment)
    end
    buffers = FlatBuffers.endvector!(b, BN)

    # compression
    if compress !== nothing
        Meta.bodyCompressionStart(b)
        Meta.bodyCompressionAddCodec(b, compress)
        Meta.bodyCompressionAddMethod(b, Meta.BodyCompressionMethod.BUFFER)
        compression = Meta.bodyCompressionEnd(b)
    else
        compression = FlatBuffers.UOffsetT(0)
    end

    # write record batch object
    @debug 1 "built record batch message: nrows = $nrows, nodes = $fieldnodes, buffers = $fieldbuffers, compress = $compress, bodylen = $bodylen"
    Meta.recordBatchStart(b)
    Meta.recordBatchAddLength(b, Int64(nrows))
    Meta.recordBatchAddNodes(b, nodes)
    Meta.recordBatchAddBuffers(b, buffers)
    Meta.recordBatchAddCompression(b, compression)
    return Meta.recordBatchEnd(b), bodylen
end

function makedictionarybatchmsg(sch, columns, id, isdelta, alignment)
    @debug 1 "building dictionary message: id = $id, sch = $sch, isdelta = $isdelta"
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns, alignment)
    Meta.dictionaryBatchStart(b)
    Meta.dictionaryBatchAddId(b, Int64(id))
    Meta.dictionaryBatchAddData(b, recordbatch)
    Meta.dictionaryBatchAddIsDelta(b, isdelta)
    dictionarybatch = Meta.dictionaryBatchEnd(b)
    return makemessage(b, Meta.DictionaryBatch, dictionarybatch, columns, bodylen)
end

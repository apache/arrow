const OBJ_METADATA = IdDict{Any, Dict{String, String}}()

function setmetadata!(x, meta::Dict{String, String})
    OBJ_METADATA[x] = meta
    return
end

getmetadata(x, default=nothing) = get(OBJ_METADATA, x, default)

"""
    Arrow.write(io::IO, tbl)
    Arrow.write(file::String, tbl)

Write any Tables.jl-compatible `tbl` out as arrow formatted data.
Providing an `io::IO` argument will cause the data to be written to it
in the "streaming" format, unless `file=true` keyword argument is passed.
Providing a `file::String` argument will result in the "file" format being written.

Multiple record batches will be written based on the number of
`Tables.partitions(tbl)` that are provided; by default, this is just
one for a given table, but some table sources support automatic
partitioning. Note you can turn multiple table objects into partitions
by doing `Tables.partitioner([tbl1, tbl2, ...])`, but note that
each table must have the exact same `Tables.Schema`.

By default, `Arrow.write` will use multiple threads to write multiple
record batches simultaneously (e.g. if julia is started with `julia -t 8`).

Supported keyword arguments to `Arrow.write` include:
  * `compress::Symbol`: possible values include `:lz4` or `:zstd`; will cause all buffers in each record batch to use the respective compression encoding
  * `dictencode::Bool=false`: whether all columns should use dictionary encoding when being written
  * `dictencodenested::Bool=false`: whether nested data type columns should also dict encode nested arrays/buffers; many other implementations don't support this
  * `denseunions::Bool=true`: whether Julia `Vector{<:Union}` arrays should be written using the dense union layout; passing `false` will result in the sparse union layout
  * `largelists::Bool=false`: causes list column types to be written with Int64 offset arrays; mainly for testing purposes; by default, Int64 offsets will be used only if needed
  * `file::Bool=false`: if a an `io` argument is being written to, passing `file=true` will cause the arrow file format to be written instead of just IPC streaming
"""
function write end

function write(file::String, tbl; largelists::Bool=false, compress::Union{Nothing, Symbol}=nothing, denseunions::Bool=true, dictencode::Bool=false, dictencodenested::Bool=false)
    open(file, "w") do io
        write(io, tbl, true, largelists, compress, denseunions, dictencode, dictencodenested)
    end
    return file
end

function write(io::IO, tbl; largelists::Bool=false, compress::Union{Nothing, Symbol}=nothing, denseunions::Bool=true, dictencode::Bool=false, dictencodenested::Bool=false, file::Bool=false)
    return write(io, tbl, file, largelists, compress, denseunions, dictencode, dictencodenested)
end

@static if VERSION >= v"1.3"
    const Cond = Threads.Condition
else
    const Cond = Condition
end

struct OrderedChannel{T}
    chan::Channel{T}
    cond::Cond
    i::Ref{Int}
end

OrderedChannel{T}(sz) where {T} = OrderedChannel{T}(Channel{T}(sz), Threads.Condition(), Ref(1))
Base.iterate(ch::OrderedChannel, st...) = iterate(ch.chan, st...)

macro lock(obj, expr)
    esc(quote
    @static if VERSION >= v"1.3"
        lock($obj)
    end
        try
            $expr
        finally
            @static if VERSION >= v"1.3"
                unlock($obj)
            end
        end
    end)
end

function Base.put!(ch::OrderedChannel{T}, x::T, i::Integer, incr::Bool=false) where {T}
    @lock ch.cond begin
        while ch.i[] < i
            wait(ch.cond)
        end
        put!(ch.chan, x)
        if incr
            ch.i[] += 1
        end
        notify(ch.cond)
    end
    return
end

function Base.close(ch::OrderedChannel)
    @lock ch.cond begin
        while Base.n_waiters(ch.cond) > 0
            wait(ch.cond)
        end
        close(ch.chan)
    end
    return
end

function write(io, source, writetofile, largelists, compress, denseunions, dictencode, dictencodenested)
    if writetofile
        @debug 1 "starting write of arrow formatted file"
        Base.write(io, "ARROW1\0\0")
    end
    msgs = OrderedChannel{Message}(Inf)
    # build messages
    sch = Ref{Tables.Schema}()
    firstcols = Ref{Any}()
    dictencodingvalues = Dict{Int, Set}()
    blocks = (Block[], Block[])
    # start message writing from channel
@static if VERSION >= v"1.3-DEV"
    tsk = Threads.@spawn for msg in msgs
        Base.write(io, msg, blocks, sch)
    end
else
    tsk = @async for msg in msgs
        Base.write(io, msg, blocks, sch)
    end
end
    @sync for (i, tbl) in enumerate(Tables.partitions(source))
        @debug 1 "processing table partition i = $i"
        if i == 1
            cols = toarrowtable(tbl, largelists, compress, denseunions, dictencode, dictencodenested)
            sch[] = Tables.schema(cols)
            firstcols[] = cols
            put!(msgs, makeschemamsg(sch[], cols), i)
            if !isempty(cols.dictencodings)
                for de in cols.dictencodings
                    dictencodingvalues[de.id] = Set(z for z in de.data)
                    dictsch = Tables.Schema((:col,), (eltype(de.data),))
                    put!(msgs, makedictionarybatchmsg(dictsch, (col=de.data,), de.id, false), i)
                end
            end
            put!(msgs, makerecordbatchmsg(sch[], cols), i, true)
        else
@static if VERSION >= v"1.3-DEV"
            Threads.@spawn begin
                try
                    cols = toarrowtable(tbl, largelists, compress, denseunions, dictencode, dictencodenested)
                    if !isempty(cols.dictencodings)
                        for de in cols.dictencodings
                            dictsch = Tables.Schema((:col,), (eltype(de.data),))
                            existing = dictencodingvalues[de.id]
                            # get new de.data we haven't seen before for delta update
                            vals = setdiff(de.data, existing)
                            if !isempty(vals)
                                put!(msgs, makedictionarybatchmsg(dictsch, (col=vals,), de.id, true), i)
                                # add new de.data to existing set for future diffs
                                union!(existing, vals)
                            end
                        end
                    end
                    put!(msgs, makerecordbatchmsg(sch[], cols), i, true)
                catch e
                    showerror(stdout, e, catch_backtrace())
                    rethrow(e)
                end
            end
else
            @async begin
                try
                    cols = toarrowtable(tbl, largelists, compress, denseunions, dictencode, dictencodenested)
                    if !isempty(cols.dictencodings)
                        for de in cols.dictencodings
                            dictsch = Tables.Schema((:col,), (eltype(de.data),))
                            existing = dictencodingvalues[de.id]
                            # get new de.data we haven't seen before for delta update
                            vals = setdiff(de.data, existing)
                            put!(msgs, makedictionarybatchmsg(dictsch, (col=vals,), de.id, true), i)
                            # add new de.data to existing set for future diffs
                            union!(existing, vals)
                        end
                    end
                    put!(msgs, makerecordbatchmsg(sch[], cols), i, true)
                catch e
                    showerror(stdout, e, catch_backtrace())
                    rethrow(e)
                end
            end
end
        end
    end
    close(msgs)
    wait(tsk)
    # write empty message
    if !writetofile
        Base.write(io, Message(UInt8[], nothing, 0, true, false), blocks, sch)
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
    dictencodings::Vector{DictEncoding}
end

function toarrowtable(x, largelists, compress, denseunions, dictencode, dictencodenested)
    @debug 1 "converting input table to arrow formatted columns"
    cols = Tables.columns(x)
    meta = getmetadata(cols)
    sch = Tables.schema(cols)
    types = collect(sch.types)
    N = length(types)
    newcols = Vector{Any}(undef, N)
    newtypes = Vector{Type}(undef, N)
    dictencodings = DictEncoding[]
    Tables.eachcolumn(sch, cols) do col, i, nm
        newcol = toarrowvector(col, dictencodings; compression=compress, largelists=largelists, denseunions=denseunions, dictencode=dictencode, dictencodenested=dictencodenested)
        newtypes[i] = eltype(newcol)
        newcols[i] = newcol
    end
    # assign dict encoding ids
    for (i, de) in enumerate(dictencodings)
        de.id = i - 1
    end
    return ToArrowTable(Tables.Schema(sch.names, newtypes), newcols, meta, dictencodings)
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

function Base.write(io::IO, msg::Message, blocks, sch)
    metalen = padding(length(msg.msgflatbuf))
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
    n += writezeros(io, paddinglength(n))
    # message body
    if msg.columns !== nothing
        # write out buffers
        for col in Tables.Columns(msg.columns)
            writebuffer(io, col)
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

function makerecordbatchmsg(sch::Tables.Schema{names, types}, columns) where {names, types}
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns)
    return makemessage(b, Meta.RecordBatch, recordbatch, columns, bodylen)
end

function makerecordbatch(b, sch::Tables.Schema{names, types}, columns) where {names, types}
    nrows = Tables.rowcount(columns)
    
    compress = nothing
    fieldnodes = FieldNode[]
    fieldbuffers = Buffer[]
    bufferoffset = 0
    for col in Tables.Columns(columns)
        if col isa Compressed
            compress = compressiontype(col)
        end
        bufferoffset = makenodesbuffers!(col, fieldnodes, fieldbuffers, bufferoffset)
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
        bodylen += padding(buf.length)
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

function makedictionarybatchmsg(sch, columns, id, isdelta)
    @debug 1 "building dictionary message: id = $id, sch = $sch, isdelta = $isdelta"
    b = FlatBuffers.Builder(1024)
    recordbatch, bodylen = makerecordbatch(b, sch, columns)
    Meta.dictionaryBatchStart(b)
    Meta.dictionaryBatchAddId(b, Int64(id))
    Meta.dictionaryBatchAddData(b, recordbatch)
    Meta.dictionaryBatchAddIsDelta(b, isdelta)
    dictionarybatch = Meta.dictionaryBatchEnd(b)
    return makemessage(b, Meta.DictionaryBatch, dictionarybatch, columns, bodylen)
end

function makenodesbuffers!(col::MissingVector, fieldnodes, fieldbuffers, bufferoffset)
    push!(fieldnodes, FieldNode(length(col), length(col)))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    return bufferoffset
end

function writebuffer(io, col::MissingVector)
    return
end

function makenodesbuffers!(col::Primitive{T, S}, fieldnodes, fieldbuffers, bufferoffset) where {T, S}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
    # adjust buffer offset, make primitive array buffer
    bufferoffset += blen
    blen = len * sizeof(T)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
    return bufferoffset + padding(blen)
end

function writebitmap(io, col::ArrowVector)
    v = col.validity
    @debug 1 "writing validity bitmap: nc = $(v.nc), n = $(bitpackedbytes(v.ℓ))"
    v.nc == 0 && return 0
    Base.write(io, view(v.bytes, v.pos:(v.pos + bitpackedbytes(v.ℓ) - 1)))
end

function writebuffer(io, col::Primitive{T, S}) where {T, S}
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col)
    n = writearray(io, S, col.data)
    @debug 1 "writing array: col = $(typeof(col.data)), n = $n, padded = $(padding(n))"
    writezeros(io, paddinglength(n))
    return
end

function makenodesbuffers!(col::Union{Map{T, O, A}, List{T, O, A}}, fieldnodes, fieldbuffers, bufferoffset) where {T, O, A}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
    # adjust buffer offset, make array buffer
    bufferoffset += blen
    blen = sizeof(O) * (len + 1)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
    bufferoffset += padding(blen)
    if eltype(A) == UInt8
        blen = length(col.data)
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
        bufferoffset += padding(blen)
    else
        bufferoffset = makenodesbuffers!(col.data, fieldnodes, fieldbuffers, bufferoffset)
    end
    return bufferoffset
end

function writebuffer(io, col::Union{Map{T, O, A}, List{T, O, A}}) where {T, O, A}
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col)
    # write offsets
    n = writearray(io, O, col.offsets.offsets)
    @debug 1 "writing array: col = $(typeof(col.offsets.offsets)), n = $n, padded = $(padding(n))"
    writezeros(io, paddinglength(n))
    # write values array
    if eltype(A) == UInt8
        n = writearray(io, UInt8, col.data)
        @debug 1 "writing array: col = $(typeof(col.data)), n = $n, padded = $(padding(n))"
        writezeros(io, paddinglength(n))
    else
        writebuffer(io, col.data)
    end
    return
end

function makenodesbuffers!(col::FixedSizeList{T, A}, fieldnodes, fieldbuffers, bufferoffset) where {T, A}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
    bufferoffset += blen
    if eltype(A) === UInt8
        blen = getN(Base.nonmissingtype(T)) * len
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
        bufferoffset += padding(blen)
    else
        bufferoffset = makenodesbuffers!(col.data, fieldnodes, fieldbuffers, bufferoffset)
    end
    return bufferoffset
end

function writebuffer(io, col::FixedSizeList{T, A}) where {T, A}
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col)
    # write values array
    if eltype(A) === UInt8
        n = writearray(io, UInt8, col.data)
        @debug 1 "writing array: col = $(typeof(col.data)), n = $n, padded = $(padding(n))"
        writezeros(io, paddinglength(n))
    else
        writebuffer(io, col.data)
    end
    return
end

function makenodesbuffers!(col::Struct{T}, fieldnodes, fieldbuffers, bufferoffset) where {T}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
    bufferoffset += blen
    for child in col.data
        bufferoffset = makenodesbuffers!(child, fieldnodes, fieldbuffers, bufferoffset)
    end
    return bufferoffset
end

function writebuffer(io, col::Struct)
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col)
    # write values arrays
    for child in col.data
        writebuffer(io, child)
    end
    return
end

function makenodesbuffers!(col::Union{DenseUnion, SparseUnion}, fieldnodes, fieldbuffers, bufferoffset)
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # typeIds buffer
    push!(fieldbuffers, Buffer(bufferoffset, len))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
    bufferoffset += padding(len)
    if col isa DenseUnion
        # offsets buffer
        blen = sizeof(Int32) * len
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
        bufferoffset += padding(blen)
    end
    for child in col.data
        bufferoffset = makenodesbuffers!(child, fieldnodes, fieldbuffers, bufferoffset)
    end
    return bufferoffset
end

function writebuffer(io, col::Union{DenseUnion, SparseUnion})
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    # typeIds buffer
    n = writearray(io, UInt8, col.typeIds)
    @debug 1 "writing array: col = $(typeof(col.typeIds)), n = $n, padded = $(padding(n))"
    writezeros(io, paddinglength(n))
    if col isa DenseUnion
        n = writearray(io, Int32, col.offsets)
        @debug 1 "writing array: col = $(typeof(col.offsets)), n = $n, padded = $(padding(n))"
        writezeros(io, paddinglength(n))
    end
    for child in col.data
        writebuffer(io, child)
    end
    return
end

function makenodesbuffers!(col::DictEncoded{T, S}, fieldnodes, fieldbuffers, bufferoffset) where {T, S}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
    bufferoffset += blen
    # indices
    blen = sizeof(S) * len
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
    bufferoffset += padding(blen)
    return bufferoffset
end

function writebuffer(io, col::DictEncoded)
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col)
    # write indices
    n = writearray(io, col.indices)
    @debug 1 "writing array: col = $(typeof(col.indices)), n = $n, padded = $(padding(n))"
    writezeros(io, paddinglength(n))
    return
end

function makenodesbuffers!(col::Compressed, fieldnodes, fieldbuffers, bufferoffset)
    push!(fieldnodes, FieldNode(col.len, col.nullcount))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    for buffer in col.buffers
        blen = length(buffer.data) == 0 ? 0 : 8 + length(buffer.data)
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length))"
        bufferoffset += padding(blen)
    end
    for child in col.children
        bufferoffset = makenodesbuffers!(child, fieldnodes, fieldbuffers, bufferoffset)
    end
    return bufferoffset
end

function writearray(io, b::CompressedBuffer)
    if length(b.data) > 0
        n = Base.write(io, b.uncompressedlength)
        @debug 1 "writing compressed buffer: uncompressedlength = $(b.uncompressedlength), n = $(length(b.data))"
        @debug 2 b.data
        return n + Base.write(io, b.data)
    end
    return 0
end

function writebuffer(io, col::Compressed)
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    for buffer in col.buffers
        n = writearray(io, buffer)
        writezeros(io, paddinglength(n))
    end
    for child in col.children
        writebuffer(io, child)
    end
    return
end

ENV["PYTHON"] = "python3"
import PyCall
pa = PyCall.pyimport("pyarrow")
include(joinpath(dirname(pathof(Arrow)), "../test/testtables.jl"))

for (nm, t, writekw, readkw, extratests) in testtables
    nm == "unions" && continue
    println("pyarrow roundtrip: $nm")
    io = IOBuffer()
    Arrow.write(io, t; writekw...)
    seekstart(io)
    buf = PyCall.pybytes(take!(io))
    reader = pa.ipc.open_stream(buf)
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, reader.schema)
    for batch in reader
        writer.write_batch(batch)
    end
    writer.close()
    buf = sink.getvalue()
    jbytes = copy(reinterpret(UInt8, buf))
    tt = Arrow.Table(jbytes)
    println("pyarrow roundtrip w/ compression: $nm")
    io = IOBuffer()
    Arrow.write(io, t; compress=((:lz4, :zstd)[rand(1:2)]), writekw...)
    seekstart(io)
    buf = PyCall.pybytes(take!(io))
    reader = pa.ipc.open_stream(buf)
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, reader.schema)
    for batch in reader
        writer.write_batch(batch)
    end
    writer.close()
    buf = sink.getvalue()
    jbytes = copy(reinterpret(UInt8, buf))
    tt = Arrow.Table(jbytes)
end

f1 = pa.field("f1", pa.float64(), true)
f2 = pa.field("f2", pa.int64(), false)
fu = pa.field("col1", pa.union([f1, f2], "dense"))
sch = pa.schema([fu])

xs = pa.array([2.0, 4.0, PyCall.pynothing[]], type=pa.float64())
ys = pa.array([1, 3], type=pa.int64())
types = pa.array([0, 1, 0, 1, 1], type=pa.int8())
offsets = pa.array([0, 0, 1, 1, 2], type=pa.int32())
union_arr = pa.UnionArray.from_dense(types, offsets, [xs, ys])
data = [union_arr]
batch = pa.record_batch(data, names=["col1"])
sink = pa.BufferOutputStream()
writer = pa.ipc.new_stream(sink, batch.schema)
writer.write_batch(batch)
writer.close()
buf = sink.getvalue()
jbytes = copy(reinterpret(UInt8, buf))
tt = Arrow.Table(jbytes)
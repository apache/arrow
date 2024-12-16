import pyarrow as pa
import pyarrow.parquet as pq

pa.set_cpu_count(1)
pa.set_io_thread_count(1)

column0_array = pa.array([0, 1], type=pa.int32())
column1_array = pa.array([10, 11], type=pa.int32())
table = pa.Table.from_arrays(
    [column0_array, column1_array], names=["column0", "column1"]
)
print(table)

# table2 = table.cast(pa.schema(list(table.schema) + [('some_int', pa.int32())]))
# print(table2)

pq.write_table(table, "/tmp/example.parquet")

table_read = pq.read_table(
    "/tmp/example.parquet", schema=pa.schema(list(reversed(table.schema)))
)
print(table_read)


nested_column_array = pa.array(
    [{"sub_column0": 0, "sub_column1": 10}, {"sub_column0": 1, "sub_column1": 11}],
    type=pa.struct(
        [pa.field("sub_column0", pa.int32()), pa.field("sub_column1", pa.int32())]
    ),
)
struct_table = pa.Table.from_arrays([nested_column_array], names=["nested_column"])
print(struct_table)

pq.write_table(struct_table, "/tmp/example2.parquet")

table_read = pq.read_table(
    "/tmp/example2.parquet",
    schema=pa.schema(
        [
            (
                struct_table.schema[0].name,
                pa.struct(
                    [
                        struct_table.schema[0].type.field(1),
                        struct_table.schema[0].type.field(0),
                    ],
                ),
            )
        ]
    ),
)
print(table_read)

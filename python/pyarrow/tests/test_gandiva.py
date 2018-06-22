
import pyarrow as pa
import pyarrow.gandiva as gandiva

def test_tree_exp_builder():
    builder = gandiva.TreeExprBuilder()
    n1 = builder.make_literal(True)
    field_a = pa.field('a', pa.int32())
    field_b = pa.field('b', pa.int32())

    schema = pa.schema([field_a, field_b])

    field_result = pa.field('res', pa.int32())

    node_a = builder.make_field(field_a)
    node_b = builder.make_field(field_b)

    condition = builder.make_function(b"greater_than", [node_a, node_b], pa.bool_())
    if_node = builder.make_if(condition, node_a, node_b, pa.int32())

    expr = builder.make_expression(if_node, field_result)

    projector = gandiva.make_projector(schema, [expr], pa.default_memory_pool())

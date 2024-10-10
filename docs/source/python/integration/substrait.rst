.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

=========
Substrait
=========

The ``arrow-substrait`` module implements support for the Substrait_ format,
enabling conversion to and from Arrow objects.

The ``arrow-dataset`` module can execute Substrait_ plans via the
:doc:`Acero <../cpp/streaming_execution>` query engine.

.. contents::

Working with Schemas
====================

Arrow schemas can be encoded and decoded using the :meth:`pyarrow.substrait.serialize_schema` and
:meth:`pyarrow.substrait.deserialize_schema` functions.

.. code-block:: python

    import pyarrow as pa
    import pyarrow.substrait as pa_substrait

    arrow_schema = pa.schema([
        pa.field("x", pa.int32()),
        pa.field("y", pa.string())
    ])
    substrait_schema = pa_substrait.serialize_schema(arrow_schema)

The schema marshalled as a Substrait ``NamedStruct`` is directly
available as ``substrait_schema.schema``::

    >>> print(substrait_schema.schema)
    b'\n\x01x\n\x01y\x12\x0c\n\x04*\x02\x10\x01\n\x04b\x02\x10\x01'

In case arrow custom types were used, the schema will require
extensions for those types to be actually usable, for this reason
the schema is also available as an `Extended Expression`_ including
all the extensions types::

    >>> print(substrait_schema.expression)
    b'"\x14\n\x01x\n\x01y\x12\x0c\n\x04*\x02\x10\x01\n\x04b\x02\x10\x01:\x19\x10,*\x15Acero 17.0.0'

If ``Substrait Python`` is installed, the schema can also be converted to
a ``substrait-python`` object::

    >>> print(substrait_schema.to_pysubstrait())
    version {
        minor_number: 44
        producer: "Acero 17.0.0"
    }
    base_schema {
        names: "x"
        names: "y"
        struct {
            types {
                i32 {
                    nullability: NULLABILITY_NULLABLE
                }
            }
            types {
                string {
                    nullability: NULLABILITY_NULLABLE
                }
            }
        }
    }

Working with Expressions
========================

Arrow compute expressions can be encoded and decoded using the
:meth:`pyarrow.substrait.serialize_expressions` and
:meth:`pyarrow.substrait.deserialize_expressions` functions.

.. code-block:: python

    import pyarrow as pa
    import pyarrow.compute as pa
    import pyarrow.substrait as pa_substrait

    arrow_schema = pa.schema([
        pa.field("x", pa.int32()),
        pa.field("y", pa.int32())
    ])

    substrait_expr = pa_substrait.serialize_expressions(
        exprs=[pc.field("x") + pc.field("y")],
        names=["total"],
        schema=arrow_schema
    )

The result of encoding to substrait an expression will be the
protobuf ``ExtendedExpression`` message data itself::

    >>> print(bytes(substrait_expr))
    b'\nZ\x12Xhttps://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml\x12\x07\x1a\x05\x1a\x03add\x1a>\n5\x1a3\x1a\x04*\x02\x10\x01"\n\x1a\x08\x12\x06\n\x02\x12\x00"\x00"\x0c\x1a\n\x12\x08\n\x04\x12\x02\x08\x01"\x00*\x11\n\x08overflow\x12\x05ERROR\x1a\x05total"\x14\n\x01x\n\x01y\x12\x0c\n\x04*\x02\x10\x01\n\x04*\x02\x10\x01:\x19\x10,*\x15Acero 17.0.0'

So in case a ``Substrait Python`` object is required, the expression
has to be decoded from ``substrait-python`` itself::

    >>> import substrait
    >>> pysubstrait_expr = substrait.proto.ExtendedExpression.FromString(substrait_expr)
    >>> print(pysubstrait_expr)
    version {
      minor_number: 44
      producer: "Acero 17.0.0"
    }
    extension_uris {
      uri: "https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml"
    }
    extensions {
      extension_function {
        name: "add"
      }
    }
    referred_expr {
      expression {
        scalar_function {
          arguments {
            value {
              selection {
                direct_reference {
                  struct_field {
                  }
                }
                root_reference {
                }
              }
            }
          }
          arguments {
            value {
              selection {
                direct_reference {
                  struct_field {
                    field: 1
                  }
                }
                root_reference {
                }
              }
            }
          }
          options {
            name: "overflow"
            preference: "ERROR"
          }
          output_type {
            i32 {
              nullability: NULLABILITY_NULLABLE
            }
          }
        }
      }
      output_names: "total"
    }
    base_schema {
      names: "x"
      names: "y"
      struct {
        types {
          i32 {
            nullability: NULLABILITY_NULLABLE
          }
        }
        types {
          i32 {
            nullability: NULLABILITY_NULLABLE
          }
        }
      }
    }

Executing Queries Using Substrait Extended Expressions
======================================================

Dataset supports executing queries using Substrait's `Extended Expression`_,
the expressions can be passed to the dataset scanner in the form of
:class:`pyarrow.substrait.BoundExpressions`

.. code-block:: python

    import pyarrow.dataset as ds
    import pyarrow.substrait as pa_substrait

    # Use substrait-python to create the queries
    from substrait import proto

    dataset = ds.dataset("./data/index-0.parquet")
    substrait_schema = pa_substrait.serialize_schema(dataset.schema).to_pysubstrait()

    # SELECT project_name FROM dataset WHERE project_name = 'pyarrow'

    projection = proto.ExtendedExpression(referred_expr=[
        {"expression": {"selection": {"direct_reference": {"struct_field": {"field": 0}}}},
        "output_names": ["project_name"]}
    ])
    projection.MergeFrom(substrait_schema)

    filtering = proto.ExtendedExpression(
            extension_uris=[{"extension_uri_anchor": 99, "uri": "/functions_comparison.yaml"}],
            extensions=[{"extension_function": {"extension_uri_reference": 99, "function_anchor": 199, "name": "equal:any1_any1"}}],
            referred_expr=[
                {"expression": {"scalar_function": {"function_reference": 199, "arguments": [
                    {"value": {"selection": {"direct_reference": {"struct_field": {"field": 0}}}}},
                    {"value": {"literal": {"string": "pyarrow"}}}
                ], "output_type": {"bool": {"nullability": False}}}}}
            ]
    )
    filtering.MergeFrom(substrait_schema)

    results = dataset.scanner(
        columns=pa.substrait.BoundExpressions.from_substrait(projection),
        filter=pa.substrait.BoundExpressions.from_substrait(filtering)
    ).head(5)


.. code-block:: text

    project_name
    0      pyarrow
    1      pyarrow
    2      pyarrow
    3      pyarrow
    4      pyarrow


.. _`Substrait`: https://substrait.io/
.. _`Substrait Python`: https://github.com/substrait-io/substrait-python
.. _`Acero`: https://arrow.apache.org/docs/cpp/streaming_execution.html
.. _`Extended Expression`: https://github.com/substrait-io/substrait/blob/main/site/docs/expressions/extended_expression.md

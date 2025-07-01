from pyarrow.includes.libarrow_dataset cimport CField

cdef CField.CMergeOptions _parse_field_merge_options(str promote_options, bint allow_none) except *

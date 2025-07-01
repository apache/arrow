from pyarrow.includes.libarrow_dataset cimport CField

cdef CField.CMergeOptions _parse_field_merge_options(str promote_options, bint allow_none) except *:
    if promote_options == "permissive":
        return CField.CMergeOptions.Permissive()
    elif promote_options == "default" or (allow_none and promote_options == "none"):
        return CField.CMergeOptions.Defaults()
    else:
        raise ValueError(f"Invalid promote_options: {promote_options}")

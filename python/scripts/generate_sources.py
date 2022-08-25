import pyarrow as pa
from pyarrow import _rstutils as arrowdoc
from pyarrow.vendored import docscrape
import textwrap


from pyarrow._compute import (  # noqa
    Function,
    FunctionOptions,
    FunctionRegistry,
    HashAggregateFunction,
    HashAggregateKernel,
    Kernel,
    ScalarAggregateFunction,
    ScalarAggregateKernel,
    ScalarFunction,
    ScalarKernel,
    VectorFunction,
    VectorKernel,
    # Option classes
    ArraySortOptions,
    AssumeTimezoneOptions,
    CastOptions,
    CountOptions,
    DayOfWeekOptions,
    DictionaryEncodeOptions,
    ElementWiseAggregateOptions,
    ExtractRegexOptions,
    FilterOptions,
    IndexOptions,
    JoinOptions,
    MakeStructOptions,
    MapLookupOptions,
    MatchSubstringOptions,
    ModeOptions,
    NullOptions,
    PadOptions,
    PartitionNthOptions,
    QuantileOptions,
    RandomOptions,
    ReplaceSliceOptions,
    ReplaceSubstringOptions,
    RoundOptions,
    RoundTemporalOptions,
    RoundToMultipleOptions,
    ScalarAggregateOptions,
    SelectKOptions,
    SetLookupOptions,
    SliceOptions,
    SortOptions,
    SplitOptions,
    SplitPatternOptions,
    StrftimeOptions,
    StrptimeOptions,
    StructFieldOptions,
    TakeOptions,
    TDigestOptions,
    TrimOptions,
    Utf8NormalizeOptions,
    VarianceOptions,
    WeekOptions,
    RankOptions,
    CumulativeSumOptions,
    # Functions
    call_function,
    function_registry,
    get_function,
    list_functions,
    _group_by,
    # Expressions
    Expression,
)
import sys
import os
import inspect
import warnings
from inspect import Parameter
from collections import namedtuple

# Avoid clashes with Python keywords
function_name_rewrites = {'and': 'and_', 'or': 'or_'}

def _get_arg_names(func):
    return func._doc.arg_names

_OptionsClassDoc = namedtuple('_OptionsClassDoc', ('params',))

def _scrape_options_class_doc(options_class):
    if not options_class.__doc__:
        return None
    doc = docscrape.NumpyDocString(options_class.__doc__)
    return _OptionsClassDoc(doc['Parameters'])


def _get_options_class(func):
    class_name = func._doc.options_class
    if not class_name:
        return None
    try:
        return globals()[class_name]
    except KeyError:
        warnings.warn("Python binding for {} not exposed"
                      .format(class_name), RuntimeWarning)
        return None


def generate_compute_function_doc(exposed_name, func, options_class,
    custom_overrides = None):
    """ Create the documentation for functions defined in Arrow C++.

    Args:
        exposed_name: The name of the function.
        func: The cython function that connects to Arrow C++.
        options_class: The class object for the options.
        custom_overrides: Custom doc overrides as processed by pyarrow.docutils
            from the `python/docs/additions/compute` directory.
    Returns:
        str: The docstring to set the documentation for the pyarrow.compute
            function.
    """

    cpp_doc = func._doc

    if not custom_overrides:
        custom_overrides = {}

    docstring = ""

    # 1. One-line summary
    summary = cpp_doc.summary
    if not summary:
        arg_str = "arguments" if func.arity > 1 else "argument"
        summary = ("Call compute function {!r} with the given {}"
                   .format(func.name, arg_str))

    docstring += f"{summary}.\n\n"

    # 2.a. Multi-line description
    if 'description' in custom_overrides and custom_overrides['description']:
        docstring += custom_overrides['description'] + "\n\n"
    elif cpp_doc.description:
        docstring += cpp_doc.description + "\n\n"

    # 2.b. If "details" are provided in the override, add them to
    # the description block
    if 'details' in custom_overrides and custom_overrides['details']:
        docstring += "\n\n".join(custom_overrides['details']) + "\n\n"


    # 2.c. Note about the C++ function
    docstring += f"This wraps the \"{func.name}\" compute function in "\
        "the Arrow C++ library.\n\n"



    # 3. Parameter description
    docstring += "Parameters\n----------\n"

    if custom_overrides and 'parameters' in custom_overrides:
        custom_params = custom_overrides['parameters']
    else:
        custom_params = {}

    # 3a. Compute function parameters
    arg_names = _get_arg_names(func)
    for arg_name in arg_names:
        if arg_name in custom_params:
            custom_arg = custom_params[arg_name]
        else:
            custom_arg = {}

        if 'classifier' in custom_arg:
            arg_type = custom_arg['classifier']
        elif func.kind in ('vector', 'scalar_aggregate'):
            arg_type = 'Array-like'
        else:
            arg_type = 'Array-like or scalar-like'
        docstring += f"{arg_name} : {arg_type}\n"

        if 'definition' in custom_arg:
            docstring += f"    {custom_arg['definition']}\n"
        else:
            docstring += "    Argument to compute function.\n"

    # 3b. Compute function option values
    if options_class is not None:
        options_class_doc = _scrape_options_class_doc(options_class)
        if options_class_doc:
            for p in options_class_doc.params:
                if custom_overrides and 'parameters' in custom_overrides and \
                    p.name in custom_overrides['parameters']:
                    custom_args = custom_overrides['parameters'][p.name]
                else:
                    custom_args = {}

                if 'type' in custom_args:
                    docstring += f"{p.name} : {custom_args['type']}\n"
                else:
                    docstring += f"{p.name} : {p.type}\n"

                if 'definition' in custom_args:
                    docstring += f"    {custom_args['definition']}"
                else:
                    for s in p.desc:
                        docstring += f"    {s}\n"
        else:
            warnings.warn(f"Options class {options_class.__name__} "
                          f"does not have a docstring", RuntimeWarning)
            options_sig = inspect.signature(options_class)
            for p in options_sig.parameters.values():
                docstring += textwrap.dedent("""\
                {0} : optional
                    Parameter for {1} constructor. Either `options`
                    or `{0}` can be passed, but not both at the same time.
                """.format(p.name, options_class.__name__))
        docstring += textwrap.dedent(f"""\
            options : pyarrow.compute.{options_class.__name__}, optional
                Alternative way of passing options.
            """)

    docstring += textwrap.dedent("""\
        memory_pool : pyarrow.MemoryPool, optional
            If not passed, will allocate memory from the default memory pool.
        \n""")

    # 4. Compute return type
    if 'return_type' in custom_overrides:
        return_string = "Returns\n-------\n"
        for retval, retdesc in custom_overrides['return_type']:
            return_string += f"{retval}\n    {retdesc}\n"
        docstring += return_string

    # 6. Custom addition (e.g. examples)
    if 'examples' in custom_overrides:
        docstring += "\n\nExamples\n--------\n" + \
            "\n\n".join(custom_overrides['examples'])

    return(docstring)


def generate_function_def(name, cpp_name, func, arity, custom_overrides=None):
    """ Create the function definition for the pyarrow.compute function.

    Args:
        name: The name of the function.
        cpp_name: The name of the Arrow C++ function, which might differ
            slightly from the name parameter.
        func: The cython function that connects to Arrow C++.
        arity: The number of non-option arguments to the function.
        custom_overrides: Custom doc overrides as processed by pyarrow.docutils
            from the `python/docs/additions/compute` directory, which are
            passed on to the `generate_compute_function_doc` function.
    Returns:
        str: The generated function definition, in string format.

    """

    # prepare args
    all_params = []
    # required options
    options_required = func._doc.options_required

    argnames = _get_arg_names(func)
    if argnames and argnames[-1].startswith('*'):
        var_argname = argnames.pop().lstrip('*')
    else:
        var_argname = None

    for argname in argnames:
        all_params.append(Parameter(argname, Parameter.POSITIONAL_ONLY))

    if var_argname:
        all_params.append(Parameter(var_argname, Parameter.VAR_POSITIONAL))
        argnames.append('*' + var_argname)

    options_class = _get_options_class(func)
    options_class_name = 'pyarrow._compute.' + func._doc.options_class
    option_params = []
    if options_class is not None:
        options_sig = inspect.signature(options_class)
        for paramname, paramdef in options_sig.parameters.items():
            assert paramdef.kind in (Parameter.POSITIONAL_OR_KEYWORD,
                              Parameter.KEYWORD_ONLY)
            if var_argname:
                # Cannot have a positional argument after a *args
                paramdef = paramdef.replace(kind=Parameter.KEYWORD_ONLY)
            if paramdef.default == inspect._empty:
                paramdef = paramdef.replace(default = None)
            all_params.append(paramdef)
            option_params.append(paramname)
        all_params.append(Parameter("options", Parameter.KEYWORD_ONLY,
                                default=None))
    all_params.append(Parameter("memory_pool", Parameter.KEYWORD_ONLY,
                            default=None))

    
    if len(argnames) == 1:
        argstring = f'[{argnames[0]}]'
    else:
        argstring = f"[{', '.join(argnames)}]"

    full_signature_obj = inspect.Signature(all_params)

    # because we are indeed committing the output to a file that will be linted
    # we need to wrap lines appropriately, and wrapping/linting requirements
    # differ based on the type of code we're dealing with. 
    # Perhaps in review (or in the future) a better way to deal with this issue
    # will emerge, but in the meantime it means doing a piece-by-piece
    # analysis. 

    # --- handle the function name and parameter signature
    function_text = f"def {name}"
    
    # now add the parameters, which need to be aligned to the parenthesis
    # to pass the linter (E128).
    # We also need to strip out the '/' that indicates the end of the ordered 
    # parameters because that is not compatiable with python 3.7
    full_signature = f"{full_signature_obj}:".replace('/, ', '')
    maxchar = 79 - len(function_text)
    function_text += textwrap.fill(
        full_signature, 
        width = maxchar, 
        subsequent_indent = " "*(len(function_text)+1),
    )

    # --- handle the documentation for the function
    
    # get the full documentation, unindented and not line-limited
    funcdoc = generate_compute_function_doc(name, func, options_class,
        custom_overrides)
    # indent it based on code requirements and wrap based on the linter
    funcdoc = "\n".join([textwrap.fill(
        line, 
        width = 77, 
        initial_indent = " "*4, 
        subsequent_indent = " "*8,
        replace_whitespace = False, 
        break_long_words = False, 
        drop_whitespace = True, 
        break_on_hyphens = False,
    ) for line in funcdoc.split("\n")]).strip()

    function_text += f'\n    """{funcdoc}\n    """\n\n'

    if options_class:
        # here we need to create the kwargs param substring for the
        # _handle_options function
        if arity is Ellipsis or len(argnames) <= arity:
            option_args = ''
        else:
            option_args = ",\n        ".join(argnames[arity:])
            option_args += ",\n        "

        keyword_options = ",\n        ".join(
            opt + '=' + opt for opt in option_params
        ) 
        function_text += f'''    _computed_options = _handle_options(
        '{name}',
        {options_class_name},
        options,
        ({option_args}),
        {keyword_options}\n    )\n'''


    function_text += f"    func = pyarrow._compute.get_function('{cpp_name}')\n"
    if len(argnames) > 2 or (len(argnames) and not var_argname):
        # handle the Expression construct
        function_text += f"\n    if isinstance({argnames[0]}, Expression):\n"\
            f"        return Expression._call(\n"\
            f"            '{name}',\n"\
            f"            {argstring}"
        if options_class:
            function_text += ",\n            _computed_options"
        function_text += "\n        )\n\n"

    if options_class:
        function_text += f"""    return(
        func.call({argstring}, _computed_options, memory_pool)
    )"""
    else:

        function_text += f"""    return(
        func.call({argstring}, memory_pool=memory_pool)
    )"""

    return(function_text)

def write_compute_file(output_path):
    """
    Write the full set of generated functions to the output_path.

    Note that, in practice, while this generates core functions for all of the
    Arrow C++ compute functions, the compute functions may be overriden in the
    `pyarrow/compute.py` file.

    Args:
        output_path: The full path to the file to write the compute function.
    """
    g = globals()
    reg = function_registry()

    doc_overrides = arrowdoc.parse_directory(
        os.path.dirname(arrowdoc.__file__) + "/../docs/additions/compute"
    )

    function_defs = []
    for cpp_name in reg.list_functions():
        name = function_name_rewrites.get(cpp_name, cpp_name)

        func = reg.get_function(cpp_name)

        if func.kind == "hash_aggregate":
            # Hash aggregate functions are not callable,
            # so let's not expose them at module level.
            continue

        function_def = generate_function_def(
            name,
            cpp_name,
            func,
            func.arity,
            doc_overrides.get(name, None)
        )
        function_defs.append(function_def)

    with open(output_path, 'w') as fh:
        fh.write(textwrap.dedent(f"""\
    # File GENERATED by scripts/generate_sources.py - DO NOT EDIT.
    #
    # Licensed to the Apache Software Foundation (ASF) under one
    # or more contributor license agreements.  See the NOTICE file
    # distributed with this work for additional information
    # regarding copyright ownership.  The ASF licenses this file
    # to you under the Apache License, Version 2.0 (the
    # "License"); you may not use this file except in compliance
    # with the License.  You may obtain a copy of the License at
    #
    #   http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing,
    # software distributed under the License is distributed on an
    # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    # KIND, either express or implied.  See the License for the
    # specific language governing permissions and limitations
    # under the License.

    import pyarrow
    import pyarrow._compute
    from pyarrow._compute import Expression


    def _handle_options(name, options_class, options, args, **kwargs):
        if options is not None:
            if isinstance(options, dict):
                return options_class(**options)
            elif isinstance(options, options_class):
                return options
            raise TypeError(
                "Function {{!r}} expected a {{}} parameter, got {{}}"
                .format(name, options_class, type(options)))

        if args or kwargs:
            # Note: This check is no longer permissable
            # Generating function code with real signatures means that
            # All of the keyword arguments have default values, and so
            # this would always be true. As the default for the options object
            # is always false, the options object takes precedence if provided.
            #
            # if options is not None:
            #    raise TypeError(
            #        "Function {{!r}} called with both an 'options' argument "
            #        "and additional arguments"
            #        .format(name))

            return options_class(*args, **kwargs)

        return None
        \n\n"""))
        fh.write("\n\n\n".join(function_defs) + "\n")
    #print("\n\n".join(function_defs))

if __name__ == "__main__":

    write_compute_file(
        os.path.dirname(__file__) + '/../pyarrow/_compute_generated.py'
    )


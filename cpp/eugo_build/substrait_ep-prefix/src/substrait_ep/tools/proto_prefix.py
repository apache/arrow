#!/usr/bin/python3
# SPDX-License-Identifier: Apache-2.0

"""
Copies the .proto files to a new directory while changing their package prefix
and rewriting their option statements.

This allows a single executable to use different versions of a protobuf package
simultaneously. Any attempt to load different versions in the same protobuf
namespace within a single executable, even if this is done by entirely
unrelated transitively-linked libraries outside of your control, will silently
break the official protobuf library implementation. This is due to a shared
global variable that it uses to map message types to their implementation.

If you use a protobuf package within a public library, it is therefore strongly
recommended to namespace said package to your library with a sufficiently unique
path (usually, the name of your library is fine).

Note that the only things Substrait-specific to this script are some default
values for optional arguments.
"""

import re
import pathlib
import sys


def tokenize(data):
    """Tokenizes a string into (cls, match) tuples, where cls is one of
    'ident', 'string', 'number', 'symbol', 'comment', or 'space', and match is
    the matched string. All characters will be made part of a token, so joining
    all matches yields exactly the original string."""
    tokens = dict(
        ident=re.compile(r"[a-zA-Z_][a-zA-Z_0-9.]*"),
        string=re.compile(r'"(?:[^"\\]|\\.)*"'),
        number=re.compile(r"[0-9]+"),
        symbol=re.compile(r"[=;{}\[\]]"),
        comment=re.compile(r"//[^\n]*\n|/\*(?:(?!\*/).)*\*/"),
        space=re.compile(r"\s+"),
    )

    while data:
        longest_match = ""
        longest_cls = ""
        for cls, regex in tokens.items():
            match = regex.match(data)
            if match:
                match = match.group(0)
            else:
                match = ""
            if len(match) > len(longest_match):
                longest_match = match
                longest_cls = cls
        if not longest_match:
            raise ValueError(f'Failed to tokenize near "{data[:30]}"')
        data = data[len(longest_match) :]
        yield longest_cls, longest_match


class Group:
    """A group of tokens, indexed as if semantically-irrelevant tokens
    (whitespace, comments, etc) don't exist."""

    def __init__(self):
        super().__init__()
        self.tokens = []
        self.indices = []

    def append(self, cls, match, significant=None):
        if significant is None:
            significant = cls not in ["comment", "space"]
        if significant:
            self.indices.append(len(self.tokens))
        self.tokens.append([cls, match])

    def __getitem__(self, idx):
        if idx >= len(self.indices):
            return ""
        return self.tokens[self.indices[idx]][1]

    def __setitem__(self, idx, value):
        self.tokens[self.indices[idx]][1] = value

    def __iter__(self):
        for idx in self.indices:
            yield self.tokens[idx][1]

    def __len__(self):
        return len(self.indices)

    def cls(self, idx):
        return self.tokens[self.indices[idx]][0]

    def __str__(self):
        return "".join(map(lambda x: x[1], self.tokens))


def group_tokens(tokens):
    """Groups tokens into "statements," where a statement is defined as
    something starting with an identifier and ending with either a
    semicolon or a {} block. That's probably not accurate for the whole
    protobuf syntax, but good enough to reliably capture package, import, and
    option statements without breaking anything else."""
    token_it = iter(tokens)
    group = Group()
    for cls, match in token_it:
        # Look for the first identifier.
        if cls != "ident":
            group.append(cls, match, False)
            continue

        group.append(cls, match, True)

        # Append tokens to the group until we reach the end of the statement.
        nesting = 0
        for cls2, match2 in token_it:
            group.append(cls2, match2)
            if match2 == "{":
                nesting += 1
            elif match2 == "}":
                nesting -= 1
                if nesting == 0:
                    break
            elif match2 == ";" and nesting == 0:
                break

        # Yield the statement group.
        yield group
        group = Group()

    # Yield the whitespace at the end of the file.
    yield group


def convert_case(string, case):
    """Converts from lowercase to uppercase (UPPER), camelcase (camel), or
    pascalcase (Pascal), or leaves the string as-is (lower)."""
    assert string == string.lower()
    if case == "lower":
        return string
    if case == "UPPER":
        return string.upper()
    if case == "Pascal":
        return "".join(map(str.title, string.split("_")))
    if case == "camel":
        first, *remain = string.split("_")
        return first + "".join(map(str.title, remain))
    raise ValueError(f"unknown case convention {case:r}")


class IgnoreFile(Exception):
    """Thrown by a group converter when the package specified in the proto
    file is not on the source prefix."""


def make_group_converter(prefix_from, prefix_to, **options):
    """Makes a group converter function bound to the given configuration.
    prefix_from and prefix_to should be either .-separated strings or lists
    of lowercase protobuf namespaces representing the namespace prefix
    replacement to be made. Any named arguments are used for generating
    option statements, where the argument name is the option name, and the
    value is either a str, int, or bool representing the value. str options
    may include {<case><sep>} capture groups, where case is either lower,
    UPPER, Pascal, or camel, and sep is any separator. This expands to
    ".extensions" (with the appropriate separator in place of . and with
    extensions written in the appropriate case convention) for
    substrait.extensions, and to the empty string for substrait."""

    def preprocess_prefix(prefix):
        if isinstance(prefix, str):
            prefix = prefix.split(".")
        else:
            prefix = list(prefix)
        if not prefix:
            raise ValueError("prefix cannot be empty")
        for element in prefix:
            if element != element.lower():
                raise ValueError("prefix must be lowercase")
        return prefix

    prefix_from = preprocess_prefix(prefix_from)
    prefix_to = preprocess_prefix(prefix_to)

    def format_inner_namespace(inner_namespace, separator, case):
        return "".join(
            map(lambda el: separator + convert_case(el, case), inner_namespace)
        )

    def generate_options(inner_namespace):
        first = True
        for key, value in options.items():
            group = Group()
            if first:
                first = False
                group.append("space", "\n\n")
            else:
                group.append("space", "\n")
            group.append("ident", "option")
            group.append("space", " ")
            group.append("ident", key)
            group.append("space", " ")
            group.append("symbol", "=")
            group.append("space", " ")
            if isinstance(value, str):
                value = re.sub(
                    r"{([^{}a-zA-Z]+)([a-zA-Z]+)}",
                    lambda x: format_inner_namespace(
                        inner_namespace, x.group(1), x.group(2)
                    ),
                    value,
                )
                value = value.replace("{{", "{")
                value = value.replace("}}", "}")
                value = value.replace("\\", "\\\\")
                value = value.replace("\n", "\\n")
                value = value.replace('"', '\\"')
                group.append("string", f'"{value}"')
            elif isinstance(value, bool):
                if value:
                    group.append("ident", "true")
                else:
                    group.append("ident", "false")
            elif isinstance(value, int):
                group.append("number", str(value))
            else:
                raise TypeError(type(value))
            group.append("symbol", ";")
            yield group

    def convert_groups(groups):
        inner_namespace = []
        seen_options = False
        for group in groups:
            # Update package statement.
            if group[0] == "package":
                assert len(group) == 3
                assert group.cls(1) == "ident"
                package = group[1].split(".")
                if package[: len(prefix_from)] != prefix_from:
                    raise IgnoreFile()
                inner_namespace = package[len(prefix_from) :]
                group[1] = ".".join(prefix_to + inner_namespace)
                yield group

            # Update import statements.
            elif group[0] == "import":
                assert len(group) == 3
                assert group.cls(1) == "string"
                components = group[1][1:-1].split("/")
                assert components
                if components[: len(prefix_from)] == prefix_from:
                    components = prefix_to + components[len(prefix_from) :]
                group[1] = '"' + "/".join(components) + '"'
                yield group

            # Replace option statements.
            elif group[0] == "option":
                if not seen_options:
                    seen_options = True
                    yield from generate_options(inner_namespace)

            # For all other groups, modify any identifiers that look like a
            # fully-qualified type name.
            else:
                for idx, token in enumerate(group):
                    if group.cls(idx) == "ident":
                        name = token.split(".")
                        if name[: len(prefix_from)] == prefix_from:
                            name = prefix_to + name[len(prefix_from) :]
                            group[idx] = ".".join(name)
                yield group

    return convert_groups


def get_package(groups):
    """Given a list of groups, find the package statement and return its
    content. If there is no package statement, return []."""
    for group in groups:
        if group[0] == "package":
            assert len(group) == 3
            assert group.cls(1) == "ident"
            return group[1].split(".")
    return []


def convert_files(
    dest_dir, dest_prefix, src_dir=".", src_prefix="substrait", **options
):
    """Converts all proto files found in src_dir (or the current directory if
    None) to the given destination directory, replacing the given package
    prefix (by default, substrait becomes dest_prefix and substrait.extensions
    becomes dest_prefix.extensions) and the given option statements. For the
    options, the argument name is the option name, and the value is either a
    str, int, or bool representing the value. str options may include
    {<case><sep>} capture groups, where case is either lower, UPPER, Pascal, or
    camel, and sep is any separator. This expands to ".extensions" (with the
    appropriate separator in place of . and with extensions written in the
    appropriate case convention) for substrait.extensions, and to the empty
    string for substrait."""

    group_converter = make_group_converter(src_prefix, dest_prefix, **options)

    n_written = 0
    n_up_to_date = 0
    n_not_in_prefix = 0

    for src_path in pathlib.Path(src_dir).rglob("*.proto"):
        with open(src_path, "r", encoding="utf-8") as fil:
            data = fil.read()
        try:
            groups = list(group_converter(group_tokens(tokenize(data))))
        except IgnoreFile:
            n_not_in_prefix += 1
            continue
        data = "".join(map(str, groups))
        dest_path = pathlib.Path(dest_dir, *get_package(groups), src_path.name)
        if dest_path.exists():
            with open(dest_path, "r", encoding="utf-8") as fil:
                if fil.read() == data:
                    n_up_to_date += 1
                    continue
        else:
            dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "w", encoding="utf-8") as fil:
            fil.write(data)
        n_written += 1

    return n_written, n_up_to_date, n_not_in_prefix


def cmd_line():
    """Runs the script as if it had been run from the command line."""

    # Unpack command line.
    positional = []
    options = {}
    for arg in sys.argv[1:]:
        arg = arg.split("=", maxsplit=1)
        if len(arg) == 2:
            option, value = arg
            if not value:
                value = None
            elif value == "true":
                value = True
            elif value == "false":
                value = False
            elif re.fullmatch(r"[0-9]+", value):
                value = int(value)
            options[option] = value
        else:
            (value,) = arg
            positional.append(value)

    # Check command line, print help if wrong.
    script = (sys.argv[:1] + ["proto_prefix.py"])[0]
    if len(positional) < 2 or len(positional) > 4:
        print(
            f"Usage: {script} <dest_dir> <dest_prefix> "
            "[src_dir] [src_prefix] [key=value...]"
        )
        print("Default src_dir = .")
        print("Default src_prefix = substrait")
        print(__doc__)
        sys.exit(2)

    # Load default options, to mimic the options currently in the Substrait
    # proto files. The go namespace is not included, as it seems to be too
    # specific to compute from just a prefix.
    lower_prefix = positional[1]
    csharp_prefix = ".".join(
        map(lambda el: convert_case(el, "Pascal"), lower_prefix.split("."))
    )
    java_prefix = lower_prefix
    default_options = dict(
        csharp_namespace=f"{csharp_prefix}.Protobuf",
        java_multiple_files=True,
        java_package=f"io.{java_prefix}.proto",
    )
    default_options.update(options)
    options = dict(filter(lambda x: x[1] is not None, default_options.items()))

    # Perform the conversion.
    n_written, n_up_to_date, n_not_in_prefix = convert_files(*positional, **options)

    # Print statistics.
    print(
        f"{script}: wrote {n_written} file(s), {n_up_to_date} up-to-date, "
        f"{n_not_in_prefix} not in src prefix"
    )


if __name__ == "__main__":
    cmd_line()

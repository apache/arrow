import pathlib


def _is_path_like(path):
    # PEP519 filesystem path protocol is available from python 3.6, so pathlib
    # doesn't implement __fspath__ for earlier versions
    return (isinstance(path, str) or
            hasattr(path, '__fspath__') or
            isinstance(path, pathlib.Path))


def _ensure_path(path):
    if isinstance(path, pathlib.Path):
        return path
    else:
        return pathlib.Path(_stringify_path(path))


def _stringify_path(path):
    """
    Convert *path* to a string or unicode path if possible.
    """
    if isinstance(path, str):
        return path

    # checking whether path implements the filesystem protocol
    try:
        return path.__fspath__()  # new in python 3.6
    except AttributeError:
        # fallback pathlib ckeck for earlier python versions than 3.6
        if isinstance(path, pathlib.Path):
            return str(path)

    raise TypeError("not a path-like object")

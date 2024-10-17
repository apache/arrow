from .Dependencies import cythonize

import sys
if sys.version_info < (3, 7):
    from .Distutils import build_ext
del sys


def __getattr__(name):
    if name == 'build_ext':
        # Lazy import, fails if distutils is not available (in Python 3.12+).
        from .Distutils import build_ext
        return build_ext
    raise AttributeError("module '%s' has no attribute '%s'" % (__name__, name))

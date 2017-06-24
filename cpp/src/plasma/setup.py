from setuptools import setup, find_packages, Extension
from Cython.Build import cythonize

extensions = [
    Extension(
        'plasma',
        ["plasma.pyx"],
        language="c++",
        extra_link_args=["../../build/debug/libplasma.a", "../../build/debug/libarrow.a"],
        extra_compile_args=["-std=c++11", "-I.", "-I..", "-I../../../cpp/build/flatbuffers_ep-prefix/src/flatbuffers_ep-install/include"])]

setup(ext_modules = cythonize(extensions))

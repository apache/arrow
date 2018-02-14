import torch.cuda
from setuptools import setup
from torch.utils.cpp_extension import CppExtension, CUDAExtension

ext_modules = [
    CppExtension(
        'pytorch_example', ['extension.cpp'],
        extra_compile_args=['-g']),
]

setup(
    name='pytorch_example',
    ext_modules=ext_modules,
    cmdclass={'build_ext': torch.utils.cpp_extension.BuildExtension})

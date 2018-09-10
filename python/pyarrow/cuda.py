try:
    import pyarrow.lib_gpu as _lib_gpu
except ImportError as _msg:
    _msg = str(_msg)
    if _msg.startswith('No module named') and 'lib_gpu' in _msg:
        has_gpu_support = False
    else: # do not silence ImportError in case of any bugs
        raise
else:
    has_gpu_support = True

if has_gpu_support:
    from pyarrow.lib_gpu \
        import (CudaDeviceManager, CudaContext, CudaIpcMemHandle,
                CudaBuffer, CudaHostBuffer, CudaBufferReader, CudaBufferWriter,
                allocate_cuda_host_buffer, cuda_serialize_record_batch,
                cuda_read_message, cuda_read_record_batch,
                )

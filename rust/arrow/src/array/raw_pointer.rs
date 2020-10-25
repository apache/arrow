use crate::memory;

pub(super) struct RawPtrBox<T> {
    inner: *const T,
}

impl<T> RawPtrBox<T> {
    pub(super) fn new(inner: *const T) -> Self {
        Self { inner }
    }

    pub(super) fn get(&self) -> *const T {
        self.inner
    }
}

unsafe impl<T> Send for RawPtrBox<T> {}
unsafe impl<T> Sync for RawPtrBox<T> {}

pub(super) fn as_aligned_pointer<T>(p: *const u8) -> *const T {
    assert!(
        memory::is_aligned(p, std::mem::align_of::<T>()),
        "memory is not aligned"
    );
    p as *const T
}

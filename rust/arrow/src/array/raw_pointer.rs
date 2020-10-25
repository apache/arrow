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

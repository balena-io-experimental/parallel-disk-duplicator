use std::slice;
use std::ops::{Deref, DerefMut};
use std::alloc::{Layout, alloc, dealloc};

const BLOCK_SIZE: usize = 4096;

pub struct Buf4K {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for Buf4K {}
unsafe impl Sync for Buf4K {}

impl Buf4K {
    pub fn new(blocks: usize) -> Self {
        let len = blocks * BLOCK_SIZE;
        let layout = Layout::from_size_align(len, BLOCK_SIZE).unwrap();

        unsafe {
            let ptr = alloc(layout);
            Self{ptr, len}
        }
    }
}

impl Drop for Buf4K {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.len, BLOCK_SIZE).unwrap();
        unsafe {
            dealloc(self.ptr, layout);
        }
    }
}

impl Deref for Buf4K {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            slice::from_raw_parts(self.ptr, self.len)
        }
    }
}

impl DerefMut for Buf4K {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            slice::from_raw_parts_mut(self.ptr, self.len)
        }
    }
}

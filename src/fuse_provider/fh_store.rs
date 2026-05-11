use std::ops::{Index, IndexMut};

use crate::fuse_provider::fh::FileHandle;

/// Keep a list of objects that can be added or deleted. We use a vector with
/// objects that are either present or absent, with a so-called Gravestone
/// indicating absence. Gravestones point to each other like a linked-list,
/// which lets us reuse slots.
///
/// Very normal-style code except that we have to play with types to use u64 as
/// an index. Since the FUSE layer has u64 file handles, this isn't a *terrible*
/// idea, and works *fine.* If you don't compile this on a 64-bit system, I
/// guess I'm a little sorry (:.
///
/// One more thing to be aware of: There are very many FileHandle types. Here,
/// we refer to the actual object that we use for communication over SFTP. In
/// other places, we use FileHandle to refer to an index into this data
/// structure.
#[derive(Default)]
pub struct FhStore {
    inner: Vec<Slot>,
    first_gravestone: Option<u64>,
}

enum Slot {
    Used(FileHandle),
    Gravestone { next: Option<u64> },
}

impl FhStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, fh: FileHandle) -> u64 {
        if let Some(idx) = self.first_gravestone.take() {
            let old = std::mem::replace(&mut self.inner[idx as usize], Slot::Used(fh));
            self.first_gravestone = old.take_gravestone();
            idx as u64
        } else {
            self.inner.push(Slot::Used(fh));
            (self.inner.len() - 1) as u64
        }
    }

    pub fn delete(&mut self, idx: u64) -> FileHandle {
        let next = std::mem::replace(&mut self.first_gravestone, Some(idx));
        std::mem::replace(&mut self.inner[idx as usize], Slot::Gravestone { next })
            .take_filehandle()
    }
}

impl IndexMut<u64> for FhStore {
    fn index_mut(&mut self, index: u64) -> &mut Self::Output {
        self.inner[index as usize].borrow_fh_mut()
    }
}

impl Index<u64> for FhStore {
    type Output = FileHandle;

    fn index(&self, index: u64) -> &Self::Output {
        self.inner[index as usize].borrow_fh()
    }
}

impl Slot {
    fn take_gravestone(self) -> Option<u64> {
        match self {
            Slot::Gravestone { next } => next,
            _ => panic!("take_gravestone on a non-gravestone"),
        }
    }

    fn take_filehandle(self) -> FileHandle {
        match self {
            Slot::Used(fh) => fh,
            _ => panic!("take_filehandle on a non-filehandle"),
        }
    }

    fn borrow_fh_mut(&mut self) -> &mut FileHandle {
        match self {
            Slot::Used(fh) => fh,
            _ => panic!("Cannot borrow file handle."),
        }
    }

    fn borrow_fh(&self) -> &FileHandle {
        match self {
            Slot::Used(fh) => fh,
            _ => panic!("Cannot borrow file handle."),
        }
    }
}

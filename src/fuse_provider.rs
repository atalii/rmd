use std::{ffi::OsStr, sync::Mutex, time::Duration};

use anyhow::{Context, Result};
use fuser::{
    AccessFlags, Errno, FileHandle, FileType, Filesystem, FopenFlags, INodeNo, ReplyAttr,
    ReplyDirectory, ReplyEmpty, ReplyEntry, Request,
};
use serde::{Deserialize, Serialize};

use crate::{fuse_provider::fh_store::FhStore, rm_manager::Manager};

mod fh;
mod fh_store;
mod tablet;

pub struct FuseProvider {
    sess_manager: Mutex<Manager>,
    tablet_db: Mutex<tablet::Db>,
    file_handles: Mutex<FhStore>,
    tokio_handle: tokio::runtime::Handle,

    // Keep track of our owning uid, gid for sending perm info.
    uid: u32,
    gid: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    #[serde(default)]
    deleted: bool,
    last_modified: String,
    #[serde(default)]
    metadata_modified: bool,
    parent: String,
    #[serde(default)]
    pinned: bool,
    #[serde(default)]
    synced: bool,
    r#type: String,
    #[serde(default)]
    version: u32,
    visible_name: String,
}

impl FuseProvider {
    pub async fn new(mut manager: Manager, tokio_handle: tokio::runtime::Handle) -> Result<Self> {
        let sess = manager.sess();
        let db = Mutex::new(
            tablet::Db::build(sess)
                .await
                .context("Failed to initialize fuse provider.")?,
        );

        log::info!("Built DB.");

        let manager = Mutex::new(manager);

        let uid = unsafe { libc::geteuid() };
        let gid = unsafe { libc::getegid() };
        log::debug!("Detected euid, egid: {uid}, {gid}");

        Ok(Self {
            sess_manager: manager,
            tokio_handle,
            tablet_db: db,
            file_handles: Mutex::new(FhStore::new()),
            uid,
            gid,
        })
    }
}

impl Filesystem for FuseProvider {
    fn access(&self, _req: &Request, ino: INodeNo, _mask: AccessFlags, reply: ReplyEmpty) {
        if self.tablet_db.lock().unwrap().exists(ino.into()) {
            reply.ok()
        } else {
            reply.error(Errno::ENOENT)
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let db = self.tablet_db.lock().unwrap();

        let Some(attr) = db.get_attr_for(ino.into(), self.uid, self.gid) else {
            reply.error(Errno::ENOENT);
            return;
        };

        reply.attr(&Duration::new(0, 0), &attr);
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let db = self.tablet_db.lock().unwrap();
        let children = match db.get_children(ino.into()) {
            Ok(children) => {
                // Send in order of increasing inode no. so that, even in presence of removals or
                // deletions, the same item is never sent more than once, which is fsr a
                // requirement here.
                let mut r = Vec::from(children);
                r.sort();
                r
            }
            Err(e) => {
                reply.error(e);
                return;
            }
        };

        for child_ino in children.into_iter().skip_while(|x| x.0 <= offset) {
            match db.get_metadata(child_ino) {
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
                Some(md) => {
                    // Returns true iff buffer is full!
                    if reply.add(ino, child_ino.0, FileType::RegularFile, &md.visible_name) {
                        break;
                    }
                }
            }
        }

        reply.ok()
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let db = self.tablet_db.lock().unwrap();
        let children = match db.get_children(parent.into()) {
            Err(e) => {
                reply.error(e);
                return;
            }
            Ok(children) => children,
        };

        for &child in children {
            let Some(md) = db.get_metadata(child) else {
                log::error!("Non-existent child referenced w/ id: {}", child);
                reply.error(Errno::EFAULT);
                return;
            };

            if <String as AsRef<OsStr>>::as_ref(&md.visible_name) == name {
                match db.get_attr_for(tablet::DbPointer::INode { id: child }, self.uid, self.gid) {
                    Some(attr) => reply.entry(&Duration::new(0, 0), &attr, fuser::Generation(0)),
                    None => reply.error(Errno::ENOENT),
                }
                return;
            }
        }

        reply.error(Errno::ENOENT);
    }

    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let Some(str_name) = name.to_str() else {
            reply.error(Errno::EINVAL);
            return;
        };

        use tablet::NewFileError;

        let mut db = self.tablet_db.lock().unwrap();
        let mut man = self.sess_manager.lock().unwrap();
        match db.new_file(parent.into(), str_name, man.sess(), &self.tokio_handle) {
            Err(e) => {
                log::warn!("while creating file: {e}");
                match e {
                    NewFileError::CantResolveParent => reply.error(Errno::EINVAL),
                    NewFileError::TransportError(_) => reply.error(Errno::EIO),
                }
            }
            Ok(id) => {
                let attr = db
                    .get_attr_for(tablet::DbPointer::INode { id }, self.uid, self.gid)
                    .unwrap();

                reply.entry(&Duration::new(0, 0), &attr, fuser::Generation(0));
            }
        };
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: fuser::OpenFlags, reply: fuser::ReplyOpen) {
        let db = self.tablet_db.lock().unwrap();
        let mut man = self.sess_manager.lock().unwrap();
        match db.open_file(ino, flags, man.sess(), &self.tokio_handle) {
            Ok(handle) => {
                let mut file_handles = self.file_handles.lock().unwrap();
                let idx = file_handles.push(handle);
                reply.opened(FileHandle(idx), FopenFlags::empty());
            }
            Err(e) => {
                log::warn!("While opening file: {e:?}");
                reply.error(e);
            }
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let fh = {
            let mut fh_store = self.file_handles.lock().unwrap();
            fh_store.delete(fh.0)
        };

        let mut sess = self.sess_manager.lock().unwrap();

        if let Err(e) = self.tokio_handle.block_on(fh.shutdown(sess.sess())) {
            log::error!("Can't shutdown file: {e}");
            reply.error(Errno::EIO);
        } else {
            reply.ok();
        }
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyData,
    ) {
        let mut fhs = self.file_handles.lock().unwrap();
        let fh = &mut fhs[fh.0];

        match self.tokio_handle.block_on(fh.read(offset, size as usize)) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e),
        };
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        let mut fhs = self.file_handles.lock().unwrap();
        let fh = &mut fhs[fh.0];

        let mut man = self.sess_manager.lock().unwrap();

        match self
            .tokio_handle
            .block_on(fh.write(man.sess(), offset, data))
        {
            Ok(x) => reply.written(x),
            Err(e) => reply.error(e),
        }
    }
}

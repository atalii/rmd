use fuser::{Errno, FileType, INodeNo};
use russh_sftp::client::SftpSession;
use std::{collections::HashMap, path::PathBuf, time::SystemTime};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use anyhow::{Context, Result, bail};

use crate::fuse_provider::fh;

#[derive(Default)]
pub struct Db {
    root: Vec<INodeNo>,
    trash: Vec<INodeNo>,
    store: HashMap<INodeNo, Asset>,
}

struct Asset {
    md: super::Metadata,
    children: Vec<INodeNo>,
    id: Uuid,
    underlying: Option<Underlying>,
    size_on_disk: u64,
}

enum Underlying {
    Pdf,
    Epub,
}

#[derive(Clone, Copy)]
pub enum DbPointer {
    Root,
    INode { id: INodeNo },
}

#[derive(Error, Debug)]
pub enum NewFileError {
    #[error("Can't resolve parent.")]
    CantResolveParent,
    #[error("Can't talk to tablet: {0}")]
    TransportError(#[from] TabletSftpError),
}

#[derive(Error, Debug)]
pub enum TabletSftpError {
    #[error("IO: {0}")]
    IO(#[from] tokio::io::Error),
    #[error("SFTP error: {0}")]
    TransportError(#[from] russh_sftp::client::error::Error),
    #[error("Serialization error: {0}")]
    SerializationFailure(#[from] serde_json::Error),
}

pub const XOCHITL_PATH: &str = "/home/root/.local/share/remarkable/xochitl";

pub fn uuid_to_ino(id: Uuid) -> INodeNo {
    let id: &[u8] = id.as_ref();
    assert_eq!(id.len(), 16);

    // discard half the bytes of the UUID. 2^64 ought to be enough for anyone
    INodeNo(u64::from_le_bytes([
        id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7],
    ]))
}

impl Db {
    pub async fn build(sess: &SftpSession) -> Result<Self> {
        let mut db = Self::default();
        let items = sess
            .read_dir(XOCHITL_PATH)
            .await?
            .filter(|x| x.file_type().is_file() && x.file_name().ends_with(".metadata"));

        for item in items {
            let file_name = item.file_name();

            let path = PathBuf::from(XOCHITL_PATH).join(&file_name);
            let md = sess.read(path.to_string_lossy()).await?;
            let md: super::Metadata = serde_json::from_slice(md.as_slice())
                .context(format!("Couldn't decode: {path:?}"))?;

            let without_ext = file_name.strip_suffix(".metadata").unwrap();
            let id = Uuid::parse_str(without_ext).context("Failed to parse ID.")?;

            let underlying = Underlying::detect(sess, without_ext).await?;
            let (underlying, size_on_disk) = match underlying {
                None => (None, 0),
                Some((u, s)) => (Some(u), s),
            };

            db.push(id, md, underlying, size_on_disk)?;
        }

        db.fixup_parentage()?;

        Ok(db)
    }

    fn fixup_parentage(&mut self) -> Result<()> {
        let keys: Vec<_> = self.store.keys().cloned().collect();
        for id in keys {
            let v = self.store.get(&id).unwrap();
            match v.md.parent.as_str() {
                "" => self.root.push(id),
                "trash" => self.trash.push(id),
                p => {
                    let p = Uuid::parse_str(p)
                        .with_context(|| format!("Failed to parse parent: {}", &v.md.parent))
                        .map(uuid_to_ino)?;

                    match self.store.get_mut(&p) {
                        None => bail!("Missing ID referred to in parent field: {p:?}"),
                        Some(v) => v.children.push(id),
                    }
                }
            }
        }

        Ok(())
    }

    /// Add an asset to the store. Do not maintain parentage structure.
    fn push(
        &mut self,
        id: Uuid,
        md: super::Metadata,
        underlying: Option<Underlying>,
        size_on_disk: u64,
    ) -> Result<()> {
        let asset = Asset {
            md,
            id,
            underlying,
            size_on_disk,
            children: Vec::new(),
        };

        let ino = uuid_to_ino(id);
        if self.store.insert(ino, asset).is_some() {
            bail!("Duplicate ID: {id:?}");
        };

        Ok(())
    }
}

impl Db {
    pub fn exists(&self, ptr: DbPointer) -> bool {
        match ptr {
            DbPointer::INode { id } => self.store.contains_key(&id),
            _ => true,
        }
    }

    pub fn get_children(&self, ptr: DbPointer) -> Result<&'_ [INodeNo], Errno> {
        match ptr {
            DbPointer::Root => Ok(self.root.as_slice()),
            DbPointer::INode { id } => match self.store.get(&id) {
                Some(asset) => {
                    if asset.md.r#type == "CollectionType" {
                        Ok(asset.children.as_slice())
                    } else {
                        Err(Errno::ENOTDIR)
                    }
                }
                None => Err(Errno::ENOENT),
            },
        }
    }

    pub fn new_file<T: AsRef<str>>(
        &mut self,
        parent_ptr: DbPointer,
        name: T,
        sess: &SftpSession,
        handle: &tokio::runtime::Handle,
    ) -> Result<INodeNo, NewFileError> {
        let parent = match parent_ptr {
            DbPointer::Root => "".to_string(),
            DbPointer::INode { id } => self
                .get_uuid(id)
                .ok_or(NewFileError::CantResolveParent)?
                .to_string(),
        };

        let child = Asset {
            md: super::Metadata {
                deleted: false,
                last_modified: "0".to_string(),
                metadata_modified: false,
                parent,
                pinned: false,
                synced: false,
                r#type: "DocumentType".to_string(),
                version: 0,
                visible_name: name.as_ref().to_string(),
            },
            id: Uuid::new_v4(),
            underlying: None,
            size_on_disk: 0,
            children: Vec::new(),
        };

        handle.block_on(async { self.init_file(sess, &child).await })?;

        // Now that it's written, update the fs bookkeeping:
        let child_ino = uuid_to_ino(child.id);
        assert!(self.store.insert(child_ino, child).is_none());

        match parent_ptr {
            DbPointer::Root => self.root.push(child_ino),
            DbPointer::INode { id } => match self.store.get_mut(&id) {
                None => return Err(NewFileError::CantResolveParent),
                Some(entry) => entry.children.push(child_ino),
            },
        };

        Ok(child_ino)
    }

    pub fn open_file(
        &self,
        ino: INodeNo,
        flags: fuser::OpenFlags,
        sess: &SftpSession,
        handle: &tokio::runtime::Handle,
    ) -> std::result::Result<fh::FileHandle, Errno> {
        let Some(asset) = self.store.get(&ino) else {
            return Err(Errno::ENOENT);
        };

        handle.block_on(async {
            match &asset.underlying {
                None => Ok(fh::FileHandle::create(asset.uuid_string()).await),
                Some(u) => {
                    let uuid = asset.uuid_string();
                    let ext = u.ext();
                    let path = format!("{XOCHITL_PATH}/{uuid}.{ext}");

                    fh::FileHandle::open(path, sess, flags.acc_mode()).await
                }
            }
        })
    }

    async fn init_file(&self, sess: &SftpSession, asset: &Asset) -> Result<(), TabletSftpError> {
        let content_path = asset.content_path();
        log::debug!("Writing content to: {content_path}");

        let mut content_file = sess.create(content_path).await?;
        content_file.write_all(b"{}").await?;

        let metadata_path = asset.metadata_path();
        log::debug!("Writing metadata to: {metadata_path}");

        let mut metadata_file = sess.create(metadata_path).await?;
        let metadata_content = serde_json::to_vec(&asset.md)?;
        metadata_file.write_all(&metadata_content).await?;

        Ok(())
    }

    pub fn get_metadata(&self, id: INodeNo) -> Option<&super::Metadata> {
        self.store.get(&id).map(|x| &x.md)
    }

    fn get_size_on_disk(&self, id: INodeNo) -> Option<u64> {
        self.store.get(&id).map(|x| x.size_on_disk)
    }

    pub fn get_uuid(&self, id: INodeNo) -> Option<Uuid> {
        self.store.get(&id).map(|x| x.id)
    }

    pub fn get_attr_for(&self, ptr: DbPointer, uid: u32, gid: u32) -> Option<fuser::FileAttr> {
        match ptr {
            DbPointer::INode { id } => {
                let tp = &self.get_metadata(id)?.r#type;
                let sz = self.get_size_on_disk(id)?;
                Some(fuser::FileAttr {
                    ino: id,
                    size: sz,
                    blocks: 0,
                    atime: SystemTime::UNIX_EPOCH,
                    mtime: SystemTime::UNIX_EPOCH,
                    ctime: SystemTime::UNIX_EPOCH,
                    crtime: SystemTime::UNIX_EPOCH,
                    kind: if tp == "CollectionType" {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    },
                    blksize: 4096,
                    flags: 0,
                    uid,
                    gid,
                    nlink: 0,
                    perm: if tp == "CollectionType" { 0o770 } else { 0o660 },
                    rdev: 0,
                })
            }
            DbPointer::Root => Some(fuser::FileAttr {
                ino: INodeNo(1),
                size: 0,
                blocks: 0,
                atime: SystemTime::UNIX_EPOCH,
                mtime: SystemTime::UNIX_EPOCH,
                ctime: SystemTime::UNIX_EPOCH,
                crtime: SystemTime::UNIX_EPOCH,
                kind: FileType::Directory,
                blksize: 4096,
                flags: 0,
                uid,
                gid,
                nlink: 0,
                perm: 0o770,
                rdev: 0,
            }),
        }
    }
}

impl Asset {
    fn content_path(&self) -> String {
        let uuid = self.uuid_string();

        // Since this is a UUID, string formatting paths is *fine.*
        format!("{XOCHITL_PATH}/{uuid}.content")
    }

    fn metadata_path(&self) -> String {
        let uuid = self.uuid_string();
        format!("{XOCHITL_PATH}/{uuid}.metadata")
    }

    fn uuid_string(&self) -> String {
        let mut buf = Uuid::encode_buffer();
        self.id.hyphenated().encode_lower(&mut buf).to_string()
    }
}

impl From<INodeNo> for DbPointer {
    fn from(id: INodeNo) -> Self {
        match id.0 {
            1 => DbPointer::Root,
            _ => DbPointer::INode { id },
        }
    }
}

impl From<Uuid> for DbPointer {
    fn from(uuid: Uuid) -> Self {
        uuid_to_ino(uuid).into()
    }
}

impl Underlying {
    async fn detect(sess: &SftpSession, uuid_slug: &str) -> Result<Option<(Self, u64)>> {
        let pdf_path = format!("{XOCHITL_PATH}/{uuid_slug}.pdf");
        let epub_path = format!("{XOCHITL_PATH}/{uuid_slug}.epub");

        if sess.try_exists(&pdf_path).await? {
            let size = sess.metadata(&pdf_path).await?.size;
            Ok(size.and_then(|size| Some((Self::Pdf, size))))
        } else if sess.try_exists(&epub_path).await? {
            let size = sess.metadata(&epub_path).await?.size;
            Ok(size.and_then(|size| Some((Self::Epub, size))))
        } else {
            Ok(None)
        }
    }

    fn ext(&self) -> &'static str {
        match self {
            Underlying::Pdf => "pdf",
            Underlying::Epub => "epub",
        }
    }
}

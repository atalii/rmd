//! Utilities for dealing with file handles.
//!
//! Unfortunately, much of the APIs must be reimplemented here since a file
//! handle has so many functions. The chief cause of complexity is that when
//! creating a file, we won't know what the file needs to be called (usually
//! either stub.epub, stub.pdf) until after we get the first several bytes.

use std::io::SeekFrom;

use fuser::{Errno, OpenAccMode};
use russh_sftp::{self, client::SftpSession, protocol::OpenFlags};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::tablet::XOCHITL_PATH;

pub type Result<T> = std::result::Result<T, Errno>;

/// Since a .metadata may exist without any content, we need to provide a
/// different kind of handle in this case which may be used to actually create
/// the file.
pub enum FileHandle {
    Created(CreatedFileHandle),
    Remote(russh_sftp::client::fs::File),
}

pub struct CreatedFileHandle {
    buf: Vec<u8>,
    future_path_stub: String,
}

const PDF_MAGIC_NUMBER: [u8; 5] = [0x25, 0x50, 0x44, 0x46, 0x2d];
const EPUB_MAGIC_NUMBER: [u8; 4] = [0x50, 0x4b, 0x03, 0x04];

impl FileHandle {
    /// Create a file and return a file handle. This will not yet create the
    /// file on the tablet - recall that we need to see several bytes before
    /// committing to a file name.
    pub async fn create<I: Into<String>>(stub: I) -> Self {
        Self::Created(CreatedFileHandle {
            buf: Vec::new(),
            future_path_stub: format!("{XOCHITL_PATH}/{}", stub.into()),
        })
    }

    pub async fn open<I: Into<String>>(
        path: I,
        sess: &SftpSession,
        mode: OpenAccMode,
    ) -> Result<Self> {
        let flags = match mode {
            OpenAccMode::O_RDONLY => OpenFlags::READ,
            OpenAccMode::O_WRONLY => OpenFlags::WRITE,
            OpenAccMode::O_RDWR => {
                log::warn!("Can't open: {}. O_RDWR unsupported.", path.into());
                return Err(Errno::ENOTSUP);
            }
        };

        match sess.open_with_flags(path, flags).await {
            Ok(h) => Ok(Self::Remote(h)),
            Err(e) => {
                log::warn!("Failed to open file: {e}");
                // There's not really a good errno for this, but we do our best:
                Err(Errno::EBADMSG)
            }
        }
    }

    pub async fn read(&mut self, offset: u64, len: usize) -> Result<Vec<u8>> {
        let Self::Remote(inner) = self else {
            return Err(Errno::EBADF);
        };

        if let Err(e) = inner.seek(SeekFrom::Start(offset)).await {
            log::warn!("Failed seeking remote file: {e}");
            return Err(Errno::EIO);
        }

        let mut out = vec![0; len];
        if let Err(e) = inner.read_exact(out.as_mut_slice()).await {
            log::warn!("Failed reading remote file: {e}");
            return Err(Errno::EIO);
        }

        Ok(out)
    }

    pub async fn write(&mut self, sess: &SftpSession, offset: u64, data: &[u8]) -> Result<u32> {
        match self {
            FileHandle::Remote(f) => {
                f.seek(SeekFrom::Start(offset))
                    .await
                    .map_err(|x| Errno::EIO)?;
                let written = f.write(data).await?;
                written.try_into().map_err(|_| Errno::EOVERFLOW)
            }
            FileHandle::Created(fh) => {
                fh.buf.extend_from_slice(data);
                if let Some(new_path) = if fh.buf.starts_with(&PDF_MAGIC_NUMBER) {
                    Some(format!("{}.pdf", fh.future_path_stub))
                } else if fh.buf.starts_with(&EPUB_MAGIC_NUMBER) {
                    Some(format!("{}.epub", fh.future_path_stub))
                } else {
                    None
                } {
                    log::info!("Creating file at: {new_path}");
                    let mut f = sess.create(new_path).await.map_err(|e| {
                        log::warn!("Can't create file: {e}");
                        Errno::EIO
                    })?;

                    if let Err(e) = f.write_all(fh.buf.as_slice()).await {
                        log::warn!("Can't write to new file: {e}");
                        return Err(Errno::EIO);
                    };

                    *self = FileHandle::Remote(f);
                };

                data.len().try_into().map_err(|_| Errno::EOVERFLOW)
            }
        }
    }

    pub async fn shutdown(self) -> std::io::Result<()> {
        match self {
            FileHandle::Created(f) => f.shutdown().await,
            FileHandle::Remote(mut file) => file.shutdown().await,
        }
    }
}

impl CreatedFileHandle {
    async fn shutdown(self) -> std::io::Result<()> {
        // If this is called, we haven't seen enough data to decide whether on
        // our file extension.
        //
        // TODO: commit to a .rmd-unknown file extension and warn.
        Err(std::io::Error::from_raw_os_error(
            fuser::Errno::EINVAL.into(),
        ))
    }
}

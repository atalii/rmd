use std::{
    ops::DerefMut,
    sync::{Arc, Mutex},
};

use anyhow::{Result, bail};
use fuse_provider::FuseProvider;
use fuser::{BackgroundSession, spawn_mount2};

pub mod fuse_provider;
pub mod net_listener;
pub mod rm_manager;

#[derive(Debug)]
enum MonitorInner {
    Running(BackgroundSession),
    Pending,
    Disconnected,
    Disconnecting,
}

pub struct Monitor {
    inner: Arc<Mutex<MonitorInner>>,
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}

impl Monitor {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MonitorInner::Disconnected)),
        }
    }

    pub async fn disconnect(&self) -> Result<()> {
        let handle = {
            let mut inner = self.inner.lock().unwrap();
            if !matches!(*inner, MonitorInner::Running(_)) {
                bail!("Attempted to disconnect while monitor reports: {inner:?}");
            } else {
                let MonitorInner::Running(i) =
                    std::mem::replace(inner.deref_mut(), MonitorInner::Disconnecting)
                else {
                    unreachable!()
                };
                i
            }
        };

        if let Err(e) = tokio::task::spawn_blocking(move || handle.umount_and_join()).await {
            log::error!("Failed to unmount: {e}");
        }

        let mut inner = self.inner.lock().unwrap();
        *inner = MonitorInner::Disconnecting;

        Ok(())
    }

    pub async fn start_connection(&self) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            match *inner {
                MonitorInner::Running(_) => bail!("Attempt to start a running connection."),
                MonitorInner::Pending => bail!("Attempt to start a pending connection."),
                _ => (),
            };

            *inner = MonitorInner::Pending;
        }

        let inner = self.inner.clone();
        tokio::spawn(async move {
            let m = match rm_manager::Manager::new().await {
                Ok(m) => {
                    log::debug!("Manager object created.");
                    m
                }
                Err(e) => {
                    log::error!("Initializing connection with tablet failed: {e}");
                    let mut inner = inner.lock().unwrap();
                    *inner = MonitorInner::Disconnected;
                    return;
                }
            };

            match FuseProvider::new(m, tokio::runtime::Handle::current()).await {
                Ok(f) => {
                    let mut inner = inner.lock().unwrap();
                    let mut opts: fuser::Config = Default::default();
                    opts.mount_options
                        .push(fuser::MountOption::DefaultPermissions);

                    match spawn_mount2(f, "/tmp/tablet", &opts) {
                        Ok(rs) => {
                            log::info!("Mounted!");
                            *inner = MonitorInner::Running(rs);
                        }
                        Err(e) => {
                            *inner = MonitorInner::Disconnected;
                            log::info!("Couldn't mount filesystem: {e}");
                        }
                    };
                }
                Err(e) => {
                    log::error!("Failed to initialize fuse provider: {e}");
                    let mut inner = inner.lock().unwrap();
                    *inner = MonitorInner::Disconnected;
                }
            };
        });

        Ok(())
    }
}

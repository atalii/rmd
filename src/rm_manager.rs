use std::{default, sync::Arc};

use anyhow::{self, Context, Result, bail};
use russh::{
    client,
    keys::{PrivateKey, key::PrivateKeyWithHashAlg},
};
use russh_sftp::client::SftpSession;
use tokio::fs;

struct SshHandler;

impl client::Handler for SshHandler {
    type Error = anyhow::Error;

    async fn check_server_key(&mut self, pub_key: &russh::keys::PublicKey) -> Result<bool> {
        match pub_key.to_openssh() {
            Ok(k) => log::info!("Accepting key: {k}"),
            Err(e) => log::warn!("Accepting key that we can't display: {e}: {pub_key:?}"),
        }

        Ok(true)
    }
}

pub struct Manager {
    handle: client::Handle<SshHandler>,
    sess: SftpSession,
}

impl Manager {
    pub async fn new() -> Result<Self> {
        let handler = SshHandler {};
        let conf = default::Default::default();
        let mut handle = client::connect(conf, "10.11.99.1:22", handler).await?;

        let pkey = read_private_key().await?;

        if !handle.authenticate_publickey("root", pkey).await?.success() {
            bail!("Connection attempted, but authentication didn't succeed.");
        };

        let channel = handle.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;

        let sess = SftpSession::new(channel.into_stream()).await?;

        Ok(Self { handle, sess })
    }

    pub fn sess(&mut self) -> &mut SftpSession {
        assert!(!self.handle.is_closed());
        &mut self.sess
    }
}

async fn read_private_key() -> Result<PrivateKeyWithHashAlg> {
    let pkey = fs::read("/home/tali/.ssh/id_ed25519").await?;
    let pkey = PrivateKey::from_openssh(pkey).with_context(|| "Failed reading private key.")?;

    // None is expected to be safe here because pkey is expected to be ed25519, for which this
    // argument does nothing.
    Ok(PrivateKeyWithHashAlg::new(Arc::new(pkey), None))
}

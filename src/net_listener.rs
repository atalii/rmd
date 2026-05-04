/// Watch the available network links, writing to a channel when something connects or disconnects.
use libc::{IFA_LOCAL, RTNLGRP_IPV4_ROUTE};
use netlink_bindings::{rt_route, utils::IpAddr};
use netlink_socket2::MulticastSocketRaw;
use tokio::sync::mpsc;

use anyhow::{Context, Result};

pub struct NetListener {
    mc_sock: MulticastSocketRaw,
}

#[derive(Debug)]
pub enum Event {
    Connection,
    Disconnection,
}

impl NetListener {
    pub async fn new() -> Result<Self> {
        let mut mc_sock = MulticastSocketRaw::new(rt_route::PROTONUM)?;
        mc_sock.listen(RTNLGRP_IPV4_ROUTE)?;

        Ok(Self { mc_sock })
    }

    /// Write an [Event] to the provided channel when a change in available devices is reported by
    /// the kernel.
    pub async fn listen(&mut self, ch: mpsc::Sender<Event>) -> Result<()> {
        loop {
            let msg = self.mc_sock.recv().await;
            match msg {
                Ok((header, buf)) => {
                    let buf = buf.to_owned();
                    if let Some(e) = self.filter_msg(header.message_type, buf) {
                        ch.send(e)
                            .await
                            .with_context(|| "Couldn't send network event to channel.")?;
                    }
                }
                Err(e) => log::error!("Failed to recv message from multicast: {e}"),
            }
        }
    }

    pub fn filter_msg(&mut self, message_type: u16, buf: Vec<u8>) -> Option<Event> {
        match message_type {
            // DELROUTE and NEWROUTE appear to trigger only when the other should. I don't know
            // why.
            libc::RTM_DELROUTE if concerns_remarkable(&buf) => Some(Event::Connection),
            libc::RTM_NEWROUTE if concerns_remarkable(&buf) => Some(Event::Disconnection),
            libc::RTM_NEWROUTE | libc::RTM_DELROUTE => {
                log::debug!("Route addition/deletion not pertaining to RM detected.");
                None
            }
            x => {
                log::debug!("Ignoring message of type: {x}");
                None
            }
        }
    }
}

/// Given the data of an RTM_{NEW,DEL}ROUTE message, return true iff it concerns something
/// happening in a subent consistent with what a Remarkable would provide.
fn concerns_remarkable(buf: &[u8]) -> bool {
    // Internally, decode_request calls unwrap_or_default. The default value happens to
    // be ignored here, so any invalid data is going to be ignored as we'd like.
    let (header, attrs) = rt_route::OpGetrouteDump::decode_request(buf);
    if header.rtm_type != IFA_LOCAL as u8 {
        log::debug!("Ignoring a non-local route.");
        return false;
    }

    let dst = match attrs.get_dst() {
        Ok(dst) => dst,
        Err(e) => {
            log::warn!("Couldn't parse a dst out of a kernel message: {e}");
            return false;
        }
    };

    match dst {
        IpAddr::V6(_) => false,
        IpAddr::V4(addr) => {
            let addr = addr.octets();
            addr[..3] == [10, 11, 99]
        }
    }
}

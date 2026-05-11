use anyhow::{Result, bail};
use rmd::Monitor;
use rmd::net_listener::{self, NetListener};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let rh = tokio::runtime::Handle::current();
    let mut nl = NetListener::new(rh)?;

    let (tx, rx) = mpsc::channel(1);
    let h = std::thread::spawn(move || nl.listen(tx));

    tokio::select! {
        Err(e) = tokio::task::spawn_blocking(move || h.join()) => Err(anyhow::Error::new(e)).flatten(),
        Err(e) = monitor_conns(rx) => Err(e),
    }?;

    bail!("Unexpected termination without error.");
}

async fn monitor_conns(mut rx: mpsc::Receiver<net_listener::Event>) -> Result<()> {
    let monitor = Monitor::new();

    while let Some(msg) = rx.recv().await {
        log::debug!("Monitor task rx'd: {msg:?}");

        match msg {
            net_listener::Event::Connection => {
                if let Err(e) = monitor.start_connection().await {
                    log::error!("Couldn't connect to tablet: {e}");
                }
            }
            net_listener::Event::Disconnection => {
                if let Err(e) = monitor.disconnect().await {
                    log::error!("Couldn't disconnect to tablet: {e}");
                }
            }
        };
    }

    bail!("Network monitor channel was closed.")
}

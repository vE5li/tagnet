use std::{io::Read, net::IpAddr, path::PathBuf};

use clap::{Parser, Subcommand};
use futures_util::SinkExt;
use tagnet_core::{
    FileId,
    state::{Change, Frame},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, clap::Args)]
struct ConnectArgs {
    #[arg(long, default_value = "127.0.0.1")]
    host: IpAddr,
    #[arg(long, default_value_t = 3468)]
    port: u16,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Upload {
        #[command(flatten)]
        connect: ConnectArgs,
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let arguments = Arguments::parse();

    match arguments.command {
        Commands::Upload { connect, path } => {
            let url = format!("ws://{}:{}", connect.host, connect.port);
            let (mut ws_stream, _) = connect_async(&url)
                .await
                .unwrap_or_else(|e| panic!("Failed to connect to {url}: {e}"));
            println!("WebSocket handshake has been successfully completed");
            let mut file = std::fs::File::open(&path).expect("File doesn't exist");
            let mut content = Vec::new();
            file.read_to_end(&mut content).expect("Failed to read file");

            let content_hash = blake3::hash(&content).to_hex().to_string();

            let change = Change::FileAdded {
                file_id: FileId::new(),
                // FIX: Decide when to use the full path pased on command line arguments.
                path: path.file_name().unwrap().to_string_lossy().to_string(),
                content,
                content_hash,
                tags: vec![],
            };

            // NOTE: This is currently broken end-to-end. The daemon now
            // expects a peer handshake (a single text frame containing the
            // sender's public_key) before any Frame, and the CLI has never
            // sent one. Wrapping in Frame::Change matches the post-handshake
            // wire format, but the daemon will reject the connection before
            // it gets here. TODO: teach the CLI to do the handshake (and
            // either configure the CLI as a peer or add a dedicated CLI
            // protocol).
            let frame = Frame::Change(change);
            let text = serde_json::to_string(&frame).expect("Failed to serialize");
            ws_stream
                .send(Message::text(text))
                .await
                .expect("Failed to send file");
        }
    }
}

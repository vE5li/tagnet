use std::{io::Read, path::PathBuf};

use clap::{Parser, Subcommand, ValueEnum, command};
use futures_util::SinkExt;
use tagnet_core::{FileId, state::Change};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Subtags {
    Include,
    Exclude,
}

// impl From<Subtags> for SubtagRule {
//     fn from(val: Subtags) -> Self {
//         match val {
//             Subtags::Include => SubtagRule::Include,
//             Subtags::Exclude => SubtagRule::Exclude,
//         }
//     }
// }

impl std::fmt::Display for Subtags {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Subtags::Include => write!(formatter, "include"),
            Subtags::Exclude => write!(formatter, "exclude"),
        }
    }
}

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Upload { path: PathBuf },
    // FilesForTag {
    //     tag_id: i64,
    //     #[arg(short, long, default_value_t=Subtags::Include)]
    //     subtags: Subtags,
    // },
    // TagsForTag {
    //     tag_id: i64,
    //     #[arg(short, long, default_value_t=Subtags::Include)]
    //     subtags: Subtags,
    // },
}

#[tokio::main]
async fn main() {
    let arguments = Arguments::parse();

    let (mut ws_stream, _) = connect_async("ws://127.0.0.1:9001")
        .await
        .expect("Failed to connect");
    println!("WebSocket handshake has been successfully completed");

    match arguments.command {
        Commands::Upload { path } => {
            let mut file = std::fs::File::open(&path).expect("File doesn't exist");
            let mut content = Vec::new();
            file.read_to_end(&mut content).expect("Failed to read file");

            let change = Change::FileAdded {
                file_id: FileId::new(),
                // FIX: Decide when to use the full path pased on command line arguments.
                path: path.file_name().unwrap().to_string_lossy().to_string(),
                content,
            };

            let text = serde_json::to_string(&change).expect("Failed to serialize");
            ws_stream
                .send(Message::text(text))
                .await
                .expect("Failed to send file");
        }
    }
}

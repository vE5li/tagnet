use clap::{command, Parser, Subcommand, ValueEnum};
use tagnet_core::{initialize, SubtagRule};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Subtags {
    Include,
    Exclude,
}

impl From<Subtags> for SubtagRule {
    fn from(val: Subtags) -> Self {
        match val {
            Subtags::Include => SubtagRule::Include,
            Subtags::Exclude => SubtagRule::Exclude,
        }
    }
}

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
    AddTag {
        name: String,
    },
    TagFile {
        file_id: i64,
        tag_id: i64,
    },
    TagTag {
        subtag_id: i64,
        tag_id: i64,
    },
    UntagFile {
        file_id: i64,
        tag_id: i64,
    },
    UntagTag {
        subtag_id: i64,
        tag_id: i64,
    },
    FilesForTag {
        tag_id: i64,
        #[arg(short, long, default_value_t=Subtags::Include)]
        subtags: Subtags,
    },
    TagsForTag {
        tag_id: i64,
        #[arg(short, long, default_value_t=Subtags::Include)]
        subtags: Subtags,
    },
    Sync,
}

fn main() {
    let arguments = Arguments::parse();

    let handle = initialize("test.db").unwrap();

    match arguments.command {
        Commands::AddTag { name } => {
            let tag_id = handle.add_tag(name, "#ff00ff").unwrap();
            println!("Created tag with ID {tag_id:?}");
        }
        Commands::TagFile { tag_id, file_id } => {
            if let Err(error) = handle.tag_file(tag_id.into(), file_id.into()) {
                println!("Invalid action: {error:?}");
            }
        }
        Commands::TagTag { tag_id, subtag_id } => {
            if let Err(error) = handle.tag_tag(tag_id.into(), subtag_id.into()) {
                println!("Invalid action: {error:?}");
            }
        }
        Commands::UntagFile { tag_id, file_id } => {
            if let Err(error) = handle.untag_file(tag_id.into(), file_id.into()) {
                println!("Invalid action: {error:?}");
            }
        }
        Commands::UntagTag { tag_id, subtag_id } => {
            if let Err(error) = handle.untag_tag(tag_id.into(), subtag_id.into()) {
                println!("Invalid action: {error:?}");
            }
        }
        Commands::FilesForTag { tag_id, subtags } => {
            let file_ids = handle
                .file_ids_for_tag(tag_id.into(), subtags.into())
                .unwrap();

            file_ids
                .into_iter()
                .map(|file_id| handle.file_from_id(file_id).unwrap())
                .for_each(|file_path| println!("> {file_path:?}"));
        }
        Commands::TagsForTag { tag_id, subtags } => {
            let tag_ids = handle
                .subtag_ids_for_tag(tag_id.into(), subtags.into())
                .unwrap();

            tag_ids
                .into_iter()
                .map(|tag_id| handle.tag_from_id(tag_id).unwrap())
                .for_each(|tag_name| println!("> {tag_name:?}"));
        }
        Commands::Sync => {
            tagnet_core::nextcloud::sync(&handle);
        }
    }

    println!("\n\n-- DEBUG --");
    handle.show_files().unwrap();
    handle.show_tags().unwrap();
    handle.show_entries().unwrap();
    handle.show_previews().unwrap();
}

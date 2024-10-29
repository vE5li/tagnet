use clap::{command, Parser, Subcommand, ValueEnum};
use tagnet_core::{initialize, FileId, SubtagRule, TagId};

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
}

fn main() {
    let arguments = Arguments::parse();

    let handle = initialize("test.db").unwrap();

    match arguments.command {
        Commands::AddTag { name } => {
            let tag_id = handle.add_tag(name).unwrap();
            println!("Created tag with ID {tag_id:?}");
        }
        Commands::TagFile { tag_id, file_id } => {
            if let Err(error) = handle.tag_file(TagId::from_raw(tag_id), FileId::from_raw(file_id))
            {
                println!("Invalid action: {error:?}");
            }
        }
        Commands::TagTag { tag_id, subtag_id } => {
            if let Err(error) = handle.tag_tag(TagId::from_raw(tag_id), TagId::from_raw(subtag_id))
            {
                println!("Invalid action: {error:?}");
            }
        }
        Commands::UntagFile { tag_id, file_id } => {
            if let Err(error) =
                handle.untag_file(TagId::from_raw(tag_id), FileId::from_raw(file_id))
            {
                println!("Invalid action: {error:?}");
            }
        }
        Commands::UntagTag { tag_id, subtag_id } => {
            if let Err(error) =
                handle.untag_tag(TagId::from_raw(tag_id), TagId::from_raw(subtag_id))
            {
                println!("Invalid action: {error:?}");
            }
        }
        Commands::FilesForTag { tag_id, subtags } => {
            let file_ids = handle
                .files_for_tag(TagId::from_raw(tag_id), subtags.into())
                .unwrap();

            file_ids
                .into_iter()
                .map(|file_id| handle.file_path(file_id).unwrap())
                .for_each(|file_path| println!("> {file_path:?}"));
        }
        Commands::TagsForTag { tag_id, subtags } => {
            let tag_ids = handle
                .tags_for_tag(TagId::from_raw(tag_id), subtags.into())
                .unwrap();

            tag_ids
                .into_iter()
                .map(|tag_id| handle.tag_name(tag_id).unwrap())
                .for_each(|tag_name| println!("> {tag_name:?}"));
        }
    }

    handle.add_file("some_test/more.rs");
    handle.add_file("some_test/secods.rs");
    handle.add_file("some_test/ree.rs");
    handle.add_file("foobar.rs");
    handle.add_file("other.rs");

    println!("\n\n-- DEBUG --");
    handle.show_files().unwrap();
    handle.show_tags().unwrap();
    handle.show_entries().unwrap();
}

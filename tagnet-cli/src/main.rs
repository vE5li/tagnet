use clap::{command, Parser, Subcommand};
use tagnet_core::{initialize, FileId, TagId};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Arguments {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    AddTag { name: String },
    TagFile { file_id: i64, tag_id: i64 },
    TagTag { other_tag_id: i64, tag_id: i64 },
    Filter { tag_id: i64 },
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
            handle
                .tag_file(TagId::from_raw(tag_id), FileId::from_raw(file_id))
                .unwrap();
        }
        Commands::TagTag {
            tag_id,
            other_tag_id,
        } => {
            handle
                .tag_tag(TagId::from_raw(tag_id), TagId::from_raw(other_tag_id))
                .unwrap();
        }
        Commands::Filter { tag_id } => {
            let file_ids = handle.files_for_tag(TagId::from_raw(tag_id)).unwrap();

            file_ids
                .into_iter()
                .map(|file_id| handle.file_path(file_id).unwrap())
                .for_each(|file_path| println!("  > {file_path:?}"));
        }
    }

    // handle.add_file("some_test/more.rs");
    // handle.add_file("some_test/secods.rs");
    // handle.add_file("some_test/ree.rs");
    // handle.add_file("foobar.rs");
    // handle.add_file("other.rs");

    println!("\n\n-- DEBUG --");
    handle.show_files().unwrap();
    handle.show_tags().unwrap();
    handle.show_entries().unwrap();
}

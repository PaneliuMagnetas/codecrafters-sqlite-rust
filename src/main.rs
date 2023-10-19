use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;

            let mut sql_schema_page = vec![0; page_size - 100];
            file.read_exact(&mut sql_schema_page)?;

            let btree_header = &sql_schema_page[0..8];
            let btree_cells_on_page =
                u16::from_be_bytes([btree_header[3], btree_header[4]]) as usize;

            println!("database page size: {}", page_size);
            println!("number of tables: {}", btree_cells_on_page);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

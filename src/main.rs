extern crate cdb_rs;
use std::env;
use std::io;

use cdb_rs::cdb;

fn dump(filename: &str) -> io::Result<()> {
    let db = cdb::CDB::load(filename)?;
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    db.dump(&mut handle)
}

fn main() {
    // let args: Vec<String> = env::args().collect();

    // if args.len() < 2 {
    //     eprintln!("Usage: dump /path/to/data.cdb");
    //     std::process::exit(1);
    // }

    // let filename = &args[1];

    let filename = "dict.cdb";

    std::process::exit(
        match dump(filename) {
            Ok(_) => 0,
            Err(err) => {
                eprintln!("error: {:?}", err);
                1
            }
        }
    );
}

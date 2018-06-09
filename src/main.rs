extern crate cdb_rs;
use std::env;
use std::io;
use std::path;
use std::time::Duration;

use cdb_rs::cdb;

fn dump(filename: &str) -> io::Result<()> {
    let db = cdb::CDB::load(filename)?;
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    db.dump(&mut handle)
}

fn dur2sec(d: &Duration) -> f64  {
    d.as_secs() as f64 + (d.subsec_nanos() as f64 * 1e-9)
}

fn randoread(filename: &str, iters: u64) -> io::Result<()> {
    let db = cdb::CDB::load(filename)?;
    let d = cdb::randoread::run(&db, iters)?;
    let d2f = dur2sec(&d);
    let rate = iters as f64 / d2f;

    eprintln!(
        "{} iters in {} sec, {} op/sec", iters, d2f, rate
    );
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let progname =
        path::Path::new(&args[0])
            .file_name()
            .and_then(|fname| fname.to_str())
            .unwrap_or("cdbrs");

    if args.len() < 2 {
        eprintln!("Usage: {} /path/to/data.cdb", progname);
        std::process::exit(1);
    }

    let filename = &args[1];

    std::process::exit(
        match randoread(filename, 100_000_000) {
            Ok(_) => 0,
            Err(err) => {
                eprintln!("error: {:?}", err);
                1
            }
        }
    );
}

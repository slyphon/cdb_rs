extern crate cdb_rs;
#[macro_use]
extern crate log;
extern crate env_logger;

extern crate cdb as other_cdb;

use std::env;
use std::io;
use std::path;
use std::time::Duration;
use std::fs;

use cdb_rs::cdb;

#[allow(dead_code)]
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

    let fp = fs::File::open(filename)?;
    let mut odb = other_cdb::CDB::new(fp)?;

    let d = cdb::randoread::run(&db, &mut odb, iters)?;
    let d2f = dur2sec(&d);
    let rate = iters as f64 / d2f;

    info!(
        "{} iters in {} sec, {} op/sec", iters, d2f, rate
    );
    Ok(())
}

fn main() {
    env_logger::init();
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

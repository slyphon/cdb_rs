use rand;
use rand::Rng;
use std::io;
use std::time::{Instant,Duration};

pub fn run(db: &super::CDB, iters: u64) -> io::Result<Duration> {
    let keys = db.sample_keys(0.3, 100000);
    let mut rng = rand::thread_rng();

    eprintln!("starting test");
    let start = Instant::now();

    for _ in 0..iters {
        match rng.choose(&keys) {
            Some(k) => db.get(k),
            None => continue
        };
    }
    Ok(start.elapsed())
}

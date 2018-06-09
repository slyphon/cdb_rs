use rand;
use rand::Rng;
use std::io;

pub fn run(db: &super::CDB, iters: u64) -> io::Result<()> {
    let keys = db.keys();
    let mut rng = rand::thread_rng();

    for _ in 0..iters {
        match rng.choose(&keys) {
            Some(k) => db.get(k),
            None => continue
        };
    }

    Ok(())
}

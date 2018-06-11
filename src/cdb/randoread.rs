use bytes::Bytes;
use rand;
use rand::{thread_rng, Rng};
use std::io;
use std::time::{Instant,Duration};

pub fn run(db: &super::CDB, iters: u64) -> io::Result<Duration> {
    let mut rng = thread_rng();

    let keys = {
        let mut ks: Vec<Bytes> =
            db.kvs_iter()
                .filter(|_| rng.gen::<f32>() < 0.3)
                .take(100_000)
                .map(|kv| kv.k)
                .collect();

        ks.shrink_to_fit();
        ks
    };

    eprintln!("starting test using {} sampled keys", keys.len());
    let start = Instant::now();

    for _ in 0..iters {
        match rng.choose(&keys) {
            Some(k) => db.get(k),
            None => continue
        };
    }

    Ok(start.elapsed())
}

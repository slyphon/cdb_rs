use bytes::Bytes;
use env_logger;
use rand::{thread_rng, Rng};
use std::io;
use std::time::{Duration, Instant};

pub fn run(db: &super::CDB, iters: u64) -> io::Result<Duration> {
    let mut rng = thread_rng();

    let keys = {
        let mut ks: Vec<Bytes> = db.kvs_iter()
            .filter(|_| rng.gen::<f32>() < 0.3)
            .take(100_000)
            .map(|kv| kv.k)
            .collect();

        ks.shrink_to_fit();
        ks
    };

    eprintln!("starting test using {} sampled keys", keys.len());
    let start = Instant::now();

    let mut hit = 0;
    let mut miss = 0;

    for _ in 0..iters {
        match rng.choose(&keys) {
            Some(k) => {
                if db.get((*k).as_ref()).is_some() {
                    hit += 1
                } else {
                    miss += 1
                }
            }
            None => continue,
        };
    }

    let hitrate = (hit as f64 / iters as f64) * 100.0;

    debug!(
        "hit: {}, miss: {}, ratio: {ratio:.*}%",
        hit,
        miss,
        3,
        ratio = hitrate
    );

    Ok(start.elapsed())
}

extern crate cdb_rs;
extern crate rand;

use rand::{thread_rng,Rng};
use rand::distributions::Alphanumeric;
use std::io;
use std::io::Write;


fn alpha(min: usize, max: usize) -> String {
    thread_rng().sample_iter(&Alphanumeric).take(
        thread_rng().gen_range(min, max)
    ).collect()
}

const MAX_KEY_SIZE: usize = 256;
const MAX_VAL_SIZE: usize = 1024 * 1024;

fn main() {
    loop {
        let k: String = alpha(10, MAX_KEY_SIZE);
        let v: String = alpha(16, MAX_VAL_SIZE);

        match writeln!(io::stdout(), "+{},{}:{}->{}", k.len(), v.len(), k, v) {
            Ok(_) => continue,
            Err(_) => break,
        }
    }
}

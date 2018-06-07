pub const STARTING_HASH: u32 = 5381;

pub fn djb_hash(bytes: &[u8]) -> u32 {
    let mut h = STARTING_HASH;
    for b in bytes {
        h = ((h << 5) + h) ^ ((*b as u32) & 0xffffffff)
    }
    h
 }

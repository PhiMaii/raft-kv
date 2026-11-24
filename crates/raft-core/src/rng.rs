//! A small, seedable pseudo-random number generator.
//!
//! The core only needs randomness to spread out election timeouts, so a tiny
//! self-contained generator is enough and keeps `raft-core` free of
//! dependencies. What matters is that the same seed always produces the same
//! sequence, which keeps simulations reproducible.

/// `SplitMix64` (Steele, Lea and Flood, 2014).
#[derive(Clone, Debug)]
pub(crate) struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `low..high`. The slight modulo bias is irrelevant for
    /// election timeouts.
    pub(crate) fn gen_range(&mut self, low: u64, high: u64) -> u64 {
        debug_assert!(low < high, "empty range");
        low + self.next_u64() % (high - low)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_gives_same_sequence() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn gen_range_stays_in_bounds_and_covers_the_range() {
        let mut rng = SplitMix64::new(7);
        let mut seen = [false; 10];
        for _ in 0..1000 {
            let value = rng.gen_range(10, 20);
            assert!((10..20).contains(&value));
            seen[usize::try_from(value - 10).unwrap()] = true;
        }
        assert!(seen.iter().all(|&s| s));
    }
}

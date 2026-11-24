//! The simulator's single source of randomness.
//!
//! Every random decision in a run (fault injection, message delays, delivery
//! order, workload) is drawn from one generator seeded with the run's seed.
//! That is what makes a failing seed reproduce exactly.

/// A probability, stored as parts per million so that no floating-point
/// arithmetic is involved in any random decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Chance(u32);

impl Chance {
    /// Never happens.
    pub const NEVER: Self = Self(0);

    /// `n` in a million.
    #[must_use]
    pub const fn per_million(n: u32) -> Self {
        Self(n)
    }

    /// `n` percent.
    #[must_use]
    pub const fn percent(n: u32) -> Self {
        Self(n * 10_000)
    }
}

/// `SplitMix64`: small, fast, and fully determined by its seed.
#[derive(Clone, Debug)]
pub(crate) struct SimRng {
    state: u64,
}

impl SimRng {
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

    /// A value in `0..n`.
    pub(crate) fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "empty range");
        self.next_u64() % n
    }

    /// A value in `low..=high`.
    pub(crate) fn between(&mut self, low: u64, high: u64) -> u64 {
        assert!(low <= high, "empty range");
        low + self.below(high - low + 1)
    }

    /// An index in `0..len`.
    pub(crate) fn index(&mut self, len: usize) -> usize {
        let len = u64::try_from(len).expect("length fits in u64");
        usize::try_from(self.below(len)).expect("index fits in usize")
    }

    /// Whether an event with the given probability happens.
    pub(crate) fn chance(&mut self, chance: Chance) -> bool {
        self.below(1_000_000) < u64::from(chance.0)
    }

    /// Shuffles `items` in place (Fisher–Yates).
    pub(crate) fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = self.index(i + 1);
            items.swap(i, j);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_decisions() {
        let mut a = SimRng::new(3);
        let mut b = SimRng::new(3);
        for _ in 0..100 {
            assert_eq!(a.between(5, 9), b.between(5, 9));
            assert_eq!(a.chance(Chance::percent(50)), b.chance(Chance::percent(50)));
        }
    }

    #[test]
    fn chance_extremes() {
        let mut rng = SimRng::new(1);
        for _ in 0..1000 {
            assert!(!rng.chance(Chance::NEVER));
            assert!(rng.chance(Chance::percent(100)));
        }
    }

    #[test]
    fn chance_is_roughly_calibrated() {
        let mut rng = SimRng::new(2);
        let hits = (0..100_000)
            .filter(|_| rng.chance(Chance::percent(10)))
            .count();
        assert!((9_000..11_000).contains(&hits), "{hits} hits");
    }

    #[test]
    fn shuffle_is_a_permutation() {
        let mut rng = SimRng::new(4);
        let mut items: Vec<u32> = (0..50).collect();
        rng.shuffle(&mut items);
        let mut sorted = items.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..50).collect::<Vec<_>>());
        assert_ne!(items, sorted, "50 items should not stay in order");
    }
}

//! Seeded randomness: a stream ([`Rng`]) and stateless draws ([`draw`]).

/// SplitMix64: small, fast and fully specified here, so streams never change with a dependency upgrade.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`, with 53 random bits.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Uniform in `0..n` (`n > 0`), by multiply-shift; the bias is below 2⁻⁶⁴·n.
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// True with probability `p`. Draws even when `p` is 0 or 1, so changing a probability does not shift the stream.
    pub fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }

    /// Standard normal, by Box–Muller (one of the pair).
    pub fn normal(&mut self) -> f64 {
        let u = 1.0 - self.unit();
        let v = self.unit();
        (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * v).cos()
    }
}

/// A uniform draw in [0, 1) that depends only on its inputs (SplitMix64 over a mix of them).
pub fn draw(seed: u64, sequence: u64, attempt: u32, purpose: u64) -> f64 {
    fn mix(mut z: u64) -> u64 {
        z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    let x = mix(mix(mix(seed) ^ sequence) ^ (u64::from(attempt) << 8 | purpose));
    (x >> 11) as f64 / (1_u64 << 53) as f64
}

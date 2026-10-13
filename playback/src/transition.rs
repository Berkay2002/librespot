//! Hooks for overlapping the end of one track with the start of the next,
//! as a DJ mix does. The player decodes the preloaded next track alongside
//! the current one and hands both signals to the application, which shapes
//! and sums them. When the overlap ends, the next track carries on from
//! where the overlap left it.

use crate::core::SpotifyUri;

/// Where an overlap sits in each track.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransitionPlan {
    /// Position in the current track where the next one comes in.
    pub start_a_ms: u32,
    /// Position in the next track it starts from.
    pub start_b_ms: u32,
    pub duration_ms: u32,
}

/// Combines the two tracks during one overlap.
pub trait TransitionMix: Send {
    /// `a` holds the outgoing track and receives the result; `b` holds the
    /// same stretch of the incoming track. Both are interleaved stereo after
    /// each track's normalisation. `position` is the first frame's offset
    /// into the overlap of `length` frames. Runs on the player thread, so it
    /// must not block.
    fn mix(&mut self, a: &mut [f64], b: &[f64], position: u64, length: u64);
}

pub trait Transitions: Send + Sync {
    /// How long before the end of a track to preload the next one. An
    /// overlap can only start once the next track is preloaded.
    fn preload_before_end_ms(&self) -> u32 {
        30_000
    }

    /// The overlap from `a` into `b`, if there is one. Called for every
    /// packet while the next track is preloaded, so it must be cheap.
    fn plan(
        &self,
        a: &SpotifyUri,
        a_duration_ms: u32,
        b: &SpotifyUri,
        b_duration_ms: u32,
    ) -> Option<TransitionPlan>;

    /// The mix for an overlap that starts now.
    fn begin(
        &self,
        a: &SpotifyUri,
        b: &SpotifyUri,
        plan: &TransitionPlan,
    ) -> Box<dyn TransitionMix>;
}

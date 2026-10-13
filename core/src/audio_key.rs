use std::{collections::HashMap, io::Write, time::Duration};

use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use bytes::Bytes;
use thiserror::Error;
use tokio::sync::oneshot;

use crate::{Error, FileId, SpotifyId, packet::PacketType, util::SeqGenerator};

#[derive(Debug, Hash, PartialEq, Eq, Copy, Clone)]
pub struct AudioKey(pub [u8; 16]);

#[derive(Debug, Error)]
pub enum AudioKeyError {
    #[error("audio key error")]
    AesKey,
    #[error("other end of channel disconnected")]
    Channel,
    #[error("unexpected packet type {0}")]
    Packet(u8),
    #[error("sequence {0} not pending")]
    Sequence(u32),
    #[error("audio key response timeout")]
    Timeout,
}

impl From<AudioKeyError> for Error {
    fn from(err: AudioKeyError) -> Self {
        match err {
            AudioKeyError::AesKey => Error::unavailable(err),
            AudioKeyError::Channel => Error::aborted(err),
            AudioKeyError::Sequence(_) => Error::aborted(err),
            AudioKeyError::Packet(_) => Error::unimplemented(err),
            AudioKeyError::Timeout => Error::aborted(err),
        }
    }
}

/// In-memory cache of audio keys, so a repeated request skips the round trip.
/// Memory only: keys are never written next to the encrypted files.
#[derive(Default)]
struct KeyCache(HashMap<(SpotifyId, FileId), AudioKey>);

impl KeyCache {
    const CAPACITY: usize = 1024;

    fn get(&self, track: SpotifyId, file: FileId) -> Option<AudioKey> {
        self.0.get(&(track, file)).copied()
    }

    /// Stores successful results only. Clears everything once the cap is reached.
    fn record(&mut self, track: SpotifyId, file: FileId, result: &Result<AudioKey, Error>) {
        if let Ok(key) = result {
            if self.0.len() >= Self::CAPACITY {
                self.0.clear();
            }
            self.0.insert((track, file), *key);
        }
    }
}

component! {
    AudioKeyManager : AudioKeyManagerInner {
        sequence: SeqGenerator<u32> = SeqGenerator::new(0),
        pending: HashMap<u32, oneshot::Sender<Result<AudioKey, Error>>> = HashMap::new(),
        keys: KeyCache = KeyCache::default(),
    }
}

impl AudioKeyManager {
    pub(crate) fn dispatch(&self, cmd: PacketType, mut data: Bytes) -> Result<(), Error> {
        let seq = BigEndian::read_u32(data.split_to(4).as_ref());

        let sender = self
            .lock(|inner| inner.pending.remove(&seq))
            .ok_or(AudioKeyError::Sequence(seq))?;

        match cmd {
            PacketType::AesKey => {
                let mut key = [0u8; 16];
                key.copy_from_slice(data.as_ref());
                sender
                    .send(Ok(AudioKey(key)))
                    .map_err(|_| AudioKeyError::Channel)?
            }
            PacketType::AesKeyError => {
                error!(
                    "error audio key {:x} {:x}",
                    data.as_ref()[0],
                    data.as_ref()[1]
                );
                sender
                    .send(Err(AudioKeyError::AesKey.into()))
                    .map_err(|_| AudioKeyError::Channel)?
            }
            _ => {
                trace!("Did not expect {cmd:?} AES key packet with data {data:#?}");
                return Err(AudioKeyError::Packet(cmd as u8).into());
            }
        }

        Ok(())
    }

    pub async fn request(&self, track: SpotifyId, file: FileId) -> Result<AudioKey, Error> {
        if let Some(key) = self.lock(|inner| inner.keys.get(track, file)) {
            return Ok(key);
        }

        let result = self.request_uncached(track, file).await;
        self.lock(|inner| inner.keys.record(track, file, &result));
        result
    }

    async fn request_uncached(&self, track: SpotifyId, file: FileId) -> Result<AudioKey, Error> {
        let (tx, rx) = oneshot::channel();

        let seq = self.lock(move |inner| {
            let seq = inner.sequence.get();
            inner.pending.insert(seq, tx);
            seq
        });

        self.send_key_request(seq, track, file)?;
        const KEY_RESPONSE_TIMEOUT: Duration = Duration::from_millis(1500);
        match tokio::time::timeout(KEY_RESPONSE_TIMEOUT, rx).await {
            Err(_) => {
                error!("Audio key response timeout");
                Err(AudioKeyError::Timeout.into())
            }
            Ok(k) => k?,
        }
    }

    fn send_key_request(&self, seq: u32, track: SpotifyId, file: FileId) -> Result<(), Error> {
        let mut data: Vec<u8> = Vec::new();
        data.write_all(&file.0)?;
        data.write_all(&track.to_raw())?;
        data.write_u32::<BigEndian>(seq)?;
        data.write_u16::<BigEndian>(0x0000)?;

        self.session().send_packet(PacketType::RequestKey, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(n: u8) -> (SpotifyId, FileId) {
        let mut track = [0u8; 16];
        track[15] = n;
        let mut file = [0u8; 20];
        file[19] = n;
        (
            SpotifyId::from_raw(&track).unwrap(),
            FileId::from_raw(&file),
        )
    }

    #[test]
    fn hit_and_miss() {
        let mut cache = KeyCache::default();
        let (track, file) = ids(1);
        assert_eq!(cache.get(track, file), None);
        cache.record(track, file, &Ok(AudioKey([7; 16])));
        assert_eq!(cache.get(track, file), Some(AudioKey([7; 16])));
        let (other_track, other_file) = ids(2);
        assert_eq!(cache.get(other_track, file), None);
        assert_eq!(cache.get(track, other_file), None);
    }

    #[test]
    fn error_is_not_stored() {
        let mut cache = KeyCache::default();
        let (track, file) = ids(1);
        cache.record(track, file, &Err(AudioKeyError::AesKey.into()));
        assert_eq!(cache.get(track, file), None);
    }

    #[test]
    fn clears_at_capacity() {
        let mut cache = KeyCache::default();
        for n in 0..KeyCache::CAPACITY {
            let mut track = [0u8; 16];
            track[14..].copy_from_slice(&(n as u16).to_be_bytes());
            let id = SpotifyId::from_raw(&track).unwrap();
            cache.record(id, FileId([0; 20]), &Ok(AudioKey([1; 16])));
        }
        assert_eq!(cache.0.len(), KeyCache::CAPACITY);

        let (track, file) = ids(255);
        cache.record(track, file, &Ok(AudioKey([2; 16])));
        assert_eq!(cache.0.len(), 1);
        assert_eq!(cache.get(track, file), Some(AudioKey([2; 16])));
    }
}

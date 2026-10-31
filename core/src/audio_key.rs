use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    sync::{LazyLock, Mutex},
    time::Duration,
};

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

/// How long one request waits for its key.
const KEY_RESPONSE_TIMEOUT: Duration = Duration::from_millis(1500);
/// Requests made for a key before it is given up as timed out.
const KEY_REQUEST_ATTEMPTS: u32 = 3;
/// Keys kept for files loaded again (see [`KEYS`]).
const KEPT_KEYS: usize = 1024;

/// The keys given, by file, the most recent last.
#[derive(Default)]
struct Keys {
    keys: HashMap<FileId, AudioKey>,
    order: VecDeque<FileId>,
}

impl Keys {
    fn get(&self, file: FileId) -> Option<AudioKey> {
        self.keys.get(&file).copied()
    }

    fn keep(&mut self, file: FileId, key: AudioKey) {
        if self.keys.insert(file, key).is_none() {
            self.order.push_back(file);
        }
        while self.order.len() > KEPT_KEYS {
            if let Some(oldest) = self.order.pop_front() {
                self.keys.remove(&oldest);
            }
        }
    }
}

/// The keys given while the program runs, across sessions. A file's key
/// never changes, so a track loaded again (a seek while it loads, a retry, a
/// reconnect, a prefetch and then the load) needn't ask for it again: each
/// request counts towards the ones Spotify refuses when they come too often.
static KEYS: LazyLock<Mutex<Keys>> = LazyLock::new(Mutex::default);

component! {
    AudioKeyManager : AudioKeyManagerInner {
        sequence: SeqGenerator<u32> = SeqGenerator::new(0),
        pending: HashMap<u32, oneshot::Sender<Result<AudioKey, Error>>> = HashMap::new(),
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

    /// Asks for a file's key. An answer that is late is asked for again, up to
    /// [`KEY_REQUEST_ATTEMPTS`] times in all: it is not a refusal, and the first
    /// request after the connection was idle can wait behind the network's
    /// retransmissions (as on a phone whose background network was cut).
    /// A key given before is not asked for again (see [`KEYS`]).
    pub async fn request(&self, track: SpotifyId, file: FileId) -> Result<AudioKey, Error> {
        if let Some(key) = KEYS.lock().ok().and_then(|keys| keys.get(file)) {
            return Ok(key);
        }
        for attempt in 1..=KEY_REQUEST_ATTEMPTS {
            let (tx, rx) = oneshot::channel();

            let seq = self.lock(move |inner| {
                let seq = inner.sequence.get();
                inner.pending.insert(seq, tx);
                seq
            });

            self.send_key_request(seq, track, file)?;
            match tokio::time::timeout(KEY_RESPONSE_TIMEOUT, rx).await {
                Ok(k) => {
                    let key = k??;
                    if let Ok(mut keys) = KEYS.lock() {
                        keys.keep(file, key);
                    }
                    return Ok(key);
                }
                Err(_) => {
                    // A late answer to this request is dropped by dispatch.
                    self.lock(|inner| inner.pending.remove(&seq));
                    if attempt < KEY_REQUEST_ATTEMPTS {
                        warn!("Audio key response timeout, asking again");
                    }
                }
            }
        }
        error!("Audio key response timeout");
        Err(AudioKeyError::Timeout.into())
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

    #[test]
    fn the_oldest_keys_make_way() {
        let file = |n: usize| FileId::from_raw(&n.to_be_bytes().repeat(3)[..20]);
        let mut keys = Keys::default();
        for n in 0..=KEPT_KEYS {
            keys.keep(file(n), AudioKey([n as u8; 16]));
        }
        assert_eq!(keys.get(file(0)), None);
        assert_eq!(keys.get(file(1)), Some(AudioKey([1; 16])));
        assert_eq!(
            keys.get(file(KEPT_KEYS)),
            Some(AudioKey([KEPT_KEYS as u8; 16]))
        );
        // Kept again, a key keeps its place.
        keys.keep(file(1), AudioKey([1; 16]));
        assert_eq!(keys.order.len(), KEPT_KEYS);
    }
}

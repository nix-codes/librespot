use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

use crate::{
    Metadata,
    artist::Artists,
    availability::Availabilities,
    copyright::Copyrights,
    external_id::ExternalIds,
    image::Images,
    request::RequestResult,
    restriction::Restrictions,
    sale_period::SalePeriods,
    track::Tracks,
    util::{impl_deref_wrapped, impl_try_from_repeated},
};

use librespot_core::{Error, Session, SpotifyUri, date::Date};

use librespot_protocol as protocol;
use protocol::metadata::Date as DateMessage;
use protocol::metadata::Disc as DiscMessage;
pub use protocol::metadata::album::Type as AlbumType;

#[derive(Debug, Clone)]
pub struct Album {
    pub id: SpotifyUri,
    pub name: String,
    pub artists: Artists,
    pub album_type: AlbumType,
    pub label: String,
    pub date: Date,
    /// When it came out, as precisely as Spotify says: "2020",
    /// "2020-03" or "2020-03-13". `date` fills what's missing with 1s.
    pub release_date: Option<String>,
    pub popularity: i32,
    pub covers: Images,
    pub external_ids: ExternalIds,
    pub discs: Discs,
    pub reviews: Vec<String>,
    pub copyrights: Copyrights,
    pub restrictions: Restrictions,
    pub related: Albums,
    pub sale_periods: SalePeriods,
    pub cover_group: Images,
    pub original_title: String,
    pub version_title: String,
    pub type_str: String,
    pub availability: Availabilities,
}

#[derive(Debug, Clone, Default)]
pub struct Albums(pub Vec<SpotifyUri>);

impl_deref_wrapped!(Albums, Vec<SpotifyUri>);

#[derive(Debug, Clone)]
pub struct Disc {
    pub number: i32,
    pub name: String,
    pub tracks: Tracks,
}

#[derive(Debug, Clone, Default)]
pub struct Discs(pub Vec<Disc>);

impl_deref_wrapped!(Discs, Vec<Disc>);

impl Album {
    pub fn tracks(&self) -> impl Iterator<Item = &SpotifyUri> {
        self.discs.iter().flat_map(|disc| disc.tracks.iter())
    }
}

#[async_trait]
impl Metadata for Album {
    type Message = protocol::metadata::Album;

    async fn request(session: &Session, album_uri: &SpotifyUri) -> RequestResult {
        let SpotifyUri::Album { .. } = album_uri else {
            return Err(Error::invalid_argument("album_uri"));
        };

        session.spclient().get_album_metadata(album_uri).await
    }

    fn parse(msg: &Self::Message, _: &SpotifyUri) -> Result<Self, Error> {
        Self::try_from(msg)
    }
}

/// A date as precisely as it's given: a year, a month or a day.
fn release_date(date: &DateMessage) -> Option<String> {
    if !date.has_year() {
        return None;
    }
    let mut text = format!("{:04}", date.year());
    if date.has_month() {
        text += &format!("-{:02}", date.month());
        if date.has_day() {
            text += &format!("-{:02}", date.day());
        }
    }
    Some(text)
}

impl TryFrom<&<Self as Metadata>::Message> for Album {
    type Error = librespot_core::Error;
    fn try_from(album: &<Self as Metadata>::Message) -> Result<Self, Self::Error> {
        Ok(Self {
            id: album.try_into()?,
            name: album.name().to_owned(),
            artists: album.artist.as_slice().try_into()?,
            album_type: album.type_(),
            label: album.label().to_owned(),
            date: album.date.get_or_default().try_into()?,
            release_date: album.date.as_ref().and_then(release_date),
            popularity: album.popularity(),
            covers: album.cover_group.get_or_default().into(),
            external_ids: album.external_id.as_slice().into(),
            discs: album.disc.as_slice().try_into()?,
            reviews: album.review.to_vec(),
            copyrights: album.copyright.as_slice().into(),
            restrictions: album.restriction.as_slice().into(),
            related: album.related.as_slice().try_into()?,
            sale_periods: album.sale_period.as_slice().try_into()?,
            cover_group: album.cover_group.image.as_slice().into(),
            original_title: album.original_title().to_owned(),
            version_title: album.version_title().to_owned(),
            type_str: album.type_str().to_owned(),
            availability: album.availability.as_slice().try_into()?,
        })
    }
}

impl_try_from_repeated!(<Album as Metadata>::Message, Albums);

impl TryFrom<&DiscMessage> for Disc {
    type Error = librespot_core::Error;
    fn try_from(disc: &DiscMessage) -> Result<Self, Self::Error> {
        Ok(Self {
            number: disc.number(),
            name: disc.name().to_owned(),
            tracks: disc.track.as_slice().try_into()?,
        })
    }
}

impl_try_from_repeated!(DiscMessage, Discs);

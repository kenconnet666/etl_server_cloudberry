use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

use crate::{CoreError, CoreResult};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PgLsn(u64);

impl PgLsn {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn saturating_sub(self, other: Self) -> u64 {
        self.0.saturating_sub(other.0)
    }
}

impl fmt::Display for PgLsn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:X}/{:08X}", self.0 >> 32, self.0 as u32)
    }
}

impl FromStr for PgLsn {
    type Err = CoreError;

    fn from_str(value: &str) -> CoreResult<Self> {
        let (high, low) = value
            .split_once('/')
            .ok_or_else(|| CoreError::InvalidLsn(value.to_owned()))?;
        if high.is_empty() || low.is_empty() || low.len() > 8 {
            return Err(CoreError::InvalidLsn(value.to_owned()));
        }
        let high =
            u32::from_str_radix(high, 16).map_err(|_| CoreError::InvalidLsn(value.to_owned()))?;
        let low =
            u32::from_str_radix(low, 16).map_err(|_| CoreError::InvalidLsn(value.to_owned()))?;
        Ok(Self((u64::from(high) << 32) | u64::from(low)))
    }
}

impl Serialize for PgLsn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PgLsn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_postgres_format() {
        let lsn: PgLsn = "16/B374D848".parse().expect("valid LSN");
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert_eq!(lsn.as_u64(), 0x0000_0016_B374_D848);
    }

    #[test]
    fn serializes_as_string_to_preserve_json_precision() {
        let lsn = PgLsn::new(u64::MAX);
        assert_eq!(
            serde_json::to_string(&lsn).unwrap(),
            r#""FFFFFFFF/FFFFFFFF""#
        );
    }

    #[test]
    fn rejects_malformed_values() {
        for invalid in ["", "0", "/0", "0/", "0/000000000", "g/0"] {
            assert!(invalid.parse::<PgLsn>().is_err(), "accepted {invalid}");
        }
    }
}

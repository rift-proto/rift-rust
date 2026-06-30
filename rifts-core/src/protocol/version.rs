//! # Protocol Version Constants and Negotiation -- Spec §5.2 & §25
//!
//! This module defines the version-related constants and negotiation
//! logic for the Rift/1 protocol.  The protocol uses a two-part version
//! number (`major.minor`) encoded as a single `u16`:
//!
//! ```text
//! encoded = major << 8 | minor
//! ```
//!
//! For example, Rift/1.0 encodes as `0x0100` (256 decimal).
//!
//! ## Versioning Rules (§25)
//!
//! * **Major version** bumps indicate backwards-incompatible wire-format
//!   changes.  A server MUST reject clients that advertise an
//!   unsupported major version.
//! * **Minor version** bumps indicate backwards-compatible additions
//!   (e.g. new optional fields).  A server SHOULD accept clients with
//!   an older minor version and simply ignore unknown fields.
//!
//! ## Negotiation Flow (§5.2)
//!
//! 1. The client sends its encoded version in the Hello frame.
//! 2. The server extracts the major version and checks it against
//!    [`SUPPORTED_MAJOR`].
//! 3. If the major version is not in the supported range, the server
//!    rejects the connection with a [`ProtocolVersionUnsupported`](super::error_code::ErrorCode::ProtocolVersionUnsupported)
//!    error.
//! 4. Otherwise the server accepts the client's major version via
//!    [`negotiate_major`].
//!
//! ## Constants
//!
//! | Constant            | Description                                  |
//! |---------------------|----------------------------------------------|
//! | `PROTOCOL_NAME`     | Wire protocol identifier (`"rift"`)          |
//! | `PROTOCOL_MAJOR`    | Current major version (1)                    |
//! | `PROTOCOL_MINOR`    | Current minor version (0)                    |
//! | `SUPPORTED_MAJOR`   | Range of major versions the server accepts   |

use std::ops::RangeInclusive;

/// Protocol name on the wire -- spec §5.2 (`hello.protocol`).
///
/// This is the identifier string the client sends in its Hello frame to
/// declare which protocol it speaks.  The server uses this value to
/// multiplex between different protocols on the same port.
pub const PROTOCOL_NAME: &str = "rift";

/// Current major version.
///
/// Bumped on backwards-incompatible wire-format changes.  A server that
/// implements Rift/`X`.* will reject clients that advertise a different
/// major version.
pub const PROTOCOL_MAJOR: u16 = 1;

/// Current minor version.
///
/// Bumped on backwards-compatible additions (new optional fields, new
/// frame types, etc.).  A server MUST accept clients with an older
/// minor version and ignore any fields it does not understand.
pub const PROTOCOL_MINOR: u16 = 0;

/// Supported major-version range offered to clients during hello.
///
/// The server will accept connections from clients whose major version
/// falls within this inclusive range.  For Rift/1 the range is
/// `1..=1`, meaning only major version 1 is accepted.
pub const SUPPORTED_MAJOR: RangeInclusive<u16> = 1..=1;

/// Encoded protocol version (`major << 8 | minor`).
///
/// This is the value transmitted in the `version` field of every Hello
/// frame and in the frame header.  Because it is a `const fn`, it is
/// evaluated at compile time and inlined at every call site.
///
/// ```rust
/// use rifts_core::protocol::version::encoded_version;
///
/// assert_eq!(encoded_version(), 0x0100);
/// ```
pub const fn encoded_version() -> u16 {
    (PROTOCOL_MAJOR << 8) | PROTOCOL_MINOR
}

/// Negotiate the highest mutually-supported major version.
///
/// Returns `Some(major)` if the client's major version is within
/// [`SUPPORTED_MAJOR`], or `None` if the client's major version is
/// unsupported.
///
/// For Rift/1 this always returns `Some(1)` for valid clients; the
/// helper exists so that a future Rift/2 server can still accept
/// Rift/1 clients by extending the supported range.
///
/// ```rust
/// use rifts_core::protocol::version::negotiate_major;
///
/// assert_eq!(negotiate_major(1), Some(1));
/// assert_eq!(negotiate_major(0), None);
/// assert_eq!(negotiate_major(2), None);
/// ```
pub fn negotiate_major(client_major: u16) -> Option<u16> {
    if SUPPORTED_MAJOR.contains(&client_major) {
        Some(client_major)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_is_major_minor() {
        assert_eq!(encoded_version(), 0x0100);
    }

    #[test]
    fn negotiate_accepts_self() {
        assert_eq!(negotiate_major(1), Some(1));
    }

    #[test]
    fn negotiate_rejects_other() {
        assert_eq!(negotiate_major(0), None);
        assert_eq!(negotiate_major(2), None);
        assert_eq!(negotiate_major(99), None);
    }
}

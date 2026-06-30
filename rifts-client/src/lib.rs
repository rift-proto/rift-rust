//! # rifts-client -- Rift Realtime Protocol / 1.0 Async Rust Client SDK
//!
//! Connect to a rifts server over WebSocket,
//! perform the Hello/Welcome/Ready handshake, and interact with topics
//! through a typed, broadcast-based event system.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! # async fn run() -> rifts_client::Result<()> {
//! use rifts_client::{RiftClient, RiftClientConfig, ClientEvent, SubscribeIntent};
//!
//! let client = RiftClient::new(
//!     "ws://localhost:9000",
//!     RiftClientConfig {
//!         client_id: "my-app".into(),
//!         token:    "my-jwt".into(),
//!         ..Default::default()
//!     },
//! );
//!
//! let mut events = client.subscribe_events();
//! client.connect().await?;
//!
//! client.subscribe("room/1", SubscribeIntent::Live, None).await?;
//! client.publish(
//!     "room/1", "chat.message", "chat.message@1.0",
//!     serde_json::json!({"text": "hello"}),
//!     None,
//! ).await?;
//!
//! while let Ok(evt) = events.recv().await {
//!     match evt {
//!         ClientEvent::EventReceived { topic, event } => {
//!             println!("[{topic}] {}: {:?}", event.event, event.payload);
//!         }
//!         ClientEvent::Disconnected { .. } => break,
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![deny(unreachable_pub)]
#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

#[cfg(feature = "client")]
mod config;
#[cfg(feature = "client")]
mod connection;
#[cfg(feature = "client")]
mod error;
#[cfg(feature = "client")]
mod events;
#[cfg(feature = "client")]
pub(crate) mod frame_builder;
#[cfg(feature = "client")]
pub(crate) mod heartbeat;
#[cfg(feature = "client")]
mod rift_client;
#[cfg(feature = "client")]
pub(crate) mod subscriber;

#[cfg(feature = "client")]
pub use config::RiftClientConfig;
#[cfg(feature = "client")]
pub use error::{ClientError, Result};
#[cfg(feature = "client")]
pub use events::ClientEvent;
#[cfg(feature = "client")]
pub use rift_client::{CommandOpts, PublishOpts, RiftClient, StateOpts};

// Re-export commonly used types for convenience.
#[cfg(feature = "client")]
pub use rifts_core::message::SubscribeIntent;
#[cfg(feature = "client")]
pub use rifts_core::message::command::Reply;
#[cfg(feature = "client")]
pub use rifts_core::protocol::close::CloseCode;
#[cfg(feature = "client")]
pub use rifts_core::{EncodingFormat, Frame, FrameFlags, FrameType, Priority};

#[cfg(all(test, feature = "client"))]
mod tests {
    use super::SubscribeIntent;

    #[test]
    fn subscribe_intent_should_be_core_protocol_type() {
        let intent: SubscribeIntent = rifts_core::message::SubscribeIntent::Live;

        assert_eq!(intent, SubscribeIntent::Live);
    }
}

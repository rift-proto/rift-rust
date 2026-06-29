//! Rift/1 server entry point.
//!
//! This module provides the [`RiftServer`] — the top-level server that
//! orchestrates the broker, authentication, metrics, and transport layers.
//! It can run in two modes:
//!
//! 1. **Standalone mode** (feature `websocket`): call
//!    [`RiftServer::run`] with a socket address and a shutdown notifier.
//!    The server binds a TCP listener, accepts WebSocket connections, and
//!    spawns a [`Connection`](crate::connection::Connection) for each.
//!
//! 2. **Framework mode**: call
//!    [`RiftServer::accept_and_spawn`] with a boxed
//!    [`TransportConnection`](crate::transport::TransportConnection)
//!    obtained from a framework adapter (axum, actix-web, warp, ntex).
//!    The server spawns the connection handler on the tokio runtime.
//!
//! # Builder pattern
//!
//! Use [`RiftServer::builder()`] to obtain a [`RiftServerBuilder`], then
//! configure the server via chainable methods:
//!
//! ```ignore
//! let server = RiftServer::builder()
//!     .config(my_config)
//!     .auth(my_auth_provider)
//!     .broker(my_broker)
//!     .websocket_transport()
//!     .metrics(my_metrics)
//!     .build()?;
//! ```

#[cfg(feature = "websocket")]
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use futures_util::FutureExt;
use tracing::error;
#[cfg(feature = "websocket")]
use tracing::info;

use crate::ack::{AckManager, SharedAckManager};
use crate::broker::{Broker, InMemoryBroker};
use crate::config::{DefaultTopicProfile, ServerConfig};
use crate::connection::Connection;
use crate::error::Result;
#[cfg(feature = "websocket")]
use crate::error::{ConfigError, RiftError};
use crate::metrics::Metrics;
use crate::session::AuthProvider;
use crate::session::resume::ResumeManager;
use crate::session::store::SessionStore;
use crate::transport::TransportConnection;
#[cfg(feature = "websocket")]
use crate::transport::{Transport, TransportListener};

// --- standalone transport support (feature = "websocket") ---

#[cfg(feature = "websocket")]
use crate::transport::websocket::WebSocketTransport;

#[cfg(feature = "websocket")]
type ListenerFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Box<dyn TransportListener>>> + Send>>;

/// Internal trait for constructing a transport listener from a socket address.
///
/// This abstraction allows the builder to defer transport construction until
/// `build()` is called, so that any `config()` call made after
/// `websocket_transport()` is honoured.
#[cfg(feature = "websocket")]
trait TransportFactory: Send + Sync {
    /// Build a transport listener bound to `addr`.
    fn build(&self, addr: SocketAddr) -> ListenerFuture;
}

/// The default transport factory for standalone WebSocket mode.
#[cfg(feature = "websocket")]
struct WebSocketFactory {
    /// Maximum WebSocket message size, taken from `ServerConfig::max_payload_bytes`.
    max_message_size: usize,
}

#[cfg(feature = "websocket")]
impl TransportFactory for WebSocketFactory {
    fn build(&self, addr: SocketAddr) -> ListenerFuture {
        let transport = WebSocketTransport::new().with_max_message_size(self.max_message_size);
        Box::pin(async move { transport.bind(addr).await })
    }
}

/// Builder for a [`RiftServer`].
///
/// Use [`RiftServer::builder()`] to obtain an instance, configure it via
/// chainable methods, and call [`build`](Self::build) to produce the final
/// server.
pub struct RiftServerBuilder {
    /// Server configuration (heartbeat, payload limits, etc.).
    config: ServerConfig,
    /// Authentication provider. Defaults to [`TokenAuth`](crate::session::TokenAuth)
    /// if not set.
    auth: Option<Arc<dyn AuthProvider>>,
    /// Broker implementation. Defaults to [`InMemoryBroker`] if not set.
    broker: Option<Arc<dyn Broker>>,
    /// Transport factory for standalone mode. `None` means the server is
    /// in framework mode and must be driven via `accept_and_spawn`.
    #[cfg(feature = "websocket")]
    transport: Option<Arc<dyn TransportFactory>>,
    /// Metrics collector. Defaults to a new [`Metrics`] instance if not set.
    metrics: Option<Arc<Metrics>>,
    /// Shared session store for cross-connection resume. If `None`,
    /// a fresh empty store is created at build time.
    session_store: Option<SessionStore>,
    /// Shared resume manager. If `None`, a fresh one is created at
    /// build time.
    resume_manager: Option<Arc<ResumeManager>>,
}

impl RiftServerBuilder {
    /// Create a new builder with all defaults.
    ///
    /// The default configuration is [`ServerConfig::default()`], no
    /// transport (framework mode), and no custom auth, broker, or metrics.
    pub fn new() -> Self {
        Self {
            config: ServerConfig::default(),
            auth: None,
            broker: None,
            #[cfg(feature = "websocket")]
            transport: None,
            metrics: None,
            session_store: None,
            resume_manager: None,
        }
    }

    /// Set the server configuration.
    ///
    /// This replaces the entire config, so call it before any other
    /// config-dependent method.
    pub fn config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the authentication provider.
    ///
    /// If not set, [`TokenAuth`](crate::session::TokenAuth) is used by
    /// default.
    pub fn auth(mut self, auth: Arc<dyn AuthProvider>) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Set a custom broker implementation.
    ///
    /// If not set, an [`InMemoryBroker`] is created with the default
    /// topic profile, dedupe window, and max payload bytes from the
    /// server config.
    pub fn broker(mut self, broker: Arc<dyn Broker>) -> Self {
        self.broker = Some(broker);
        self
    }

    /// Enable standalone WebSocket transport mode (requires feature
    /// `websocket`).
    ///
    /// The actual transport factory is constructed in [`build`](Self::build)
    /// so that any [`config`](Self::config) call made after this point is
    /// honoured (specifically `max_payload_bytes`).
    #[cfg(feature = "websocket")]
    pub fn websocket_transport(mut self) -> Self {
        // Placeholder; the real factory is constructed in `build()`
        // with the current `config.max_payload_bytes`.
        self.transport = Some(Arc::new(WebSocketFactory {
            max_message_size: 0,
        }));
        self
    }

    /// Set the metrics collector.
    ///
    /// If not set, a new [`Metrics`] instance is created with all
    /// counters initialized to zero.
    pub fn metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Build the [`RiftServer`].
    ///
    /// This method finalizes the configuration, creates default
    /// components where none were provided, and returns the ready-to-use
    /// server. In standalone mode the transport factory is reconstructed
    /// with the current `config.max_payload_bytes` to ensure any
    /// post-`websocket_transport()` config changes are applied.
    /// Provide a pre-populated [`SessionStore`].
    ///
    /// Useful when embedding the server in a larger process that
    /// already tracks sessions externally. If not set, a fresh empty
    /// store is created at build time.
    pub fn session_store(mut self, store: SessionStore) -> Self {
        self.session_store = Some(store);
        self
    }

    /// Provide a shared [`ResumeManager`].
    ///
    /// If not set, a fresh default manager is created at build time.
    pub fn resume_manager(mut self, rm: Arc<ResumeManager>) -> Self {
        self.resume_manager = Some(rm);
        self
    }

    pub fn build(self) -> Result<RiftServer> {
        let metrics = self.metrics.unwrap_or_else(|| Arc::new(Metrics::new()));
        let config_max_payload = self.config.max_payload_bytes;
        let broker = self.broker.unwrap_or_else(|| {
            let topic_profile: crate::topic::TopicProfile =
                self.config.default_topic_profile.clone().into();
            Arc::new(InMemoryBroker::new(
                topic_profile,
                self.config.dedupe_window,
                config_max_payload,
            ))
        });
        let auth = self
            .auth
            .unwrap_or_else(|| Arc::new(crate::session::TokenAuth::new()));

        // Reconstruct the WebSocket factory with the *current*
        // `config.max_payload_bytes` so that any `config()` call made
        // after `websocket_transport()` is honoured.
        #[cfg(feature = "websocket")]
        let transport = self.transport.as_ref().map(|_| {
            Arc::new(WebSocketFactory {
                max_message_size: self.config.max_payload_bytes,
            }) as Arc<dyn TransportFactory>
        });

        let session_store = self.session_store.unwrap_or_default();
        let resume_manager = self
            .resume_manager
            .unwrap_or_else(|| Arc::new(ResumeManager::new()));

        Ok(RiftServer {
            config: self.config,
            auth,
            broker,
            metrics,
            #[cfg(feature = "websocket")]
            transport,
            next_conn_id: Arc::new(AtomicU64::new(1)),
            session_store,
            resume_manager,
        })
    }
}

impl Default for RiftServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The Rift/1 server.
///
/// This is the top-level entry point. It holds the broker, auth provider,
/// metrics, and (in standalone mode) the transport factory. Each accepted
/// connection is assigned a unique connection id and spawned as a new
/// tokio task running the full Rift protocol lifecycle.
pub struct RiftServer {
    /// Server configuration (heartbeat, payload limits, topic defaults, etc.).
    pub config: ServerConfig,
    /// Authentication provider used for the hello handshake.
    auth: Arc<dyn AuthProvider>,
    /// Broker that routes messages between publishers and subscribers.
    broker: Arc<dyn Broker>,
    /// Metrics collector for connection, message, and error counters.
    metrics: Arc<Metrics>,
    /// Standalone transport factory. `None` in framework mode (the
    /// server is driven via `accept_and_spawn`).
    #[cfg(feature = "websocket")]
    transport: Option<Arc<dyn TransportFactory>>,
    /// Monotonically increasing connection id counter.
    next_conn_id: Arc<AtomicU64>,
    /// Shared session store for cross-connection session resumption.
    session_store: SessionStore,
    /// Shared resume manager for evaluating session resume requests.
    resume_manager: Arc<ResumeManager>,
}

impl RiftServer {
    /// Create a new [`RiftServerBuilder`].
    pub fn builder() -> RiftServerBuilder {
        RiftServerBuilder::new()
    }

    /// Bind and run the server in standalone mode, blocking until
    /// `shutdown` is notified.
    ///
    /// Requires feature `websocket` and a transport set on the builder
    /// (call `builder.websocket_transport()` before `build()`).
    ///
    /// Each accepted connection is spawned as a new tokio task. The
    /// method returns when the shutdown notifier fires or when the
    /// listener encounters a fatal error.
    #[cfg(feature = "websocket")]
    pub async fn run(&self, addr: SocketAddr, shutdown: Arc<tokio::sync::Notify>) -> Result<()> {
        let transport = self.transport.as_ref().ok_or_else(|| {
            RiftError::Config(ConfigError::Invalid {
                field: "transport",
                message:
                    "no transport configured; call builder.websocket_transport() before build(), \
                     or use accept_and_spawn() for framework mode"
                        .to_string(),
            })
        })?;
        let mut listener = transport.build(addr).await?;
        info!(addr = ?listener.local_addr()?, "rift server listening");

        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    info!("shutdown signaled");
                    return Ok(());
                }
                res = listener.accept() => {
                    match res {
                        Ok(conn) => {
                            self.spawn_connection(conn);
                        }
                        Err(e) => {
                            error!("accept error: {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Accept a single transport connection and spawn the Rift protocol
    /// handler onto the tokio runtime.
    ///
    /// This is the entry point for framework integrations (axum, actix-web,
    /// warp, ntex). The caller obtains a `Box<dyn TransportConnection>` from
    /// the framework adapter and passes it here. The server assigns a unique
    /// connection id, creates a [`Connection`], and spawns it as a new task.
    pub fn accept_and_spawn(&self, transport: Box<dyn TransportConnection>) {
        self.spawn_connection(transport);
    }

    /// Internal helper: create and spawn a [`Connection`] for the given
    /// transport.
    fn spawn_connection(&self, transport: Box<dyn TransportConnection>) {
        let id = self
            .next_conn_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let ack_manager: SharedAckManager = Arc::new(AckManager::new());
        let offset_tracker = self.session_store.offset_tracker().clone();
        let connection = Connection::new(
            id,
            self.broker.clone(),
            self.auth.clone(),
            self.config.clone(),
            self.metrics.clone(),
            ack_manager,
            self.resume_manager.clone(),
            offset_tracker,
            self.session_store.clone(),
        );
        tokio::spawn(async move {
            // Catch panics in the connection task so a misbehaving
            // session does not bring down the whole server. The
            // `AssertUnwindSafe` is sound here because the
            // connection owns no `&mut` state shared with other
            // tasks.
            let result = AssertUnwindSafe(connection.run(transport))
                .catch_unwind()
                .await;
            match result {
                Ok(Ok(())) => {
                    tracing::debug!(conn = id, "connection ended cleanly");
                }
                Ok(Err(e)) => {
                    error!(conn = id, "connection ended with error: {}", e);
                }
                Err(panic) => {
                    error!(conn = id, "connection task panicked: {:?}", panic);
                }
            }
        });
    }
}

/// Conversion from the config-level [`DefaultTopicProfile`] to the
/// topic-layer [`TopicProfile`](crate::topic::TopicProfile).
///
/// This conversion is used when the builder creates the default
/// [`InMemoryBroker`].
impl From<DefaultTopicProfile> for crate::topic::TopicProfile {
    fn from(d: DefaultTopicProfile) -> Self {
        // The `name` field of a TopicProfile acts as a
        // template default applied to topics that don't
        // specify their own. The empty string is used to
        // signal "unset"; the per-topic entry will be
        // named on creation.
        Self {
            name: String::new(),
            retention: d.retention,
            ordering: d.ordering,
            max_subscribers: d.max_subscribers,
            max_publishers: d.max_publishers,
            rate_limit_per_publisher: None,
            rate_limit_total: None,
            replay_enabled: d.replay_enabled,
            snapshot_enabled: d.snapshot_enabled,
            snapshot_ttl: d.snapshot_ttl,
            replay_window: std::time::Duration::from_secs(300),
        }
    }
}

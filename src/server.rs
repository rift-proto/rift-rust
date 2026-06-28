//! Rift/1 server entry point.

#[cfg(feature = "websocket")]
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

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
use crate::transport::TransportConnection;
#[cfg(feature = "websocket")]
use crate::transport::{Transport, TransportListener};

// --- standalone transport support (feature = "websocket") ---

#[cfg(feature = "websocket")]
use crate::transport::websocket::WebSocketTransport;

#[cfg(feature = "websocket")]
type ListenerFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Box<dyn TransportListener>>> + Send>>;

#[cfg(feature = "websocket")]
trait TransportFactory: Send + Sync {
    fn build(&self, addr: SocketAddr) -> ListenerFuture;
}

#[cfg(feature = "websocket")]
struct WebSocketFactory {
    max_message_size: usize,
}

#[cfg(feature = "websocket")]
impl TransportFactory for WebSocketFactory {
    fn build(&self, addr: SocketAddr) -> ListenerFuture {
        let transport = WebSocketTransport::new().with_max_message_size(self.max_message_size);
        Box::pin(async move { transport.bind(addr).await })
    }
}

/// Builder for a `RiftServer`.
pub struct RiftServerBuilder {
    config: ServerConfig,
    auth: Option<Arc<dyn AuthProvider>>,
    broker: Option<Arc<dyn Broker>>,
    /// Transport is set by `websocket_transport()`; `None` means the
    /// server is in framework mode and must be driven via
    /// `accept_and_spawn`.
    #[cfg(feature = "websocket")]
    transport: Option<Arc<dyn TransportFactory>>,
    metrics: Option<Arc<Metrics>>,
}

impl RiftServerBuilder {
    /// Create a new builder with defaults.
    pub fn new() -> Self {
        Self {
            config: ServerConfig::default(),
            auth: None,
            broker: None,
            #[cfg(feature = "websocket")]
            transport: None,
            metrics: None,
        }
    }

    /// Set server configuration.
    pub fn config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    /// Set authentication provider.
    pub fn auth(mut self, auth: Arc<dyn AuthProvider>) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Set custom broker.
    pub fn broker(mut self, broker: Arc<dyn Broker>) -> Self {
        self.broker = Some(broker);
        self
    }

    /// Set WebSocket as the standalone transport (requires feature
    /// `websocket`). The factory is built in `build()` so that any
    /// `config()` call after this point is honoured.
    #[cfg(feature = "websocket")]
    pub fn websocket_transport(mut self) -> Self {
        // Placeholder; the real factory is constructed in `build()`
        // with the current `config.max_payload_bytes`.
        self.transport = Some(Arc::new(WebSocketFactory {
            max_message_size: 0,
        }));
        self
    }

    /// Set metrics collector.
    pub fn metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Build the server.
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

        Ok(RiftServer {
            config: self.config,
            auth,
            broker,
            metrics,
            #[cfg(feature = "websocket")]
            transport,
            next_conn_id: Arc::new(AtomicU64::new(1)),
        })
    }
}

impl Default for RiftServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The Rift/1 server.
pub struct RiftServer {
    /// Server configuration.
    pub config: ServerConfig,
    auth: Arc<dyn AuthProvider>,
    broker: Arc<dyn Broker>,
    metrics: Arc<Metrics>,
    /// Standalone transport factory. `None` in framework mode (the
    /// server is driven via `accept_and_spawn`).
    #[cfg(feature = "websocket")]
    transport: Option<Arc<dyn TransportFactory>>,
    next_conn_id: Arc<AtomicU64>,
}

impl RiftServer {
    /// Create a new builder.
    pub fn builder() -> RiftServerBuilder {
        RiftServerBuilder::new()
    }

    /// Bind and run the server in standalone mode, blocking until
    /// `shutdown` is notified. Requires feature `websocket` and a
    /// transport set on the builder (call
    /// `builder.websocket_transport()` before `build()`).
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

    /// Accept a single transport connection and spawn the Rift
    /// protocol handler onto the tokio runtime. This is the entry
    /// point for framework integrations.
    pub fn accept_and_spawn(&self, transport: Box<dyn TransportConnection>) {
        self.spawn_connection(transport);
    }

    fn spawn_connection(&self, transport: Box<dyn TransportConnection>) {
        let id = self
            .next_conn_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let ack_manager: SharedAckManager = Arc::new(AckManager::new());
        let resume = Arc::new(ResumeManager::new());
        let connection = Connection::new(
            id,
            self.broker.clone(),
            self.auth.clone(),
            self.config.clone(),
            self.metrics.clone(),
            ack_manager,
            resume,
        );
        tokio::spawn(async move {
            if let Err(e) = connection.run(transport).await {
                error!(conn = id, "connection ended with error: {}", e);
            }
        });
    }
}

impl From<DefaultTopicProfile> for crate::topic::TopicProfile {
    fn from(d: DefaultTopicProfile) -> Self {
        Self {
            name: "default".into(),
            retention: d.retention,
            ordering: d.ordering,
            max_subscribers: d.max_subscribers,
            max_publishers: d.max_publishers,
            rate_limit_per_publisher: None,
            rate_limit_total: None,
            replay_enabled: d.replay_enabled,
            snapshot_enabled: d.snapshot_enabled,
            replay_window: std::time::Duration::from_secs(300),
        }
    }
}

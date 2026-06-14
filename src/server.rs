//! Rift/1 server entry point.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use tokio::sync::Notify;
use tracing::{error, info};

use crate::ack::{AckManager, SharedAckManager};
use crate::broker::{Broker, InMemoryBroker};
use crate::config::{DefaultTopicProfile, ServerConfig};
use crate::connection::Connection;
use crate::error::Result;
use crate::metrics::Metrics;
use crate::session::AuthProvider;
use crate::session::resume::ResumeManager;
use crate::transport::websocket::WebSocketTransport;
use crate::transport::{Transport, TransportListener};

/// Builder for a `RiftServer`.
pub struct RiftServerBuilder {
    config: ServerConfig,
    auth: Option<Arc<dyn AuthProvider>>,
    broker: Option<Arc<dyn Broker>>,
    transport: Option<Box<dyn TransportFactory>>,
    metrics: Option<Arc<Metrics>>,
}

trait TransportFactory: Send + Sync {
    fn build(
        &self,
        addr: SocketAddr,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Box<dyn TransportListener>>> + Send>,
    >;
}

struct WebSocketFactory;
impl TransportFactory for WebSocketFactory {
    fn build(
        &self,
        addr: SocketAddr,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Box<dyn TransportListener>>> + Send>,
    > {
        Box::pin(async move { WebSocketTransport::new().bind(addr).await })
    }
}

impl RiftServerBuilder {
    pub fn new() -> Self {
        Self {
            config: ServerConfig::default(),
            auth: None,
            broker: None,
            transport: None,
            metrics: None,
        }
    }

    pub fn config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn auth(mut self, auth: Arc<dyn AuthProvider>) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn broker(mut self, broker: Arc<dyn Broker>) -> Self {
        self.broker = Some(broker);
        self
    }

    pub fn websocket_transport(mut self) -> Self {
        self.transport = Some(Box::new(WebSocketFactory));
        self
    }

    pub fn metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn build(self) -> Result<RiftServer> {
        let metrics = self.metrics.unwrap_or_else(|| Arc::new(Metrics::new()));
        let broker = self.broker.unwrap_or_else(|| {
            let profile = DefaultTopicProfile::default();
            let topic_profile: crate::topic::TopicProfile = profile.into();
            Arc::new(InMemoryBroker::new(
                topic_profile,
                self.config.dedupe_window,
            ))
        });
        let auth = self
            .auth
            .unwrap_or_else(|| Arc::new(crate::session::TokenAuth::new()));
        let transport = self.transport.ok_or_else(|| {
            crate::error::RiftError::Config(crate::error::ConfigError::Invalid {
                field: "transport",
                message: "transport is required".to_string(),
            })
        })?;
        Ok(RiftServer {
            config: self.config,
            auth,
            broker,
            metrics,
            transport: Arc::new(transport),
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
    config: ServerConfig,
    auth: Arc<dyn AuthProvider>,
    broker: Arc<dyn Broker>,
    metrics: Arc<Metrics>,
    transport: Arc<Box<dyn TransportFactory>>,
    next_conn_id: Arc<AtomicU64>,
}

impl RiftServer {
    pub fn builder() -> RiftServerBuilder {
        RiftServerBuilder::new()
    }

    /// Bind and run the server, blocking until `shutdown` is notified.
    pub async fn run(&self, addr: SocketAddr, shutdown: Arc<Notify>) -> Result<()> {
        let mut listener = self.transport.build(addr).await?;
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
                                if let Err(e) = connection.run(conn).await {
                                    error!(conn = id, "connection ended with error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("accept error: {}", e);
                        }
                    }
                }
            }
        }
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

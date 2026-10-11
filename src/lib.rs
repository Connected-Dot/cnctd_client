//! Client for servers built with the [`cnctd_server`](https://crates.io/crates/cnctd_server)
//! Rust crate: REST calls that return the server's JSON envelope and fail
//! with an [`ApiError`] when the server says no, plus the WebSocket with
//! reconnect, push routing by channel, and request/response over the
//! socket. The wire types are the server crate's own, so the two cannot
//! drift.
//!
//! ```no_run
//! use cnctd_client::{Client, Registration};
//! use serde_json::json;
//!
//! # async fn demo() -> cnctd_client::Result<()> {
//! let client = Client::builder("https://cnctd.world")
//!     .token(|| async { Some("<access token>".to_string()) })
//!     .build()?;
//!
//! let dots = client.get("dots", "all", json!({})).await?;          // SuccessResponse, or Error::Api
//!
//! let socket = client
//!     .socket(Registration::rest(|c| async move {
//!         let r = c.post("client-sessions", "register", json!({ "subscriptions": ["server-info"] })).await?;
//!         Ok(r.data.and_then(|d| d.as_str().map(String::from)).unwrap_or_default())
//!     }))
//!     .start()?;                                                     // reconnects on its own
//! let mut chat = socket.subscribe("chatChunk");
//! socket.send("client-data", Some("set-client-info"), json!({ "platform": "hub" }))?;
//! let reply = socket.call("sync", Some("snapshot"), json!({})).await?;
//! # let _ = (dots, chat.recv().await, reply);
//! # Ok(()) }
//! ```
//!
//! Nothing app-specific lives here: how a token is obtained, how a
//! connection is registered, and what happens on open are injected or
//! read off the socket's events.

mod client;
mod error;
mod socket;

pub use client::{to_query, Client, ClientBuilder, RawResponse, TokenFn};
pub use error::{problem_text, ApiError, Error, Result};
pub use socket::{Backoff, Event, Frame, RegisterFn, Registration, Socket, SocketBuilder, State};

/// The wire types, as the server defines them.
pub mod protocol {
    pub use cnctd_server::router::{
        error::{ErrorCode, ErrorResponse},
        message::{Message, SocketMessage, UserMessage, UserSocketMessage},
        response::{SocketResponse, SuccessCode, SuccessResponse},
        HttpMethod,
    };
    pub use cnctd_server::server::server_info::{ServerInfo, ServerStatus};
    pub use cnctd_server::socket::connection::{ConnectionFormat, ConnectionInfo};
}

pub use protocol::*;

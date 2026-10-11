# cnctd_client

The client side of a server built with the
[`cnctd_server`](https://crates.io/crates/cnctd_server) Rust crate: REST calls
that return the JSON envelope and fail with an `ApiError` when the server says
no, plus the WebSocket with reconnect, push routing by channel, and
request/response over the socket. The wire types are the server crate's own,
so the two cannot drift.

```rust
use cnctd_client::{Client, Registration};
use serde_json::json;

let client = Client::builder("https://cnctd.world")
    .token(|| async { tokens.access_token().await })   // Authorization, verbatim; may refresh
    .build()?;

let dots = client.get("dots", "all", json!({})).await?;   // SuccessResponse, or Error::Api

let socket = client
    .socket(Registration::rest(|c| async move {           // or Registration::inline([...])
        let r = c.post("client-sessions", "register", json!({ "subscriptions": ["server-info"] })).await?;
        Ok(r.data.and_then(|d| d.as_str().map(String::from)).unwrap_or_default())
    }))
    .start()?;                                            // reconnects on its own after that
let mut chat = socket.subscribe("chatChunk");
socket.send("client-data", Some("set-client-info"), json!({ "platform": "hub" }))?;
let reply = socket.call("sync", Some("snapshot"), json!({})).await?;
```

Nothing app-specific lives here: how a token is obtained, how a connection is
registered, and what happens on open are injected or read off the socket's
events (`Open`, `Close`, `Reconnecting`, `Heartbeat`) and its `state()` watch.

The TypeScript twin is [`cnctd-server`](https://github.com/Connected-Dot/cnctd-server) on npm.

## Install

```toml
cnctd_client = "1.0.0-alpha.1"
```

TLS is `rustls` by default (`native-tls` is a feature). The crate installs no
rustls crypto provider: a binary that links both `ring` and `aws-lc-rs`
through other crates must pick one in `main()`.

## Development

```bash
cargo test                 # units + tests/live.rs against a real cnctd_server on an ephemeral port
cargo clippy --all-targets
```

MIT

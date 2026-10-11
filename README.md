# cnctd_client

The client side of a server built with the
[`cnctd_server`](https://crates.io/crates/cnctd_server) Rust crate: REST calls
that return the JSON envelope and fail with an `ApiError` when the server says
no, plus the WebSocket with reconnect, push routing by channel, and
request/response over the socket. Wire types come from `cnctd_protocol`, the
same crate the server uses, so the two cannot drift.

The TypeScript twin is [`cnctd-server`](https://github.com/Connected-Dot/cnctd-server) on npm.

Scaffold; the crate is being written. MIT.

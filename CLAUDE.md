# CLAUDE.md - cnctd_client

> The Rust client for servers built with the `cnctd_server` crate. Published
> to crates.io as `cnctd_client`; the server crate's wire types are the
> source of truth. The TypeScript twin is `modules/typescript/cnctd-server`
> (npm `cnctd-server`); the two have the same shape on purpose.

## What it is

- `Client` (`src/client.rs`) — REST (`get/post/put/delete/request`)
  returning the server's `SuccessResponse` and failing with `Error::Api`
  (`ApiError`: HTTP status, the server's status word and message, what was
  attempted), `raw()` for non-envelope routes (bytes + content type). A
  builder takes the base URL, a `token()` future for `Authorization` (sent
  verbatim: raw or `Bearer`, the caller's choice), extra headers (sent on
  REST and on the socket upgrade), a timeout, and a socket URL override.
  Sends `Client-Id` once it knows its connection id.
- `Socket` (`src/socket.rs`) — one logical WebSocket across reconnects:
  `Registration::Rest` (an app REST call returns the id) or
  `Registration::Inline` (`?subscriptions=`, id learned from the server's
  `socket/connected` frame when it sends one), exponential backoff, a
  client-side ping (30 s) and read timeout (90 s), `subscribe(channel)` for
  pushes as `Message`, `call()` over `response_channel` (legacy
  `response_resource` accepted), `frames()` for every text/binary frame,
  `events()` (`Open / Close / Reconnecting / Heartbeat`), `state()` watch.
- `protocol` — re-exports of the wire types from `cnctd_server`
  (`cnctd_protocol` once it exists; same paths).

## Rules

- **Nothing app-specific.** Token refresh, pairing, what to do on open,
  which store a push updates: injected or read off the events in the app.
  cnctd.world's `hub/src/modules/world_link.rs` is the reference consumer.
- **Every connect attempt runs in its own task** (`supervise` →
  `tokio::spawn(run_once)`), so a panic inside one (a TLS library refusing
  to start, 2026-10-06 in the hub) is logged and retried, never the end of
  the link. Keep it that way.
- **Connection ids are per-connect.** The registration runs again on every
  attempt; the id is cleared on close.
- **No crypto provider is installed here.** A binary that links both ring
  and aws-lc-rs picks one in `main()` (`rustls::crypto::ring::default_provider().install_default()`).
- **The client ping stays on by default.** Production (0.6.5) does not
  ping; something in front of it reset idle sockets ~125 s after connect.
  A 1.0 server pings too; the duplication is harmless.
- **Either signal rejects.** HTTP status >= 400 OR `success: false` is an
  `ApiError`, so a proxy or error page that mangles one still fails closed.

## Development

```bash
cargo test            # units + tests/live.rs: a real cnctd_server on an ephemeral port
cargo clippy --all-targets
```

`tests/live.rs` runs the same routers as the server crate's `demo_server`
example (which the TypeScript client's integration test drives): echo,
fail, bin, REST registration; socket echo, rpc, heartbeat, connected
announcement. Reconnect is tested through the read timeout.

The crate has its own `[workspace]` table because it sits inside the cnctd
monorepo, whose root workspace it is not a member of.

## Version

**1.0.0-alpha.1**, against `cnctd_server` 1.0.0-alpha.1's types. When
`cnctd_protocol` is published, swap the dependency; the paths do not change.

---

*Part of the cnctd monorepo. See `../../../CLAUDE.md` for ecosystem context.*

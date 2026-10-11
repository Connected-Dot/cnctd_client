//! The client against a REAL cnctd_server: the crate's demo routers on an
//! ephemeral loopback port (the same routers as `cnctd_server`'s
//! `demo_server` example, which the TypeScript client's integration test
//! drives). This is the proof the two sides agree.

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, OnceLock},
    time::Duration,
};

use cnctd_client::{Backoff, Client, Error, Event, HttpMethod, Registration, Socket, State};
use cnctd_server::{
    async_trait, bad_request, not_found, success_data, unauthorized,
    router::{
        error::{ErrorCode, ErrorResponse},
        message::SocketMessage,
        response::{BinaryResponse, SuccessResponse},
    },
    CnctdServer, Connections, Request, Response, RestRouter, RunningServer, ServerConfig, SocketConfig, SocketRouter,
};
use serde_json::{json, Value};

/// The demo routers. The register route uses THIS server's table, not
/// `Connections::global()`: every test starts its own server, and the
/// global handle is whichever started last.
struct Rest {
    connections: Arc<OnceLock<Connections>>,
}

#[async_trait]
impl RestRouter for Rest {
    async fn route(&self, req: Request) -> Result<Response, ErrorResponse> {
        let (resource, operation) = req.resource_and_operation();
        match (resource.as_str(), operation.as_deref()) {
            ("echo", _) => Ok(success_data!(json!({
                "method": req.method.as_str(),
                "path": req.path,
                "data": req.data,
                "auth_token": req.auth_token,
                "connection_id": req.connection_id,
                "ip_address": req.ip_address,
                "headers": req.headers,
            }))
            .into()),
            ("fail", Some("bad-request")) => Err(bad_request!("you asked for it")),
            ("fail", Some("unauthorized")) => Err(unauthorized!("no token")),
            ("bin", _) => Ok(BinaryResponse::octet_stream(b"BIN".to_vec()).into()),
            ("connections", Some("register")) => {
                let subs: Vec<String> = req
                    .data
                    .get("subscriptions")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect())
                    .unwrap_or_default();
                let id = self
                    .connections
                    .get()
                    .cloned()
                    .ok_or_else(|| ErrorResponse::new(Some(ErrorCode::InternalServerError), Some("no table yet".into())))?
                    .register(subs, req.ip_address, Default::default())
                    .await;
                Ok(success_data!(json!(id)).into())
            }
            _ => Err(not_found!(format!("no {}", req.path))),
        }
    }
}

struct Sock;

#[async_trait]
impl SocketRouter<SocketMessage, Value> for Sock {
    async fn route(&self, msg: SocketMessage, connection_id: String) -> Option<Value> {
        match msg.channel.as_str() {
            "echo" => Some(json!({"channel": "echo", "instruction": msg.instruction, "data": msg.data})),
            "rpc" => Some(json!({
                "success": true,
                "msg": null,
                "data": {"answered": msg.data, "from": connection_id},
                "response_channel": msg.response_channel,
            })),
            _ => None,
        }
    }
}

async fn start(heartbeat_secs: Option<u64>) -> RunningServer {
    let connections = Arc::new(OnceLock::new());
    let rest = Rest { connections: Arc::clone(&connections) };
    let server = ServerConfig::new("demo", "0", None, rest, heartbeat_secs, None, None).with_bind_addr(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let socket = SocketConfig::new(Sock, None, None, None).with_connected_announcement(true);
    let running = CnctdServer::bind(server, Some(socket)).await.expect("server starts");
    let _ = connections.set(running.connections().clone());
    running
}

fn client(server: &RunningServer) -> Client {
    Client::builder(server.url()).header("X-Test", "yes").build().unwrap()
}

/// The id once the server has announced it (`socket/connected`).
async fn announced_id(socket: &Socket) -> String {
    let mut state = socket.state();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let State::Connected { connection_id: Some(id) } = &*state.borrow() {
                return id.clone();
            }
            state.changed().await.expect("state stays alive");
        }
    })
    .await
    .expect("the server announces the connection")
}

async fn next_event(events: &mut tokio::sync::broadcast::Receiver<Event>, what: impl Fn(&Event) -> bool) -> Event {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let e = events.recv().await.expect("events stay alive");
            if what(&e) {
                return e;
            }
        }
    })
    .await
    .expect("the event arrives")
}

#[tokio::test]
async fn rest_round_trip() {
    let server = start(None).await;
    let c = Client::builder(server.url())
        .token(|| async { Some("Bearer tok-1".to_string()) })
        .header("X-Test", "yes")
        .build()
        .unwrap();
    c.set_connection_id(Some("pretend-conn".into()));

    let res = c.post("echo", "ctx", json!({ "a": 1 })).await.unwrap();
    assert!(res.success);
    let d = res.data.unwrap();
    assert_eq!(d["method"], "POST");
    assert_eq!(d["path"], "/api/echo/ctx");
    assert_eq!(d["data"], json!({ "a": 1 }));
    assert_eq!(d["auth_token"], "Bearer tok-1", "Authorization goes through verbatim");
    assert_eq!(d["connection_id"], "pretend-conn", "Client-Id carries the connection id");
    assert_eq!(d["ip_address"], "127.0.0.1");
    assert_eq!(d["headers"]["x-test"], "yes", "extra headers ride every request");

    let q = c.get("echo", "q", json!({ "x": 1, "y": "two", "n": { "k": true } })).await.unwrap();
    let d = q.data.unwrap();
    assert_eq!(d["method"], "GET");
    assert_eq!(d["data"], json!({ "x": "1", "y": "two", "n": "{\"k\":true}" }), "query values arrive as strings");

    match c.get("fail", "bad-request", json!({})).await {
        Err(Error::Api(e)) => {
            assert_eq!((e.http_status, e.status.as_str(), e.msg.as_deref()), (400, "BadRequest", Some("you asked for it")));
            assert_eq!(e.code(), ErrorCode::BadRequest);
            assert!(!e.is_auth());
        }
        other => panic!("expected an ApiError, got {other:?}"),
    }
    match c.get("fail", "unauthorized", json!({})).await {
        Err(Error::Api(e)) => assert!(e.is_auth() && e.http_status == 401),
        other => panic!("expected 401, got {other:?}"),
    }
    match c.delete("nothing", "here", json!({})).await {
        Err(Error::Api(e)) => assert_eq!((e.http_status, e.status.as_str()), (404, "NotFound")),
        other => panic!("expected 404, got {other:?}"),
    }

    let raw = c.raw(HttpMethod::GET, "bin", "x", Value::Null).await.unwrap();
    assert_eq!(raw.body, b"BIN");
    assert!(raw.content_type.as_deref().unwrap_or("").starts_with("application/octet-stream"));
    let as_envelope = c.get("bin", "x", json!({})).await.unwrap();
    assert_eq!(as_envelope.data, Some(json!("BIN")), "a non-envelope body is handed over as data");

    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn inline_registration_echo_rpc_heartbeat() {
    let server = start(Some(1)).await;
    let c = client(&server);
    let socket = c.socket(Registration::inline(["server-info", "news"])).reconnect(None).start().unwrap();
    let mut events = socket.events();
    socket.wait_connected(Duration::from_secs(3)).await.unwrap();
    let id = announced_id(&socket).await;
    assert_eq!(c.connection_id().as_deref(), Some(id.as_str()), "the announced id reaches the client");

    let res = c.get("echo", "with-id", json!({})).await.unwrap();
    assert_eq!(res.data.unwrap()["connection_id"], id, "REST now carries the socket's id");

    let mut echo = socket.subscribe("echo");
    socket.send("echo", Some("hi"), json!({ "n": 1 })).unwrap();
    let pushed = tokio::time::timeout(Duration::from_secs(3), echo.recv()).await.unwrap().unwrap();
    assert_eq!((pushed.channel.as_str(), pushed.instruction.as_str()), ("echo", "hi"));
    assert_eq!(pushed.data, Some(json!({ "n": 1 })));

    let reply = socket.call("rpc", Some("ask"), 42).await.unwrap();
    assert!(reply.success);
    let data = reply.data.unwrap();
    assert_eq!(data["answered"], 42);
    assert_eq!(data["from"], id);

    let beat = next_event(&mut events, |e| matches!(e, Event::Heartbeat(_))).await;
    let Event::Heartbeat(info) = beat else { unreachable!() };
    assert_eq!(info.id, "demo");

    socket.close();
    let closed = next_event(&mut events, |e| matches!(e, Event::Close { .. })).await;
    let Event::Close { will_reconnect, .. } = closed else { unreachable!() };
    assert!(!will_reconnect);
    assert!(matches!(socket.send("echo", None, json!({})), Err(Error::NotConnected)));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn rest_registration_first() {
    let server = start(None).await;
    let c = client(&server);
    let socket = c
        .socket(Registration::rest(|c| async move {
            let r = c.post("connections", "register", json!({ "subscriptions": ["news"] })).await?;
            Ok(r.data.and_then(|d| d.as_str().map(String::from)).unwrap_or_default())
        }))
        .reconnect(None)
        .start()
        .unwrap();
    socket.wait_connected(Duration::from_secs(3)).await.unwrap();
    let id = socket.connection_id().expect("the registration's id");
    assert_eq!(socket.current_state(), State::Connected { connection_id: Some(id.clone()) });

    let reply = socket.call_raw("rpc", None, "x").await.unwrap();
    assert_eq!(reply["data"]["from"], id);
    socket.close();
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_connection_id_is_refused_before_the_upgrade() {
    let server = start(None).await;
    let c = client(&server);
    let socket = c
        .socket(Registration::rest(|_| async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok("never-registered".to_string())
        }))
        .reconnect(None)
        .start()
        .unwrap();
    let mut events = socket.events();
    let closed = next_event(&mut events, |e| matches!(e, Event::Close { .. })).await;
    let Event::Close { reason, will_reconnect } = closed else { unreachable!() };
    assert!(!will_reconnect);
    assert!(reason.contains("404"), "a 1.0 server refuses with a 404 envelope: {reason}");
    assert!(socket.wait_connected(Duration::from_millis(200)).await.is_err());
    assert_eq!(socket.current_state(), State::Disconnected);
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn reconnects_after_the_read_timeout() {
    // No heartbeat, no subscription, no client ping: a socket with no frames
    // is presumed dead after `read_timeout` and comes back with a new id.
    let server = start(None).await;
    let c = client(&server);
    let socket = c
        .socket(Registration::inline(["news"]))
        .keepalive(None)
        .read_timeout(Some(Duration::from_millis(300)))
        .reconnect(Some(Backoff { min: Duration::from_millis(50), max: Duration::from_millis(100) }))
        .start()
        .unwrap();
    let mut events = socket.events();
    socket.wait_connected(Duration::from_secs(3)).await.unwrap();
    let first = announced_id(&socket).await;

    let closed = next_event(&mut events, |e| matches!(e, Event::Close { .. })).await;
    let Event::Close { reason, will_reconnect } = closed else { unreachable!() };
    assert!(will_reconnect);
    assert!(reason.contains("no frame"), "{reason}");
    let again = next_event(&mut events, |e| matches!(e, Event::Reconnecting { .. })).await;
    let Event::Reconnecting { attempt, delay } = again else { unreachable!() };
    assert_eq!((attempt, delay), (1, Duration::ZERO), "a dropped connection retries right away");
    next_event(&mut events, |e| matches!(e, Event::Open { .. })).await;
    let second = announced_id(&socket).await;
    assert_ne!(first, second, "connection ids are per-connect");

    socket.close();
    next_event(&mut events, |e| matches!(e, Event::Close { will_reconnect: false, .. })).await;
    server.shutdown().await.unwrap();
}

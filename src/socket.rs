//! One logical WebSocket connection across reconnects.
//!
//! Pushes are routed by channel (`subscribe`), request/response rides on
//! `response_channel` (`call`), binary frames and every text frame are on
//! `frames()`, and `events()` says what the link is doing. After an
//! unexpected close the socket reconnects with exponential backoff; each
//! attempt runs the registration again, since connection ids are
//! per-connect, and runs in its own task, so a panic inside one attempt
//! (a TLS library refusing to start) is logged and retried instead of
//! ending the link for the life of the process.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{broadcast, mpsc, oneshot, watch, Notify};
use tokio_tungstenite::tungstenite::{
    self,
    client::IntoClientRequest,
    http::{HeaderName, HeaderValue},
    Message as WsMessage,
};
use tracing::{debug, warn};

use crate::{
    client::Client,
    error::{Error, Result},
    protocol::{ConnectionFormat, ConnectionInfo, Message, ServerInfo, SocketMessage, SocketResponse},
};

/// The app's REST registration: returns the connection id to open the
/// socket with (cnctd.world: `POST client-sessions/register`).
pub type RegisterFn = Arc<dyn Fn(Client) -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync>;

/// How a connection gets its id before the upgrade. Run again on every
/// (re)connect.
#[derive(Clone)]
pub enum Registration {
    /// An app-level REST call first; the id is the upgrade query.
    Rest(RegisterFn),
    /// Register inline on the upgrade (`?subscriptions=a,b`). The id is
    /// learned from the server's `socket/connected` frame when the server
    /// sends one (`SocketConfig::with_connected_announcement`); a 0.6
    /// server never does.
    Inline {
        subscriptions: Vec<String>,
        format: ConnectionFormat,
        /// `&token=<jwt>`: honoured by a 1.0 server with a socket secret.
        token: Option<String>,
    },
}

impl Registration {
    pub fn rest<F, Fut>(f: F) -> Self
    where
        F: Fn(Client) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        Self::Rest(Arc::new(move |c| Box::pin(f(c))))
    }

    pub fn inline<I, S>(subscriptions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Inline {
            subscriptions: subscriptions.into_iter().map(Into::into).collect(),
            format: ConnectionFormat::Json,
            token: None,
        }
    }

    /// Inline only: ask for binary pushes.
    pub fn with_format(mut self, f: ConnectionFormat) -> Self {
        if let Self::Inline { format, .. } = &mut self {
            *format = f;
        }
        self
    }

    /// Inline only: authenticate at the upgrade.
    pub fn with_token(mut self, jwt: impl Into<String>) -> Self {
        if let Self::Inline { token, .. } = &mut self {
            *token = Some(jwt.into());
        }
        self
    }

    /// The upgrade query, and the connection id when this registration
    /// already knows it.
    async fn query(&self, client: &Client) -> Result<(String, Option<String>)> {
        match self {
            Self::Rest(f) => {
                let id = f(client.clone()).await?;
                if id.is_empty() {
                    return Err(Error::Registration("the registration returned no connection id".into()));
                }
                let q = url::form_urlencoded::Serializer::new(String::new()).append_pair("connection_id", &id).finish();
                Ok((q, Some(id)))
            }
            Self::Inline { subscriptions, format, token } => {
                let mut q = url::form_urlencoded::Serializer::new(String::new());
                q.append_pair("subscriptions", &subscriptions.join(","));
                if *format == ConnectionFormat::Binary {
                    q.append_pair("format", "binary");
                }
                if let Some(t) = token {
                    q.append_pair("token", t);
                }
                Ok((q.finish(), None))
            }
        }
    }
}

/// Reconnect delays: `min`, doubling per consecutive failure, capped at
/// `max`. A connection that was up and dropped retries once right away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    pub min: Duration,
    pub max: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self { min: Duration::from_secs(5), max: Duration::from_secs(60) }
    }
}

impl Backoff {
    fn delay(&self, failures: u32) -> Duration {
        let factor = 2u32.saturating_pow(failures.min(16));
        self.min.saturating_mul(factor).min(self.max)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    Disconnected,
    Connecting,
    /// Open. The id is `None` for an inline registration until the server
    /// announces it (if it ever does).
    Connected { connection_id: Option<String> },
}

#[derive(Debug, Clone)]
pub enum Event {
    Open { connection_id: Option<String> },
    Close { reason: String, will_reconnect: bool },
    Reconnecting { attempt: u32, delay: Duration },
    /// A `server-info` heartbeat (only for connections subscribed to it).
    Heartbeat(ServerInfo),
}

/// Every frame the server sends, after `call` replies are taken out.
#[derive(Debug, Clone)]
pub enum Frame {
    Text(Value),
    Binary(Vec<u8>),
}

enum Outbound {
    Text(String),
    Binary(Vec<u8>),
}

struct Shared {
    client: Client,
    registration: Registration,
    url: String,
    headers: Vec<(String, String)>,
    backoff: Option<Backoff>,
    keepalive: Option<Duration>,
    read_timeout: Option<Duration>,
    connect_timeout: Duration,
    call_timeout: Duration,
    tx: Mutex<Option<mpsc::UnboundedSender<Outbound>>>,
    pending: Mutex<HashMap<String, oneshot::Sender<Value>>>,
    channels: Mutex<HashMap<String, broadcast::Sender<Message>>>,
    frames: broadcast::Sender<Frame>,
    events: broadcast::Sender<Event>,
    state: watch::Sender<State>,
    closed: AtomicBool,
    was_open: AtomicBool,
    close_notify: Notify,
}

const BROADCAST_CAPACITY: usize = 256;

pub struct SocketBuilder {
    client: Client,
    registration: Registration,
    url: Option<String>,
    headers: Vec<(String, String)>,
    backoff: Option<Backoff>,
    keepalive: Option<Duration>,
    read_timeout: Option<Duration>,
    connect_timeout: Duration,
    call_timeout: Duration,
}

impl SocketBuilder {
    pub(crate) fn new(client: Client, registration: Registration) -> Self {
        Self {
            client,
            registration,
            url: None,
            headers: Vec::new(),
            backoff: Some(Backoff::default()),
            keepalive: Some(Duration::from_secs(30)),
            read_timeout: Some(Duration::from_secs(90)),
            connect_timeout: Duration::from_secs(10),
            call_timeout: Duration::from_secs(10),
        }
    }

    /// `None` = never reconnect; the socket ends after its first close.
    /// Default: 5 s doubling to 60 s.
    pub fn reconnect(mut self, backoff: Option<Backoff>) -> Self {
        self.backoff = backoff;
        self
    }

    /// A WebSocket ping this often while open, so nothing in front of the
    /// server takes an idle socket for a dead one (something in front of
    /// cnctd.world reset idle sockets ~125 s after connect, with no close
    /// handshake). Default 30 s; `None` to leave it to the server (1.0
    /// servers ping on their own, 0.6 servers do not).
    pub fn keepalive(mut self, every: Option<Duration>) -> Self {
        self.keepalive = every;
        self
    }

    /// No frame of any kind for this long = the socket is dead; close and
    /// reconnect. Default 90 s; `None` to wait forever.
    pub fn read_timeout(mut self, after: Option<Duration>) -> Self {
        self.read_timeout = after;
        self
    }

    /// The registration call plus the upgrade. Default 10 s.
    pub fn connect_timeout(mut self, d: Duration) -> Self {
        self.connect_timeout = d;
        self
    }

    /// Default timeout for `call`. 10 s.
    pub fn call_timeout(mut self, d: Duration) -> Self {
        self.call_timeout = d;
        self
    }

    /// The socket URL (`wss://host/ws`, no query), when it is not the
    /// client's.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// A header on the upgrade request only (the client's headers are
    /// sent on the upgrade as well).
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Spawn the link. It connects now and keeps itself connected until
    /// `close()`; the handle is cheap to clone.
    pub fn start(self) -> Result<Socket> {
        let url = match self.url {
            Some(u) => u,
            None => self.client.socket_url()?,
        };
        let mut headers = self.client.headers().to_vec();
        headers.extend(self.headers);
        let (frames, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (events, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (state, _) = watch::channel(State::Disconnected);
        let shared = Arc::new(Shared {
            client: self.client,
            registration: self.registration,
            url,
            headers,
            backoff: self.backoff,
            keepalive: self.keepalive,
            read_timeout: self.read_timeout,
            connect_timeout: self.connect_timeout,
            call_timeout: self.call_timeout,
            tx: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            channels: Mutex::new(HashMap::new()),
            frames,
            events,
            state,
            closed: AtomicBool::new(false),
            was_open: AtomicBool::new(false),
            close_notify: Notify::new(),
        });
        tokio::spawn(supervise(Arc::clone(&shared)));
        Ok(Socket { shared })
    }
}

/// The handle to a running link. Clone it freely; `close()` ends the link
/// for every clone. Dropping the last handle does NOT close it.
#[derive(Clone)]
pub struct Socket {
    shared: Arc<Shared>,
}

impl Socket {
    pub fn state(&self) -> watch::Receiver<State> {
        self.shared.state.subscribe()
    }

    pub fn current_state(&self) -> State {
        self.shared.state.borrow().clone()
    }

    pub fn connected(&self) -> bool {
        matches!(*self.shared.state.borrow(), State::Connected { .. })
    }

    /// The current connection id (the client's; `None` while closed).
    pub fn connection_id(&self) -> Option<String> {
        self.shared.client.connection_id()
    }

    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.shared.events.subscribe()
    }

    pub fn frames(&self) -> broadcast::Receiver<Frame> {
        self.shared.frames.subscribe()
    }

    /// Every push on `channel`, as a `Message`. A receiver that falls behind
    /// by more than the buffer drops the oldest (`RecvError::Lagged`).
    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<Message> {
        let mut channels = self.shared.channels.lock().unwrap_or_else(|p| p.into_inner());
        channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(BROADCAST_CAPACITY).0)
            .subscribe()
    }

    /// Fire-and-forget: `{channel, instruction, data}`. Fails when not
    /// connected; nothing is queued.
    pub fn send(&self, channel: &str, instruction: Option<&str>, data: impl Serialize) -> Result<()> {
        let msg = SocketMessage {
            channel: channel.to_string(),
            instruction: instruction.map(String::from),
            data: Some(serde_json::to_value(data)?),
            response_channel: None,
        };
        self.send_text(serde_json::to_string(&msg)?)
    }

    /// Any object, as is (an app's own frame shape).
    pub fn send_frame(&self, frame: &impl Serialize) -> Result<()> {
        self.send_text(serde_json::to_string(frame)?)
    }

    pub fn send_binary(&self, bytes: Vec<u8>) -> Result<()> {
        self.send_outbound(Outbound::Binary(bytes))
    }

    /// Send with a fresh `response_channel` and resolve with the frame that
    /// answers on it, as a `SocketResponse` (the house reply shape; use
    /// `call_raw` for an app's own).
    pub async fn call(&self, channel: &str, instruction: Option<&str>, data: impl Serialize) -> Result<SocketResponse> {
        let reply = self.call_raw(channel, instruction, data).await?;
        serde_json::from_value(reply).map_err(|e| Error::Socket(format!("reply is not a SocketResponse: {e}")))
    }

    pub async fn call_raw(&self, channel: &str, instruction: Option<&str>, data: impl Serialize) -> Result<Value> {
        let response_channel = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.shared.pending.lock().unwrap_or_else(|p| p.into_inner()).insert(response_channel.clone(), tx);
        let msg = SocketMessage {
            channel: channel.to_string(),
            instruction: instruction.map(String::from),
            data: Some(serde_json::to_value(data)?),
            response_channel: Some(response_channel.clone()),
        };
        let text = serde_json::to_string(&msg)?;
        if let Err(e) = self.send_text(text) {
            self.forget(&response_channel);
            return Err(e);
        }
        match tokio::time::timeout(self.shared.call_timeout, rx).await {
            Ok(Ok(frame)) => Ok(frame),
            Ok(Err(_)) => Err(Error::Socket("the socket closed before the reply".into())),
            Err(_) => {
                self.forget(&response_channel);
                Err(Error::Timeout(self.shared.call_timeout))
            }
        }
    }

    /// Resolve once the link is open, or fail after `timeout`.
    pub async fn wait_connected(&self, timeout: Duration) -> Result<()> {
        let mut rx = self.shared.state.subscribe();
        let wait = async {
            loop {
                if matches!(*rx.borrow(), State::Connected { .. }) {
                    return Ok(());
                }
                if rx.changed().await.is_err() {
                    return Err(Error::NotConnected);
                }
            }
        };
        tokio::time::timeout(timeout, wait).await.map_err(|_| Error::Timeout(timeout))?
    }

    /// Close for good: no reconnect.
    pub fn close(&self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        self.shared.close_notify.notify_one();
    }

    fn forget(&self, response_channel: &str) {
        self.shared.pending.lock().unwrap_or_else(|p| p.into_inner()).remove(response_channel);
    }

    fn send_text(&self, text: String) -> Result<()> {
        self.send_outbound(Outbound::Text(text))
    }

    fn send_outbound(&self, out: Outbound) -> Result<()> {
        let guard = self.shared.tx.lock().unwrap_or_else(|p| p.into_inner());
        match guard.as_ref() {
            Some(tx) if self.connected() => tx.send(out).map_err(|_| Error::NotConnected),
            _ => Err(Error::NotConnected),
        }
    }
}

// ------------------------------------------------------------------ the link

async fn supervise(shared: Arc<Shared>) {
    let mut failures: u32 = 0;
    loop {
        if shared.closed.load(Ordering::SeqCst) {
            break;
        }
        shared.was_open.store(false, Ordering::SeqCst);
        shared.state.send_replace(State::Connecting);
        let attempt = tokio::spawn(run_once(Arc::clone(&shared)));
        let result = match attempt.await {
            Ok(r) => r,
            Err(e) => Err(Error::Socket(format!("the connect attempt crashed: {e}"))),
        };
        let was_open = shared.was_open.load(Ordering::SeqCst);

        *shared.tx.lock().unwrap_or_else(|p| p.into_inner()) = None;
        shared.pending.lock().unwrap_or_else(|p| p.into_inner()).clear();
        shared.client.set_connection_id(None);
        shared.state.send_replace(State::Disconnected);

        let reason = match &result {
            Ok(()) => "closed".to_string(),
            Err(e) => e.to_string(),
        };
        let will_reconnect = !shared.closed.load(Ordering::SeqCst) && shared.backoff.is_some();
        if let Err(e) = &result {
            warn!("socket: {e}{}", if will_reconnect { "; reconnecting" } else { "" });
        }
        let _ = shared.events.send(Event::Close { reason, will_reconnect });
        if !will_reconnect {
            break;
        }

        let backoff = shared.backoff.unwrap_or_default();
        if was_open {
            failures = 0;
        }
        let delay = if was_open { Duration::ZERO } else { backoff.delay(failures) };
        failures = failures.saturating_add(1);
        let _ = shared.events.send(Event::Reconnecting { attempt: failures, delay });
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shared.close_notify.notified() => {}
        }
    }
    shared.state.send_replace(State::Disconnected);
}

enum Read {
    Frame(WsMessage),
    Error(String),
    Closed,
    TimedOut,
}

async fn run_once(shared: Arc<Shared>) -> Result<()> {
    let (query, known_id) = tokio::time::timeout(shared.connect_timeout, shared.registration.query(&shared.client))
        .await
        .map_err(|_| Error::Timeout(shared.connect_timeout))??;
    shared.client.set_connection_id(known_id.clone());

    let url = format!("{}?{}", shared.url, query);
    let mut request = url.as_str().into_client_request().map_err(|e| Error::Socket(format!("{url}: {e}")))?;
    for (k, v) in &shared.headers {
        let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| Error::Socket(format!("header {k}: {e}")))?;
        let value = HeaderValue::from_str(v).map_err(|e| Error::Socket(format!("header {k}: {e}")))?;
        request.headers_mut().insert(name, value);
    }
    let (ws, _) = tokio::time::timeout(shared.connect_timeout, tokio_tungstenite::connect_async(request))
        .await
        .map_err(|_| Error::Timeout(shared.connect_timeout))?
        .map_err(upgrade_error)?;
    let (mut sink, mut stream) = ws.split();

    let (tx, mut rx) = mpsc::unbounded_channel::<Outbound>();
    *shared.tx.lock().unwrap_or_else(|p| p.into_inner()) = Some(tx);
    shared.was_open.store(true, Ordering::SeqCst);
    shared.state.send_replace(State::Connected { connection_id: known_id.clone() });
    let _ = shared.events.send(Event::Open { connection_id: known_id });
    debug!(url = shared.url, "socket open");

    let mut keepalive = shared.keepalive.map(|every| {
        let mut i = tokio::time::interval(every);
        i.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        i
    });
    if let Some(i) = keepalive.as_mut() {
        i.tick().await; // the immediate first tick
    }

    loop {
        tokio::select! {
            read = read_next(&mut stream, shared.read_timeout) => match read {
                Read::Frame(WsMessage::Text(text)) => handle_text(&shared, text.as_str()),
                Read::Frame(WsMessage::Binary(bytes)) => {
                    let _ = shared.frames.send(Frame::Binary(bytes.to_vec()));
                }
                Read::Frame(WsMessage::Ping(payload)) => {
                    sink.send(WsMessage::Pong(payload)).await.map_err(|e| Error::Socket(format!("pong: {e}")))?;
                }
                Read::Frame(WsMessage::Pong(_)) | Read::Frame(WsMessage::Frame(_)) => {}
                Read::Frame(WsMessage::Close(_)) | Read::Closed => return Err(Error::Socket("the server closed the socket".into())),
                Read::Error(e) => return Err(Error::Socket(format!("read: {e}"))),
                Read::TimedOut => return Err(Error::Socket(format!(
                    "no frame for {}s", shared.read_timeout.map(|d| d.as_secs()).unwrap_or(0)
                ))),
            },
            out = rx.recv() => match out {
                Some(Outbound::Text(s)) => sink.send(WsMessage::Text(s.into())).await.map_err(|e| Error::Socket(format!("write: {e}")))?,
                Some(Outbound::Binary(b)) => sink.send(WsMessage::Binary(b.into())).await.map_err(|e| Error::Socket(format!("write: {e}")))?,
                None => return Ok(()),
            },
            _ = tick(&mut keepalive) => {
                sink.send(WsMessage::Ping(Vec::new().into())).await.map_err(|e| Error::Socket(format!("ping: {e}")))?;
            }
            _ = shared.close_notify.notified() => {
                let _ = sink.send(WsMessage::Close(None)).await;
                return Ok(());
            }
        }
    }
}

async fn read_next<S>(stream: &mut S, timeout: Option<Duration>) -> Read
where
    S: futures_util::Stream<Item = tungstenite::Result<WsMessage>> + Unpin,
{
    let next = async {
        match stream.next().await {
            Some(Ok(frame)) => Read::Frame(frame),
            Some(Err(e)) => Read::Error(e.to_string()),
            None => Read::Closed,
        }
    };
    match timeout {
        Some(t) => tokio::time::timeout(t, next).await.unwrap_or(Read::TimedOut),
        None => next.await,
    }
}

async fn tick(interval: &mut Option<tokio::time::Interval>) {
    match interval {
        Some(i) => {
            i.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

fn handle_text(shared: &Shared, text: &str) {
    let value: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => {
            debug!("dropped a text frame that is not JSON");
            return;
        }
    };

    // A reply to `call`? (`response_resource` is the legacy spelling.)
    let reply_to = value
        .get("response_channel")
        .or_else(|| value.get("response_resource"))
        .and_then(Value::as_str)
        .map(String::from);
    if let Some(key) = reply_to {
        let waiting = shared.pending.lock().unwrap_or_else(|p| p.into_inner()).remove(&key);
        if let Some(tx) = waiting {
            let _ = tx.send(value);
            return;
        }
    }

    let _ = shared.frames.send(Frame::Text(value.clone()));

    let Ok(msg) = serde_json::from_value::<Message>(value) else { return };
    match (msg.channel.as_str(), msg.instruction.as_str()) {
        ("server-info", "heartbeat") => {
            if let Some(info) = msg.data.as_ref().and_then(|d| serde_json::from_value::<ServerInfo>(d.clone()).ok()) {
                let _ = shared.events.send(Event::Heartbeat(info));
            }
        }
        ("socket", "connected") => {
            if let Some(info) = msg.data.as_ref().and_then(|d| serde_json::from_value::<ConnectionInfo>(d.clone()).ok()) {
                shared.client.set_connection_id(Some(info.connection_id.clone()));
                shared.state.send_replace(State::Connected { connection_id: Some(info.connection_id) });
            }
        }
        _ => {}
    }
    let channels = shared.channels.lock().unwrap_or_else(|p| p.into_inner());
    if let Some(tx) = channels.get(&msg.channel) {
        let _ = tx.send(msg);
    }
}

/// A refused upgrade carries its status and, from a 1.0 server, an error
/// envelope; a 0.6 server answers some refusals with an HTML page.
fn upgrade_error(e: tungstenite::Error) -> Error {
    match e {
        tungstenite::Error::Http(resp) => {
            let status = resp.status().as_u16();
            let body = resp.body().as_deref().map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default();
            let msg = serde_json::from_str::<Value>(&body)
                .ok()
                .and_then(|v| v.get("msg").and_then(Value::as_str).map(String::from))
                .unwrap_or_else(|| body.chars().take(120).collect());
            Error::Socket(format!("upgrade refused ({status}): {msg}"))
        }
        other => Error::Socket(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_caps() {
        let b = Backoff { min: Duration::from_secs(5), max: Duration::from_secs(60) };
        assert_eq!(b.delay(0), Duration::from_secs(5));
        assert_eq!(b.delay(1), Duration::from_secs(10));
        assert_eq!(b.delay(3), Duration::from_secs(40));
        assert_eq!(b.delay(4), Duration::from_secs(60));
        assert_eq!(b.delay(40), Duration::from_secs(60));
    }

    #[tokio::test]
    async fn registration_queries() {
        let c = Client::builder("http://127.0.0.1:1").build().unwrap();
        let (q, id) = Registration::rest(|_| async { Ok("abc def".to_string()) }).query(&c).await.unwrap();
        assert_eq!((q.as_str(), id.as_deref()), ("connection_id=abc+def", Some("abc def")));
        let (q, id) = Registration::inline(["server-info", "news"])
            .with_format(ConnectionFormat::Binary)
            .with_token("t.k")
            .query(&c)
            .await
            .unwrap();
        assert_eq!(q, "subscriptions=server-info%2Cnews&format=binary&token=t.k");
        assert!(id.is_none());
        assert!(Registration::rest(|_| async { Ok(String::new()) }).query(&c).await.is_err());
    }
}

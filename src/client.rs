//! REST: `METHOD /<prefix>/<resource>/<operation>` with the JSON envelope.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use serde::Serialize;
use serde_json::{json, Value};
use tracing::debug;

use crate::{
    error::{ApiError, Error, Result},
    protocol::{ErrorCode, HttpMethod, SuccessCode, SuccessResponse},
    socket::{Registration, SocketBuilder},
};

/// Produces the `Authorization` header value for a request, or `None` for
/// no header. Called on every request, so it may refresh. Sent VERBATIM:
/// a raw token or `Bearer <token>` is the caller's choice, the server
/// passes it to the app as is.
pub type TokenFn = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Option<String>> + Send>> + Send + Sync>;

/// A non-envelope answer: raw bytes with their content type (files, MCP,
/// images, a redirect's target).
#[derive(Debug, Clone)]
pub struct RawResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

pub struct ClientBuilder {
    base_url: String,
    api_prefix: String,
    socket_path: String,
    socket_url: Option<String>,
    timeout: Duration,
    headers: Vec<(String, String)>,
    token: Option<TokenFn>,
    user_agent: Option<String>,
}

impl ClientBuilder {
    /// `https://cnctd.world`: scheme and host, no path.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_prefix: "api".into(),
            socket_path: "/ws".into(),
            socket_url: None,
            timeout: Duration::from_secs(10),
            headers: Vec::new(),
            token: None,
            user_agent: None,
        }
    }

    /// API prefix, `api` by default.
    pub fn api_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.api_prefix = prefix.into();
        self
    }

    /// Socket path on the base URL, `/ws` by default.
    pub fn socket_path(mut self, path: impl Into<String>) -> Self {
        self.socket_path = path.into();
        self
    }

    /// The full socket URL when it is not the base URL with the scheme
    /// swapped (a shell that proxies HTTP but not WebSocket).
    pub fn socket_url(mut self, url: impl Into<String>) -> Self {
        self.socket_url = Some(url.into());
        self
    }

    /// Per-request timeout for HTTP. 10 s by default.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// A header on every request AND on the socket upgrade (Cloudflare
    /// Access credentials, an app's own identification).
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self
    }

    /// How the `Authorization` value is obtained; see [`TokenFn`].
    pub fn token<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<String>> + Send + 'static,
    {
        self.token = Some(Arc::new(move || Box::pin(f())));
        self
    }

    pub fn build(self) -> Result<Client> {
        let base = url::Url::parse(&self.base_url).map_err(|e| Error::Url(format!("{}: {e}", self.base_url)))?;
        if !matches!(base.scheme(), "http" | "https") {
            return Err(Error::Url(format!("{}: must be http or https", self.base_url)));
        }
        let mut http = reqwest::Client::builder().timeout(self.timeout);
        if let Some(ua) = &self.user_agent {
            http = http.user_agent(ua.clone());
        }
        let http = http.build()?;
        Ok(Client {
            inner: Arc::new(Inner {
                base_url: self.base_url.trim_end_matches('/').to_string(),
                api_prefix: self.api_prefix.trim_matches('/').to_string(),
                socket_path: self.socket_path,
                socket_url: self.socket_url,
                http,
                headers: self.headers,
                token: self.token,
                connection_id: RwLock::new(None),
            }),
        })
    }
}

struct Inner {
    base_url: String,
    api_prefix: String,
    socket_path: String,
    socket_url: Option<String>,
    http: reqwest::Client,
    headers: Vec<(String, String)>,
    token: Option<TokenFn>,
    connection_id: RwLock<Option<String>>,
}

/// A client for one cnctd_server. Cheap to clone; clones share the
/// connection id, so the socket's id reaches every REST call.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    pub fn builder(base_url: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(base_url)
    }

    pub fn base_url(&self) -> &str {
        &self.inner.base_url
    }

    /// This client's WebSocket connection id, once known. Sent as
    /// `Client-Id` on every REST call. Per-connect: the socket sets it on
    /// open and clears it on close.
    pub fn connection_id(&self) -> Option<String> {
        self.inner.connection_id.read().ok().and_then(|g| g.clone())
    }

    pub fn set_connection_id(&self, id: Option<String>) {
        if let Ok(mut g) = self.inner.connection_id.write() {
            *g = id;
        }
    }

    pub(crate) fn headers(&self) -> &[(String, String)] {
        &self.inner.headers
    }

    /// `ws(s)://host/ws`: the override, or the base URL with the scheme swapped.
    pub fn socket_url(&self) -> Result<String> {
        if let Some(u) = &self.inner.socket_url {
            return Ok(u.clone());
        }
        let mut u = url::Url::parse(&self.inner.base_url).map_err(|e| Error::Url(e.to_string()))?;
        let scheme = if u.scheme() == "https" { "wss" } else { "ws" };
        u.set_scheme(scheme).map_err(|_| Error::Url("cannot derive the socket scheme".into()))?;
        let path = if self.inner.socket_path.starts_with('/') {
            self.inner.socket_path.clone()
        } else {
            format!("/{}", self.inner.socket_path)
        };
        u.set_path(&path);
        u.set_query(None);
        u.set_fragment(None);
        Ok(u.to_string())
    }

    /// `${base}/${prefix}/${resource}/${operation}`; GET and DELETE data
    /// becomes the query string (flat `key=value`, nested values JSON-encoded,
    /// nulls skipped: what the server's lenient query parsing expects).
    pub fn url(&self, method: HttpMethod, resource: &str, operation: &str, data: &Value) -> String {
        let mut url = format!("{}/{}/{}", self.inner.base_url, self.inner.api_prefix, resource);
        if !operation.is_empty() {
            url.push('/');
            url.push_str(operation);
        }
        if matches!(method, HttpMethod::GET | HttpMethod::DELETE) {
            let qs = to_query(data);
            if !qs.is_empty() {
                url.push('?');
                url.push_str(&qs);
            }
        }
        url
    }

    pub async fn get(&self, resource: &str, operation: &str, data: impl Serialize) -> Result<SuccessResponse> {
        self.request(HttpMethod::GET, resource, operation, serde_json::to_value(data)?).await
    }

    pub async fn post(&self, resource: &str, operation: &str, data: impl Serialize) -> Result<SuccessResponse> {
        self.request(HttpMethod::POST, resource, operation, serde_json::to_value(data)?).await
    }

    pub async fn put(&self, resource: &str, operation: &str, data: impl Serialize) -> Result<SuccessResponse> {
        self.request(HttpMethod::PUT, resource, operation, serde_json::to_value(data)?).await
    }

    pub async fn delete(&self, resource: &str, operation: &str, data: impl Serialize) -> Result<SuccessResponse> {
        self.request(HttpMethod::DELETE, resource, operation, serde_json::to_value(data)?).await
    }

    /// One call. A rejected request is an `Error::Api`: EITHER an HTTP
    /// status of 400 or more OR an envelope with `success: false` rejects,
    /// so a proxy or error page that mangles one still fails closed. A
    /// non-envelope 2xx body (a raw route) comes back as `data`: parsed
    /// when it is JSON, else the text.
    pub async fn request(&self, method: HttpMethod, resource: &str, operation: &str, data: Value) -> Result<SuccessResponse> {
        let raw = self.send(method, resource, operation, data).await?;
        let parsed: Option<Value> = serde_json::from_slice(&raw.body).ok();
        if let Some(err) = rejection(&raw, parsed.as_ref(), method, resource, operation) {
            return Err(err.into());
        }
        if let Some(obj) = parsed.as_ref().filter(|v| v.get("success").is_some()) {
            return Ok(SuccessResponse {
                success: true,
                status: serde_json::from_value(obj.get("status").cloned().unwrap_or(Value::Null)).unwrap_or(SuccessCode::OK),
                msg: obj.get("msg").and_then(Value::as_str).map(String::from),
                data: obj.get("data").cloned().filter(|d| !d.is_null()),
            });
        }
        let data = match parsed {
            Some(v) => Some(v),
            None if raw.body.is_empty() => None,
            None => Some(Value::String(String::from_utf8_lossy(&raw.body).into_owned())),
        };
        Ok(SuccessResponse {
            success: true,
            status: SuccessCode::from_u16(raw.status),
            msg: None,
            data,
        })
    }

    /// One call whose answer is bytes, not an envelope. A rejection is still
    /// an `Error::Api`.
    pub async fn raw(&self, method: HttpMethod, resource: &str, operation: &str, data: Value) -> Result<RawResponse> {
        let raw = self.send(method, resource, operation, data).await?;
        if raw.status >= 400 {
            let parsed: Option<Value> = serde_json::from_slice(&raw.body).ok();
            if let Some(err) = rejection(&raw, parsed.as_ref(), method, resource, operation) {
                return Err(err.into());
            }
        }
        Ok(raw)
    }

    async fn send(&self, method: HttpMethod, resource: &str, operation: &str, data: Value) -> Result<RawResponse> {
        let url = self.url(method, resource, operation, &data);
        let http = &self.inner.http;
        let body = if data.is_null() { json!({}) } else { data };
        let mut req = match method {
            HttpMethod::GET => http.get(&url),
            HttpMethod::DELETE => http.delete(&url),
            HttpMethod::POST => http.post(&url).json(&body),
            HttpMethod::PUT => http.put(&url).json(&body),
        };
        for (k, v) in &self.inner.headers {
            req = req.header(k, v);
        }
        if let Some(id) = self.connection_id() {
            req = req.header("Client-Id", id);
        }
        if let Some(token) = &self.inner.token {
            if let Some(t) = token().await {
                req = req.header("Authorization", t);
            }
        }
        debug!(method = method.as_str(), url, "request");
        let resp = req.send().await?;
        let status = resp.status().as_u16();
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let body = resp.bytes().await?.to_vec();
        Ok(RawResponse { status, content_type, body })
    }

    /// The socket for this client. `start()` it; it reconnects on its own.
    pub fn socket(&self, registration: Registration) -> SocketBuilder {
        SocketBuilder::new(self.clone(), registration)
    }
}

/// `Error::Api` when the answer is a rejection, by status or by envelope.
fn rejection(raw: &RawResponse, parsed: Option<&Value>, method: HttpMethod, resource: &str, operation: &str) -> Option<ApiError> {
    let envelope = parsed.filter(|v| v.is_object());
    let said_no = envelope.and_then(|v| v.get("success")).and_then(Value::as_bool) == Some(false);
    if raw.status < 400 && !said_no {
        return None;
    }
    let word = envelope
        .and_then(|v| v.get("status"))
        .and_then(Value::as_str)
        .map(String::from)
        .unwrap_or_else(|| status_word(raw.status));
    Some(ApiError {
        http_status: raw.status,
        status: word,
        msg: envelope.and_then(|v| v.get("msg")).and_then(Value::as_str).map(String::from),
        method: method.as_str().to_string(),
        resource: resource.to_string(),
        operation: operation.to_string(),
    })
}

/// The protocol's word for an HTTP status when the body had none; the bare
/// number when the protocol has no word for it.
fn status_word(status: u16) -> String {
    let code = ErrorCode::from_u16(status);
    if code.as_u16() == status {
        format!("{code:?}")
    } else {
        status.to_string()
    }
}

trait SuccessCodeExt {
    fn from_u16(status: u16) -> SuccessCode;
}

impl SuccessCodeExt for SuccessCode {
    fn from_u16(status: u16) -> SuccessCode {
        match status {
            201 => SuccessCode::Created,
            202 => SuccessCode::Accepted,
            204 => SuccessCode::NoContent,
            _ => SuccessCode::OK,
        }
    }
}

/// Flat `key=value` pairs; nested values JSON-encoded; null skipped.
pub fn to_query(data: &Value) -> String {
    let Some(obj) = data.as_object() else { return String::new() };
    let mut qs = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in obj {
        match v {
            Value::Null => {}
            Value::String(s) => {
                qs.append_pair(k, s);
            }
            Value::Bool(_) | Value::Number(_) => {
                qs.append_pair(k, &v.to_string());
            }
            other => {
                qs.append_pair(k, &other.to_string());
            }
        }
    }
    qs.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        let c = Client::builder("https://cnctd.world/").build().unwrap();
        assert_eq!(c.url(HttpMethod::POST, "dots", "all", &json!({"a": 1})), "https://cnctd.world/api/dots/all");
        assert_eq!(
            c.url(HttpMethod::GET, "echo", "q", &json!({"x": 1, "y": "two", "z": null, "n": {"a": 1}})),
            "https://cnctd.world/api/echo/q?n=%7B%22a%22%3A1%7D&x=1&y=two"
        );
        assert_eq!(c.url(HttpMethod::GET, "echo", "", &json!({})), "https://cnctd.world/api/echo");
        assert_eq!(c.socket_url().unwrap(), "wss://cnctd.world/ws");
        let c = Client::builder("http://127.0.0.1:9000").socket_path("socket").build().unwrap();
        assert_eq!(c.socket_url().unwrap(), "ws://127.0.0.1:9000/socket");
        let c = Client::builder("http://127.0.0.1:9000").socket_url("wss://cnctd.world/ws").build().unwrap();
        assert_eq!(c.socket_url().unwrap(), "wss://cnctd.world/ws");
        assert!(Client::builder("cnctdstudio://app").build().is_err());
    }

    #[test]
    fn rejection_reads_either_signal() {
        let raw = RawResponse { status: 200, content_type: None, body: b"{\"success\":false,\"status\":\"NotFound\",\"msg\":\"no\"}".to_vec() };
        let parsed: Value = serde_json::from_slice(&raw.body).unwrap();
        let e = rejection(&raw, Some(&parsed), HttpMethod::GET, "a", "b").unwrap();
        assert_eq!((e.http_status, e.status.as_str(), e.msg.as_deref()), (200, "NotFound", Some("no")));

        let raw = RawResponse { status: 502, content_type: None, body: b"<html>bad gateway</html>".to_vec() };
        let e = rejection(&raw, None, HttpMethod::GET, "a", "b").unwrap();
        assert_eq!((e.http_status, e.status.as_str()), (502, "BadGateway"));
        assert_eq!(e.to_string(), "GET /a/b failed (502 BadGateway)");

        let raw = RawResponse { status: 418, content_type: None, body: Vec::new() };
        assert_eq!(rejection(&raw, None, HttpMethod::GET, "a", "b").unwrap().status, "418");

        let raw = RawResponse { status: 200, content_type: None, body: b"BIN".to_vec() };
        assert!(rejection(&raw, None, HttpMethod::GET, "a", "b").is_none());
    }
}

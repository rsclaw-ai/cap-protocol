//! Minimal A2A HTTPS+SSE driver and CAP event mapping helpers.

use std::collections::VecDeque;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::core::{AgentEvent, ClientFrame};
use crate::driver::{Driver, DriverError, DriverExitStatus, text_content};
use crate::wire::to_strict_events;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCard {
    pub name: String,
    #[serde(default)]
    pub extensions: Vec<AgentExtension>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExtension {
    pub uri: String,
    #[serde(default)]
    pub required: bool,
}

impl AgentCard {
    pub fn supports_cap_v1(&self) -> bool {
        self.extensions.iter().any(|e| e.uri == "cap-protocol/v1")
    }
}

pub fn parse_sse_events(sse: &str) -> Result<Vec<AgentEvent>, serde_json::Error> {
    let mut events = VecDeque::new();
    let mut parser = SseParser::default();
    parser.push_bytes(sse.as_bytes(), &mut events)?;
    parser.finish(&mut events)?;
    Ok(events.into_iter().collect())
}
#[derive(Debug, Default)]
struct SseParser {
    data: String,
    line: String,
}

impl SseParser {
    fn push_bytes(
        &mut self,
        bytes: &[u8],
        events: &mut VecDeque<AgentEvent>,
    ) -> Result<(), serde_json::Error> {
        for ch in String::from_utf8_lossy(bytes).chars() {
            if ch == '\n' {
                let line = self.line.trim_end_matches('\r').to_string();
                self.line.clear();
                self.push_line(&line, events)?;
            } else {
                self.line.push(ch);
            }
        }
        Ok(())
    }

    fn push_line(
        &mut self,
        line: &str,
        events: &mut VecDeque<AgentEvent>,
    ) -> Result<(), serde_json::Error> {
        if let Some(rest) = line.strip_prefix("data:") {
            if !self.data.is_empty() {
                self.data.push('\n');
            }
            self.data.push_str(rest.trim_start());
        } else if line.trim().is_empty() {
            self.flush(events)?;
        }
        Ok(())
    }

    fn finish(mut self, events: &mut VecDeque<AgentEvent>) -> Result<(), serde_json::Error> {
        if !self.line.is_empty() {
            let line = std::mem::take(&mut self.line);
            self.push_line(line.trim_end_matches('\r'), events)?;
        }
        self.flush(events)
    }

    fn flush(&mut self, events: &mut VecDeque<AgentEvent>) -> Result<(), serde_json::Error> {
        if !self.data.trim().is_empty() {
            if let Some(event) =
                a2a_stream_response_to_core_event(serde_json::from_str(&self.data)?)?
            {
                events.push_back(event);
            }
            self.data.clear();
        }
        Ok(())
    }
}

fn a2a_stream_response_to_core_event(
    value: Value,
) -> Result<Option<AgentEvent>, serde_json::Error> {
    if value
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|k| k.starts_with("cap."))
    {
        return serde_json::from_value(value).map(Some);
    }

    if let Some(event) = cap_data_part_to_core_event(&value)? {
        return Ok(Some(event));
    }

    if let Some(parts) = value.pointer("/message/parts").and_then(Value::as_array)
        && let Some(text) = parts.iter().find_map(|part| {
            (part.get("kind").or_else(|| part.get("type"))?.as_str()? == "text")
                .then(|| part.get("text")?.as_str())
                .flatten()
        })
    {
        return Ok(Some(AgentEvent::TextChunk {
            msg_id: value
                .pointer("/message/messageId")
                .or_else(|| value.pointer("/message/message_id"))
                .and_then(Value::as_str)
                .unwrap_or("a2a-message")
                .to_string(),
            text: text.to_string(),
            channel: crate::core::TextChannel::Assistant,
        }));
    }

    if value.pointer("/task/status/state").and_then(Value::as_str) == Some("failed") {
        return Ok(Some(AgentEvent::Error {
            code: "a2a_task_failed".to_string(),
            message: value
                .pointer("/task/status/message")
                .and_then(Value::as_str)
                .unwrap_or("A2A task failed")
                .to_string(),
            retryable: false,
            details: Some(value),
        }));
    }

    Ok(None)
}

fn cap_data_part_to_core_event(value: &Value) -> Result<Option<AgentEvent>, serde_json::Error> {
    let parts = value
        .pointer("/message/parts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            value
                .pointer("/task/status/message/parts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
        .chain(
            value
                .pointer("/artifact/parts")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        );

    for part in parts {
        let Some(cap_kind) = part
            .pointer("/_meta/cap/kind")
            .and_then(Value::as_str)
            .filter(|kind| kind.starts_with("cap."))
        else {
            continue;
        };
        let Some(data) = part.get("data") else {
            continue;
        };

        let mut event = data.clone();
        if event.get("kind").and_then(Value::as_str).is_none()
            && let Some(obj) = event.as_object_mut()
        {
            obj.insert("kind".to_string(), Value::String(cap_kind.to_string()));
        }
        return serde_json::from_value(event).map(Some);
    }

    Ok(None)
}

pub fn cap_event_to_a2a_part(event: &AgentEvent) -> Option<Value> {
    let event = to_strict_events(event.clone()).into_iter().next()?;
    match &event {
        AgentEvent::TextChunk { text, .. } => Some(json!({
            "kind": "text",
            "text": text,
        })),
        other => Some(json!({
            "kind": "data",
            "data": other,
            "_meta": {
                "cap": {
                    "kind": serialized_event_kind(other)?,
                }
            }
        })),
    }
}

fn serialized_event_kind(event: &AgentEvent) -> Option<String> {
    serde_json::to_value(event)
        .ok()?
        .get("kind")?
        .as_str()
        .map(str::to_string)
}

#[derive(Debug)]
pub struct A2aDriver {
    endpoint: String,
    client: reqwest::Client,
    pending: VecDeque<AgentEvent>,
    alive: bool,
}

impl A2aDriver {
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, DriverError> {
        let endpoint = endpoint.into().trim_end_matches('/').to_string();
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(reqwest_to_driver_error)?;
        let card: AgentCard = client
            .get(format!("{endpoint}/.well-known/agent-card.json"))
            .send()
            .await
            .map_err(reqwest_to_driver_error)?
            .error_for_status()
            .map_err(reqwest_to_driver_error)?
            .json()
            .await
            .map_err(reqwest_to_driver_error)?;

        if !card.supports_cap_v1() {
            return Err(DriverError::Parse(
                "A2A AgentCard does not advertise cap-protocol/v1".to_string(),
            ));
        }

        let mut pending = VecDeque::new();
        pending.push_back(AgentEvent::Ready {
            session_id: Some(card.name),
            version: crate::core::CAP_PROTOCOL_VERSION.to_string(),
            model: None,
        });

        Ok(Self {
            endpoint,
            client,
            pending,
            alive: true,
        })
    }
}

#[async_trait::async_trait]
impl Driver for A2aDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        match frame {
            ClientFrame::SessionConfig(_) => Ok(()),
            ClientFrame::Prompt { content } => {
                let text = text_content(&content, "\n");
                let response = self
                    .client
                    .post(format!("{}/message/send", self.endpoint))
                    .json(&json!({
                        "message": {
                            "role": "user",
                            "parts": [{ "kind": "text", "text": text }]
                        }
                    }))
                    .send()
                    .await
                    .map_err(reqwest_to_driver_error)?
                    .error_for_status()
                    .map_err(reqwest_to_driver_error)?;
                let mut parser = SseParser::default();
                let mut stream = response.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    parser
                        .push_bytes(&chunk.map_err(reqwest_to_driver_error)?, &mut self.pending)
                        .map_err(|e| DriverError::Parse(e.to_string()))?;
                }
                parser
                    .finish(&mut self.pending)
                    .map_err(|e| DriverError::Parse(e.to_string()))?;
                Ok(())
            }
            ClientFrame::Cancel { .. } => {
                let _ = self
                    .client
                    .post(format!("{}/tasks/cancel", self.endpoint))
                    .send()
                    .await
                    .map_err(reqwest_to_driver_error)?
                    .error_for_status()
                    .map_err(reqwest_to_driver_error)?;
                Ok(())
            }
            other => Err(DriverError::Parse(format!(
                "A2A driver does not support client frame: {other:?}"
            ))),
        }
    }

    async fn next_event(&mut self) -> Option<AgentEvent> {
        self.pending.pop_front()
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        self.alive = false;
        Ok(())
    }

    fn is_alive(&self) -> bool {
        self.alive
    }

    fn exit_status(&self) -> Option<DriverExitStatus> {
        (!self.alive).then_some(DriverExitStatus::Disconnected)
    }

    fn prompt_after_ready(&self) -> bool {
        true
    }
}

fn reqwest_to_driver_error(err: reqwest::Error) -> DriverError {
    DriverError::Io(std::io::Error::other(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{AgentEvent, ClientFrame, Content, TextChannel};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn agent_card_must_advertise_cap_extension() {
        let card = AgentCard {
            name: "remote".into(),
            extensions: vec![AgentExtension {
                uri: "cap-protocol/v1".into(),
                required: true,
            }],
        };
        assert!(card.supports_cap_v1());
    }

    #[test]
    fn sse_data_part_maps_to_core_event() {
        let sse = r#"event: message
data: {"kind":"cap.text_chunk","message_id":"m1","text":"hi","channel":"assistant"}

"#;
        let events = parse_sse_events(sse).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::TextChunk {
                text,
                channel: TextChannel::Assistant,
                ..
            } => {
                assert_eq!(text, "hi");
            }
            other => panic!("expected text chunk, got {other:?}"),
        }
    }

    #[test]
    fn sse_cap_data_part_maps_to_core_usage_event() {
        let sse = r#"data: {"message":{"messageId":"m2","parts":[{"kind":"data","data":{"kind":"cap.usage","input_tokens":3,"output_tokens":4,"cost_usd_estimate":0.01},"_meta":{"cap":{"kind":"cap.usage"}}}]}}

"#;

        let events = parse_sse_events(sse).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::Usage { usage, .. } => {
                assert_eq!(usage.input_tokens, 3);
                assert_eq!(usage.output_tokens, 4);
                assert_eq!(usage.cost_usd_estimate, Some(0.01));
            }
            other => panic!("expected usage event, got {other:?}"),
        }
    }

    #[test]
    fn sse_cap_data_part_maps_to_core_ask_user_event() {
        let sse = r#"data: {"message":{"messageId":"m3","parts":[{"kind":"data","data":{"kind":"cap.ask_user","ask_id":"ask-1","prompt":"Continue?","ask_kind":"yes_no"},"_meta":{"cap":{"kind":"cap.ask_user"}}}]}}

"#;

        let events = parse_sse_events(sse).unwrap();

        assert!(matches!(
            &events[0],
            AgentEvent::AskUser { ask_id, prompt, .. } if ask_id == "ask-1" && prompt == "Continue?"
        ));
    }

    #[test]
    fn sse_cap_data_part_maps_to_core_permission_event() {
        let sse = r#"data: {"message":{"messageId":"m4","parts":[{"kind":"data","data":{"kind":"cap.permission.request","req_id":"perm-1","tool":"shell","intent":{"command":"cargo test"},"scope":"execute","risk_level":"medium"},"_meta":{"cap":{"kind":"cap.permission.request"}}}]}}

"#;

        let events = parse_sse_events(sse).unwrap();

        assert!(matches!(
            &events[0],
            AgentEvent::PermissionRequest { req_id, tool, .. } if req_id == "perm-1" && tool == "shell"
        ));
    }

    #[test]
    fn unknown_sse_payload_is_ignored_instead_of_done() {
        let sse = r#"data: {"kind":"statusUpdate","task":{"status":{"state":"working"}}}

"#;

        let events = parse_sse_events(sse).unwrap();

        assert!(events.is_empty());
    }

    #[test]
    fn text_chunk_maps_to_a2a_text_part() {
        let part = cap_event_to_a2a_part(&AgentEvent::TextChunk {
            msg_id: "m1".into(),
            text: "hello".into(),
            channel: TextChannel::Assistant,
        })
        .unwrap();
        assert_eq!(part["kind"], "text");
        assert_eq!(part["text"], "hello");
    }

    #[tokio::test]
    async fn driver_fetches_agent_card_and_sends_prompt_to_sse_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]);
                if req.starts_with("GET /.well-known/agent-card.json") {
                    write_response(
                        &mut socket,
                        "application/json",
                        r#"{"name":"remote","extensions":[{"uri":"cap-protocol/v1","required":true}]}"#,
                    )
                    .await;
                } else {
                    assert!(req.starts_with("POST /message/send"));
                    assert!(req.contains("ping"));
                    write_response(
                        &mut socket,
                        "text/event-stream",
                        "data: {\"kind\":\"cap.text_chunk\",\"message_id\":\"m1\",\"text\":\"pong\",\"channel\":\"assistant\"}\n\n",
                    )
                    .await;
                }
            }
        });

        let mut driver = A2aDriver::connect(format!("http://{addr}")).await.unwrap();
        assert!(matches!(
            driver.next_event().await,
            Some(AgentEvent::Ready { .. })
        ));
        driver
            .send(ClientFrame::Prompt {
                content: vec![Content::text("ping")],
            })
            .await
            .unwrap();
        assert!(matches!(
            driver.next_event().await,
            Some(AgentEvent::TextChunk { text, .. }) if text == "pong"
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn driver_handles_sse_line_split_across_chunks() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]);
                if req.starts_with("GET /.well-known/agent-card.json") {
                    write_response(
                        &mut socket,
                        "application/json",
                        r#"{"name":"remote","extensions":[{"uri":"cap-protocol/v1","required":true}]}"#,
                    )
                    .await;
                } else {
                    assert!(req.starts_with("POST /message/send"));
                    write_chunked_response(
                        &mut socket,
                        "text/event-stream",
                        &[
                            "data: {\"kind\":\"cap.text",
                            "_chunk\",\"message_id\":\"m1\",\"text\":\"pong\",\"channel\":\"assistant\"}\n\n",
                        ],
                    )
                    .await;
                }
            }
        });

        let mut driver = A2aDriver::connect(format!("http://{addr}")).await.unwrap();
        assert!(matches!(
            driver.next_event().await,
            Some(AgentEvent::Ready { .. })
        ));
        driver
            .send(ClientFrame::Prompt {
                content: vec![Content::text("ping")],
            })
            .await
            .unwrap();
        assert!(matches!(
            driver.next_event().await,
            Some(AgentEvent::TextChunk { text, .. }) if text == "pong"
        ));
        server.await.unwrap();
    }

    async fn write_chunked_response(
        socket: &mut tokio::net::TcpStream,
        content_type: &str,
        chunks: &[&str],
    ) {
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ntransfer-encoding: chunked\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        for chunk in chunks {
            socket
                .write_all(format!("{:x}\r\n{}\r\n", chunk.len(), chunk).as_bytes())
                .await
                .unwrap();
            socket.flush().await.unwrap();
        }
        socket.write_all(b"0\r\n\r\n").await.unwrap();
    }

    async fn write_response(socket: &mut tokio::net::TcpStream, content_type: &str, body: &str) {
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }
}

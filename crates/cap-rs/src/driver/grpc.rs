//! OpenClaude gRPC driver — connects to an `openclaude grpc` server.
//!
//! Wire format: gRPC bidirectional stream (openclaude.v1.AgentService.Chat).
//! The driver spawns a background task that manages the stream and translates
//! between [`ClientFrame`] / [`AgentEvent`] and the proto messages.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{error, warn};

use crate::core::{AgentEvent, ClientFrame, Content, StopReason, TextChannel, Usage};
use crate::driver::{Driver, DriverError, DriverExitStatus};

pub mod proto {
    tonic::include_proto!("openclaude.v1");
}

use proto::{
    CancelSignal, ChatRequest, ClientMessage, ServerMessage,
    agent_service_client::AgentServiceClient, client_message, server_message,
};

/// Driver for the OpenClaude gRPC server (`openclaude grpc`).
pub struct GrpcDriver {
    event_rx: mpsc::Receiver<AgentEvent>,
    frame_tx: mpsc::Sender<ClientFrame>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    alive: Arc<AtomicBool>,
    exit_status: Option<DriverExitStatus>,
}

impl std::fmt::Debug for GrpcDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcDriver").finish_non_exhaustive()
    }
}

impl Drop for GrpcDriver {
    fn drop(&mut self) {
        // Abort the background gRPC stream task to prevent task leaks.
        // If shutdown() was called explicitly, the handle is already taken.
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        self.alive.store(false, Ordering::Relaxed);
    }
}

impl GrpcDriver {
    /// Connect to the openclaude gRPC server at `host:port`.
    pub async fn connect(addr: impl AsRef<str>) -> Result<Self, DriverError> {
        let addr = addr.as_ref().to_string();
        let alive = Arc::new(AtomicBool::new(true));
        let alive_clone = alive.clone();

        let (event_tx, event_rx) = mpsc::channel::<AgentEvent>(256);
        let (frame_tx, frame_rx) = mpsc::channel::<ClientFrame>(64);

        let handle = tokio::spawn(async move {
            if let Err(e) = run_stream(&addr, event_tx, frame_rx).await {
                error!("gRPC stream error: {e}");
            }
            alive_clone.store(false, Ordering::Relaxed);
        });

        Ok(Self {
            event_rx,
            frame_tx,
            task_handle: Some(handle),
            alive,
            exit_status: None,
        })
    }
}

async fn run_stream(
    addr: &str,
    event_tx: mpsc::Sender<AgentEvent>,
    mut frame_rx: mpsc::Receiver<ClientFrame>,
) -> Result<(), DriverError> {
    // Wait for the first frame so we can send it in the initial gRPC request.
    let first_frame = frame_rx.recv().await.ok_or_else(|| {
        DriverError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "stream closed before first frame",
        ))
    })?;

    let mut client = AgentServiceClient::connect(format!("http://{addr}"))
        .await
        .map_err(|e| DriverError::AgentError {
            code: "CONNECT".into(),
            message: e.to_string(),
        })?;

    // Build the first message and a stream for subsequent ones.
    let first_msg = frame_to_client_message(first_frame);
    let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel::<ClientMessage>();
    request_tx.send(first_msg).map_err(|_| {
        DriverError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "gRPC request channel closed",
        ))
    })?;

    let request_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(request_rx);

    let response = client
        .chat(request_stream)
        .await
        .map_err(|e| DriverError::AgentError {
            code: "STREAM".into(),
            message: e.to_string(),
        })?;

    // Spawn writer for subsequent frames.
    let write_handle = tokio::spawn(async move {
        write_loop(frame_rx, request_tx).await;
    });

    let mut stream = response.into_inner();
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(msg) => match translate_server_message(msg) {
                Ok(Some(event)) => {
                    if event_tx.send(event).await.is_err() {
                        break;
                    }
                }
                Ok(None) => {}
                Err(e) => warn!("gRPC message translation failed: {e}"),
            },
            Err(e) => {
                error!("gRPC receive error: {e}");
                break;
            }
        }
    }

    write_handle.abort();
    Ok(())
}

async fn write_loop(
    mut frame_rx: mpsc::Receiver<ClientFrame>,
    request_tx: tokio::sync::mpsc::UnboundedSender<ClientMessage>,
) {
    while let Some(frame) = frame_rx.recv().await {
        let msg = frame_to_client_message(frame);
        if request_tx.send(msg).is_err() {
            break;
        }
    }
}

fn frame_to_client_message(frame: ClientFrame) -> ClientMessage {
    match frame {
        ClientFrame::SessionConfig(cfg) => ClientMessage {
            payload: Some(client_message::Payload::Request(ChatRequest {
                message: String::new(),
                working_directory: cfg.cwd.to_string_lossy().to_string(),
                session_id: cfg.session_resume_id.unwrap_or_default(),
                model: cfg.model,
            })),
        },
        ClientFrame::Prompt { content } => {
            let text = content
                .iter()
                .filter_map(|c| match c {
                    Content::Text { text } => Some(text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");

            ClientMessage {
                payload: Some(client_message::Payload::Request(ChatRequest {
                    message: text,
                    working_directory: String::new(),
                    session_id: String::new(),
                    model: None,
                })),
            }
        }
        ClientFrame::PermissionResponse { req_id, decision } => {
            use crate::core::PermissionDecision;
            let reply = match decision {
                PermissionDecision::AllowOnce | PermissionDecision::AllowAlways => "y",
                PermissionDecision::Deny => "n",
            };
            ClientMessage {
                payload: Some(client_message::Payload::Input(proto::UserInput {
                    prompt_id: req_id,
                    reply: reply.to_string(),
                })),
            }
        }
        ClientFrame::Cancel { .. } => ClientMessage {
            payload: Some(client_message::Payload::Cancel(CancelSignal {
                reason: String::new(),
            })),
        },
        _ => ClientMessage { payload: None },
    }
}

fn translate_server_message(msg: ServerMessage) -> Result<Option<AgentEvent>, DriverError> {
    use server_message::Event;

    match msg.event {
        Some(Event::TextChunk(chunk)) => Ok(Some(AgentEvent::TextChunk {
            msg_id: String::new(),
            text: chunk.text,
            channel: TextChannel::Assistant,
        })),
        Some(Event::ToolStart(tool)) => Ok(Some(AgentEvent::ToolCallStart {
            call_id: tool.tool_use_id,
            name: tool.tool_name,
            input: serde_json::from_str(&tool.arguments_json).unwrap_or_default(),
        })),
        Some(Event::ToolResult(result)) => Ok(Some(AgentEvent::ToolCallEnd {
            call_id: result.tool_use_id,
            output: result.output,
            is_error: result.is_error,
        })),
        Some(Event::ActionRequired(action)) => Ok(Some(AgentEvent::PermissionRequest {
            req_id: action.prompt_id,
            tool: String::new(),
            intent: serde_json::json!({ "question": action.question }),
            scope: crate::core::PermissionScope::Execute,
            risk_level: crate::core::RiskLevel::Medium,
        })),
        Some(Event::Done(done)) => Ok(Some(AgentEvent::Done {
            stop_reason: StopReason::EndTurn,
            usage: Usage {
                input_tokens: done.prompt_tokens as u64,
                output_tokens: done.completion_tokens as u64,
                ..Default::default()
            },
        })),
        Some(Event::Error(err)) => Err(DriverError::AgentError {
            code: err.code,
            message: err.message,
        }),
        None => Ok(None),
    }
}

#[async_trait]
impl Driver for GrpcDriver {
    async fn send(&mut self, frame: ClientFrame) -> Result<(), DriverError> {
        self.frame_tx.send(frame).await.map_err(|_| {
            DriverError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "gRPC stream closed",
            ))
        })
    }

    async fn next_event(&mut self) -> Option<AgentEvent> {
        self.event_rx.recv().await
    }

    async fn shutdown(&mut self) -> Result<(), DriverError> {
        self.alive.store(false, Ordering::Relaxed);
        self.exit_status = Some(DriverExitStatus::Killed);
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
        Ok(())
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    fn exit_status(&self) -> Option<DriverExitStatus> {
        self.exit_status.clone()
    }

    fn prompt_after_ready(&self) -> bool {
        false
    }
}

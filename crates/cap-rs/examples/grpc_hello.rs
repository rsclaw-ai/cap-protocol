use std::time::Duration;

use cap_rs::core::{ClientFrame, Content};
use cap_rs::driver::Driver;
use cap_rs::driver::grpc::GrpcDriver;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("GRPC_ADDR").unwrap_or_else(|_| "localhost:50051".into());

    let mut driver = GrpcDriver::connect(&addr).await?;

    driver
        .send(ClientFrame::Prompt {
            content: vec![Content::text(
                "Say \"hello from Rust gRPC\" and nothing else.",
            )],
        })
        .await?;

    // Read events with timeout
    loop {
        tokio::select! {
            event = driver.next_event() => {
                match event {
                    Some(event) => {
                        println!("EVENT: {event:?}");
                        if matches!(event, cap_rs::core::AgentEvent::Done { .. }) {
                            break;
                        }
                    }
                    None => break,
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(30)) => {
                eprintln!("TIMEOUT");
                break;
            }
        }
    }

    driver.shutdown().await?;
    Ok(())
}

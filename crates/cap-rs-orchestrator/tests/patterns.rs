use cap_rs::core::StopReason;
use cap_rs_orchestrator::config::FleetSpec;
use cap_rs_orchestrator::event::OrchestratorEvent;
use cap_rs_orchestrator::executor::Executor;
use cap_rs_orchestrator::testing::{StubDriver, StubDriverFactory};
use cap_rs_orchestrator::worktree::NoopWorktreeManager;

/// Drain the engine to completion, returning ordered event-tag strings + audit pairs.
async fn run_to_completion(
    spec: FleetSpec,
    factory: StubDriverFactory,
) -> (Vec<String>, Vec<(String, String)>) {
    let wt = NoopWorktreeManager::new();
    let (mut handle, mut events) = Executor::start(spec, factory, wt, "the task")
        .await
        .expect("executor start");

    let mut tags = Vec::new();
    while let Some(ev) = events.recv().await {
        match &ev {
            OrchestratorEvent::SessionStarted { session } => tags.push(format!("start:{session}")),
            OrchestratorEvent::SessionDone { session, .. } => tags.push(format!("done:{session}")),
            OrchestratorEvent::Routed { from, to } => tags.push(format!("route:{from}->{to}")),
            OrchestratorEvent::AwaitSelection { candidates } => {
                tags.push(format!("select:{}", candidates.join(",")));
            }
            OrchestratorEvent::FleetComplete => {
                tags.push("complete".into());
                break;
            }
            _ => {}
        }
    }
    let audit = handle.audit_pairs().await;
    (tags, audit)
}

#[tokio::test]
async fn pipeline_a_then_b() {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: main
  sessions:
    coder: { driver: claude, permissions: allow }
    reviewer: { driver: codex, permissions: allow }
  start: coder
  routes:
    - { when: coder.done, route_to: reviewer }
"#,
    )
    .unwrap();
    let factory = StubDriverFactory::new()
        .with("coder", StubDriver::new("coder").text("wrote code").done(StopReason::EndTurn))
        .with("reviewer", StubDriver::new("reviewer").text("looks ok").done(StopReason::EndTurn));

    let (tags, audit) = run_to_completion(spec, factory).await;

    assert_eq!(tags.iter().filter(|t| t.starts_with("done:")).count(), 2);
    let route_pos = tags.iter().position(|t| t == "route:coder->reviewer").unwrap();
    let coder_done = tags.iter().position(|t| t == "done:coder").unwrap();
    let reviewer_done = tags.iter().position(|t| t == "done:reviewer").unwrap();
    assert!(coder_done < route_pos, "route must follow coder done");
    assert!(route_pos < reviewer_done, "reviewer done must follow the route");
    assert!(tags.last().unwrap() == "complete");
    assert_eq!(audit, vec![("coder".to_string(), "reviewer".to_string())]);
}

#[tokio::test]
async fn lead_worker_fan_out_then_join() {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: main
  sessions:
    lead: { driver: claude, permissions: allow }
    a: { driver: codex, permissions: allow }
    b: { driver: codex, permissions: allow }
    rev: { driver: claude, permissions: allow }
  start: lead
  routes:
    - when: lead.done
      fan_out: { to: [a, b], split: broadcast }
    - when: [a.done, b.done]
      route_to: rev
"#,
    )
    .unwrap();
    let factory = StubDriverFactory::new()
        .with("lead", StubDriver::new("lead").text("plan").done(StopReason::EndTurn))
        .with("a", StubDriver::new("a").text("a-work").done(StopReason::EndTurn))
        .with("b", StubDriver::new("b").text("b-work").done(StopReason::EndTurn))
        .with("rev", StubDriver::new("rev").text("merged").done(StopReason::EndTurn));

    let (tags, audit) = run_to_completion(spec, factory).await;

    let rev_start = tags.iter().position(|t| t == "start:rev").unwrap();
    let a_done = tags.iter().position(|t| t == "done:a").unwrap();
    let b_done = tags.iter().position(|t| t == "done:b").unwrap();
    assert!(a_done < rev_start && b_done < rev_start, "join must wait for both");
    assert!(audit.contains(&("lead".into(), "a".into())));
    assert!(audit.contains(&("lead".into(), "b".into())));
    assert!(audit.contains(&("a".into(), "rev".into())) || audit.contains(&("b".into(), "rev".into())));
    assert_eq!(tags.last().unwrap(), "complete");
}

#[tokio::test]
async fn parallel_race_collects_for_human() {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: main
  sessions:
    x: { driver: claude, permissions: allow }
    y: { driver: codex, permissions: allow }
  start: [x, y]
  routes:
    - when: [x.done, y.done]
      collect: human
"#,
    )
    .unwrap();
    let factory = StubDriverFactory::new()
        .with("x", StubDriver::new("x").text("sol-x").done(StopReason::EndTurn))
        .with("y", StubDriver::new("y").text("sol-y").done(StopReason::EndTurn));

    let (tags, _audit) = run_to_completion(spec, factory).await;
    assert!(tags.iter().any(|t| t == "select:x,y"), "tags: {tags:?}");
    assert_eq!(tags.last().unwrap(), "complete");
}

#[tokio::test]
async fn lead_worker_by_subtask_split() {
    let spec = FleetSpec::from_yaml(
        r#"
fleet:
  base_branch: main
  sessions:
    lead: { driver: claude, permissions: allow }
    a: { driver: codex, permissions: allow }
    b: { driver: codex, permissions: allow }
  start: lead
  routes:
    - when: lead.done
      fan_out: { to: [a, b], split: by_subtask }
"#,
    )
    .unwrap();

    let fence = "`".repeat(3);
    let lead_out =
        format!("Here is the plan.\n{fence}cap-subtasks\n[\"task for A\", \"task for B\"]\n{fence}\n");
    let factory = StubDriverFactory::new()
        .with("lead", StubDriver::new("lead").text(&lead_out).done(StopReason::EndTurn))
        .with("a", StubDriver::new("a").text("did A").done(StopReason::EndTurn))
        .with("b", StubDriver::new("b").text("did B").done(StopReason::EndTurn));

    let (tags, audit) = run_to_completion(spec, factory).await;

    assert!(audit.contains(&("lead".into(), "a".into())), "audit: {audit:?}");
    assert!(audit.contains(&("lead".into(), "b".into())), "audit: {audit:?}");
    assert!(tags.iter().any(|t| t == "done:a"));
    assert!(tags.iter().any(|t| t == "done:b"));
    assert_eq!(tags.last().unwrap(), "complete");
}

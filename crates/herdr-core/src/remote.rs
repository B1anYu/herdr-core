//! Lines to other machines: the core keeps one framed connection open to each
//! machine it added, and calls naming `machine/…` go over it. The other way,
//! a core that dialed in (a peer) may have parents of terminals here: they
//! hear from them over the same line, or later from an outbox kept here.

use crate::machines::Machine;
use crate::rpc::RpcError;
use crate::runtime::{Core, State, now_ms};
use crate::wire::{self, Conn};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};

/// A viewer here that a stream there goes to: its connection and channel.
type Sink = (Arc<Conn>, u32);
/// A pending reply, its stream sink and whether resize support is required.
type Waiting = (oneshot::Sender<Value>, Option<(Sink, bool)>);

mod viewers;
#[cfg(test)]
use viewers::automatic_input;
use viewers::{RESIZE_LOST, negotiate_resize, reattach};
pub use viewers::{Relayed, attach, conn_gone, relay_in};

#[cfg(test)]
mod tests;

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

/// One open line: calls go out on channel 0, replies come back by id;
/// attach streams there are relayed to viewers here.
pub struct Line {
    frames: mpsc::UnboundedSender<Vec<u8>>,
    pending: Mutex<HashMap<u64, Waiting>>,
    /// Stream channel there -> the viewer here.
    streams: Mutex<HashMap<u32, Sink>>,
    next: AtomicU64,
    /// The events subscription that keeps this machine's mirror: its call id,
    /// then its channel, and a line of events not yet whole.
    mirror: Mutex<(u64, Option<u32>, Vec<u8>)>,
    /// What the core there said it can do (its `hello`).
    features: Vec<String>,
    closed: AtomicBool,
}

impl Line {
    fn new(frames: mpsc::UnboundedSender<Vec<u8>>, hello: &Value) -> Arc<Line> {
        let features = hello["features"].as_array().into_iter().flatten();
        Arc::new(Line {
            features: features.filter_map(|f| f.as_str().map(String::from)).collect(),
            frames,
            closed: AtomicBool::new(false),
            pending: Mutex::default(),
            streams: Mutex::default(),
            next: AtomicU64::new(2),
            mirror: Mutex::default(),
        })
    }

    fn close(&self) {
        let mut pending = lock(&self.pending);
        self.closed.store(true, Ordering::Relaxed);
        pending.clear();
    }

    /// A reply to one of our calls: to whoever waits for it.
    fn take_reply(&self, mut reply: Value) {
        let Some((waiter, sink)) = reply["id"].as_u64().and_then(|id| lock(&self.pending).remove(&id)) else { return };
        if let (Some((sink, require_resize)), Some(there)) = (sink, reply["result"]["channel"].as_u64()) {
            if require_resize && reply["result"]["resize_notifications"] != true {
                let _ = self.frames.send(wire::encode(there as u32, wire::CLOSE, b""));
                reply = crate::rpc::response(
                    reply["id"].clone(),
                    Err(RpcError::new("unavailable", RESIZE_LOST, "attach again")),
                );
            } else {
                lock(&self.streams).insert(there as u32, sink);
            }
        }
        let _ = waiter.send(reply);
    }

    fn message(&self, message: &Value) {
        let _ = self.frames.send(wire::encode(0, wire::DATA, message.to_string().as_bytes()));
    }

    /// A call bounded by its deadline and the line's health.
    pub async fn call(
        &self,
        machine: &str,
        method: &str,
        params: Value,
        deadline: Duration,
    ) -> Result<Value, RpcError> {
        self.call_into(machine, method, params, None, deadline).await
    }

    /// A call whose reply may open a stream (`channel`): it goes to `sink`
    /// from its first byte.
    async fn call_into(
        &self,
        machine: &str,
        method: &str,
        params: Value,
        sink: Option<(Sink, bool)>,
        deadline: Duration,
    ) -> Result<Value, RpcError> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string();
        // Too big for one frame would end the whole line: refuse just this call.
        if req.len() > wire::MAX_PAYLOAD {
            return Err(RpcError::invalid_params(format!(
                "too big to send to {machine} ({} MB at most)",
                wire::MAX_PAYLOAD >> 20
            )));
        }
        let lost = || {
            RpcError::new(
                "unavailable",
                format!("lost the line to {machine} during the call"),
                "it may have happened there; check, then retry",
            )
        };
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = lock(&self.pending);
            if self.closed.load(Ordering::Relaxed) {
                return Err(lost());
            }
            pending.insert(id, (tx, sink));
        }
        let _asked = Asked { line: self, id };
        self.frames.send(wire::encode(0, wire::DATA, req.as_bytes())).map_err(|_| lost())?;
        let reply = tokio::select! {
            reply = tokio::time::timeout(deadline, rx) => reply.map_err(|_| {
                RpcError::new(
                    "unavailable",
                    format!("{machine} didn't answer within {} s", deadline.as_secs()),
                    if method == "terminal.send" {
                        "it may still have been typed; check with hc read before retrying"
                    } else {
                        "it may be busy or stuck; hc machine ls shows its line"
                    },
                )
            })?,
            err = crate::server::send_cancelled(), if method == "terminal.send" => return Err(err),
        };
        let reply = reply.map_err(|_| lost())?;
        match reply.get("error") {
            Some(e) => Err(RpcError::from_json(e)),
            None => Ok(reply["result"].clone()),
        }
    }
}

/// A call on its way: given up on (its deadline passed, or its caller went
/// away) before the reply came, the other side hears so and can stop it. A
/// call no longer pending was answered, or its line is gone.
struct Asked<'a> {
    line: &'a Line,
    id: u64,
}

impl Drop for Asked<'_> {
    fn drop(&mut self) {
        if lock(&self.line.pending).remove(&self.id).is_some() {
            self.line.message(&json!({"jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": self.id}}));
        }
    }
}

pub enum LineState {
    Connecting,
    Connected(Arc<Line>),
    NeedsLogin {
        since_ms: u64,
    },
    /// Its hc speaks another protocol generation.
    NeedsUpdate {
        since_ms: u64,
    },
    Unreachable {
        since_ms: u64,
        why: String,
    },
}

/// A machine as `machine ls` and `ls` show it: its profile and its line now.
#[derive(Serialize, Deserialize, JsonSchema, Clone)]
pub struct MachineView {
    #[serde(flatten)]
    pub machine: Machine,
    /// connecting, connected, needs_login or unreachable.
    pub state: String,
    /// Since when it is not connected (ms since the epoch).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    /// `personal:` The machine it was reached through, when it was not added
    /// here (`sg` through `windows`). None: added here, or this core.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<String>,
}

pub fn view(st: &State, m: &Machine) -> MachineView {
    let (state, since_ms, why) = match st.lines.get(&m.id) {
        None | Some(LineState::Connecting) => ("connecting", None, None),
        Some(LineState::Connected(_)) => ("connected", None, None),
        Some(LineState::NeedsLogin { since_ms }) => ("needs_login", Some(*since_ms), None),
        Some(LineState::NeedsUpdate { since_ms }) => ("needs_update", Some(*since_ms), None),
        Some(LineState::Unreachable { since_ms, why }) => ("unreachable", Some(*since_ms), Some(why.clone())),
    };
    MachineView { machine: m.clone(), state: state.into(), since_ms, why, through: None }
}

fn reconnect_max() -> Duration {
    Duration::from_millis(std::env::var("HC_RECONNECT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(30_000))
}

/// Keeps the line to every added machine up.
pub fn start_all(core: &Arc<Core>) {
    let ids: Vec<String> = core.lock().machines.iter().map(|m| m.id.clone()).collect();
    for id in ids {
        keep_up(core, id);
    }
}

/// Dials the machine, holds the line, and dials again after a drop (backing
/// off to 30 s, or more slowly when a login is needed) until it is removed.
pub fn keep_up(core: &Arc<Core>, id: String) {
    let mut st = core.lock();
    st.lines.insert(id.clone(), LineState::Connecting);
    let task = tokio::spawn(supervise(core.clone(), id.clone()));
    if let Some(old) = st.line_tasks.insert(id, task.abort_handle()) {
        old.abort();
    }
}

pub fn forget(core: &Core, st: &mut State, id: &str) {
    for (address, record) in st.mirrors.remove(id).unwrap_or_default() {
        core.deliver(st, "removed", &address, json!({}), &record, Some(&record));
    }
    if let Some(LineState::Connected(line)) = st.lines.remove(id) {
        line.close();
    }
    if let Some(task) = st.line_tasks.remove(id) {
        task.abort();
    }
    st.mesh.retain(|e| e.next != NextHop::Machine(id.to_owned()));
    // Viewers waiting for it won't get it back.
    let waiting: Vec<(usize, u32)> = st.relayed.iter().filter(|(_, r)| r.machine == id).map(|(k, _)| *k).collect();
    for key in waiting {
        if let Some(r) = st.relayed.remove(&key) {
            r.conn.with(|out| out.finish(key.1, b""));
        }
    }
}

async fn supervise(core: Arc<Core>, id: String) {
    let mut backoff = Duration::from_millis(250);
    let mut login_backoff = Duration::ZERO;
    loop {
        let Some(m) = core.lock().machines.iter().find(|m| m.id == id).cloned() else { return };
        let down = |st: &mut State, why: String| {
            let since_ms = match st.lines.get(&id) {
                Some(
                    LineState::Unreachable { since_ms, .. }
                    | LineState::NeedsLogin { since_ms }
                    | LineState::NeedsUpdate { since_ms },
                ) => *since_ms,
                _ => now_ms(),
            };
            let state = match why.as_str() {
                "needs_login" => LineState::NeedsLogin { since_ms },
                "needs_update" => LineState::NeedsUpdate { since_ms },
                _ => LineState::Unreachable { since_ms, why },
            };
            st.lines.insert(id.clone(), state);
            core.changed.notify_waiters();
        };
        let hello = json!({"framed": true, "core": core.info, "calls_you": m.name, "features": crate::api::FEATURES});
        match crate::machines::dial(&core.paths, m.reach(), m.session.as_deref(), hello).await {
            Ok(d) if d.hello["core"]["id"] != m.id.as_str() => {
                down(&mut core.lock(), format!("a different core answers there ({})", d.hello["core"]["id"]));
            }
            Ok(d) => {
                backoff = Duration::from_millis(250);
                login_backoff = Duration::ZERO;
                // It may have been updated since it was added.
                if let Some(v) = d.hello["version"].as_str().filter(|v| *v != m.version) {
                    let mut st = core.lock();
                    if let Some(saved) = st.machines.iter_mut().find(|x| x.id == id) {
                        saved.version = v.into();
                    }
                    crate::machines::save(&core.paths.state, &st.machines);
                }
                hold(&core, &m, d).await;
                // Dial again at once: most drops are a restart there. Calls meanwhile
                // wait for that dial instead of failing; only a failed one is "down".
                core.lock().lines.insert(id.clone(), LineState::Connecting);
                core.changed.notify_waiters();
                continue;
            }
            Err(e) if e.kind == "needs_login" => {
                down(&mut core.lock(), "needs_login".into());
                // Slow enough that a failing key doesn't get the person banned (fail2ban, CrowdSec),
                // but an ssh-agent unlocked later still reconnects on its own.
                login_backoff = Duration::from_secs(match login_backoff.as_secs() {
                    0 => 120,
                    120 => 300,
                    _ => 600,
                });
                tokio::time::sleep(login_backoff).await;
                continue;
            }
            Err(e) if e.kind == "incompatible" => down(&mut core.lock(), "needs_update".into()),
            Err(e) => down(&mut core.lock(), e.message),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(reconnect_max());
    }
}

async fn refresh_status(core: &Arc<Core>, m: &Machine, line: &Arc<Line>) {
    loop {
        if let Ok(status) = line.call(&m.name, "server.status", json!({}), Duration::from_secs(2)).await {
            let mut st = core.lock();
            let Some(saved) = st.machines.iter_mut().find(|saved| saved.id == m.id) else { return };
            saved.keeps_running_after_logout = status["keeps_running_after_logout"].as_bool();
            saved.logout_linger_user = status["logout_linger_user"].as_str().map(String::from);
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Serves one open line until it closes.
async fn hold(core: &Arc<Core>, m: &Machine, mut d: crate::machines::Dialed) {
    let (frames, mut out) = mpsc::unbounded_channel::<Vec<u8>>();
    let line = Line::new(frames, &d.hello);
    {
        let mut st = core.lock();
        st.lines.insert(m.id.clone(), LineState::Connected(line.clone()));
        for message in st.machine_outbox.remove(&m.id).unwrap_or_default() {
            line.message(&message);
        }
    }
    core.changed.notify_waiters();
    tokio::spawn(reattach(core.clone(), m.clone(), line.clone()));
    let refresh = refresh_status(core, m, &line);
    tokio::pin!(refresh);
    // Everything that happens there, to keep its mirror here and tell subscribers.
    let id = line.next.fetch_add(1, Ordering::Relaxed);
    lock(&line.mirror).0 = id;
    line.message(&json!({"jsonrpc": "2.0", "id": id, "method": "events.subscribe", "params": {}}));
    let mut stdin = d.stdin;
    let writer = tokio::spawn(async move {
        while let Some(frame) = out.recv().await {
            if stdin.write_all(&frame).await.is_err() {
                return;
            }
        }
    });
    // ssh's own messages must not fill its pipe.
    let mut stderr = d.stderr;
    let drain = tokio::spawn(async move { tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await });
    // It sends heartbeats: a machine silent this long is stuck, not quiet.
    let silence = crate::config::load(&core.paths.config).unwrap_or_default().machine_timeout()
        + crate::server::heartbeat_every();
    // Silence is no bytes at all: a big frame on a slow link keeps flowing.
    let heard = wire::Heard::default();
    let mut stdout = heard.watch(&mut d.stdout);
    let mut sends = crate::server::Sends::default();
    loop {
        let (channel, kind, payload) = tokio::select! {
            () = &mut refresh => break,
            frame = wire::read_frame(&mut stdout) => match frame {
                Ok(Some(frame)) => frame,
                _ => break,
            },
            () = heard.silent_for(silence) => {
                eprintln!("core: {} sent nothing for {} s: line given up", m.name, silence.as_secs());
                break;
            }
        };
        match (channel, kind) {
            (0, wire::DATA) => {
                let Ok(message) = serde_json::from_slice::<Value>(&payload) else { continue };
                let mirror_call = lock(&line.mirror).0;
                match message["method"].as_str() {
                    None if message["id"] == mirror_call => {
                        lock(&line.mirror).1 = message["result"]["channel"].as_u64().map(|c| c as u32);
                        mirror_replace(core, m, message["result"]["terminals"].as_array().cloned().unwrap_or_default());
                        if message["result"]["channel"].is_u64() {
                            // Docs are not in terminal snapshots: readers must refetch after a gap.
                            core.documents_missed(&mut core.lock());
                        }
                    }
                    None => line.take_reply(message),
                    Some("$/cancelRequest") => sends.cancel(Some(&message["params"]["id"].to_string())),
                    Some(_) => {
                        let cancelled = sends.track(&message);
                        drop(tokio::spawn(
                            crate::server::SEND_CANCELLED
                                .scope(cancelled, from_machine(core.clone(), m.clone(), line.clone(), message)),
                        ));
                    }
                }
            }
            (0, _) => {}
            (there, wire::DATA) if lock(&line.mirror).1 == Some(there) => {
                mirror_events(core, m, &line, &payload);
                let _ = line.frames.send(wire::encode(there, wire::CREDIT, &(payload.len() as u32).to_le_bytes()));
            }
            (there, wire::DATA) => {
                if let Some((conn, here)) = lock(&line.streams).get(&there) {
                    conn.with(|out| {
                        out.data(*here, &payload);
                    });
                }
            }
            (there, wire::RESIZE) if payload.len() == 4 => {
                if let Some((conn, here)) = lock(&line.streams).get(&there) {
                    conn.with(|out| out.ordered_resize(*here, &payload));
                }
            }
            (there, wire::CLOSE) => {
                if let Some((conn, here)) = lock(&line.streams).remove(&there) {
                    core.lock().relayed.remove(&(conn_key(&conn), here));
                    conn.with(|out| out.finish(here, &payload));
                }
            }
            _ => {}
        }
    }
    writer.abort();
    drain.abort();
    // No longer the line to use, before anyone waiting hears it is lost (a
    // retry must not pick this dead line up again).
    {
        let mut st = core.lock();
        if matches!(st.lines.get(&m.id), Some(LineState::Connected(l)) if Arc::ptr_eq(l, &line)) {
            st.lines.insert(m.id.clone(), LineState::Connecting);
        }
    }
    // Calls still waiting on it fail now ("lost the line"), not at their deadline.
    line.close();
    // Viewers here hear that the line is gone and stay open; the programs keep
    // running there, and the viewers get them back with the line.
    let notice = lost_notice(&m.name);
    for (_, (conn, here)) in lock(&line.streams).drain() {
        conn.with(|out| {
            out.data(here, notice.as_bytes());
        });
    }
    // Killed, not waited for: a Mac core once never heard the old ssh had
    // died and stayed "connecting" for good. It is reaped in the background.
    let _ = d.child.start_kill();
}

/// CAN first: the line may have dropped mid-sequence, which would swallow this.
fn lost_notice(machine: &str) -> String {
    format!("\x18\r\n[line to {machine} lost; reconnecting...]\r\n")
}

/// A machine's terminals now (after a connect, or after it says we missed
/// some): subscribers here hear what appeared, changed or went.
fn mirror_replace(core: &Core, m: &Machine, terminals: Vec<Value>) {
    let mut st = core.lock();
    let Some(name) = st.machines.iter().find(|current| current.id == m.id).map(|m| m.name.clone()) else { return };
    let fresh: std::collections::BTreeMap<String, Value> = terminals
        .into_iter()
        .map(|t| localize_in(core, &st, &name, &[], t))
        .map(|t| (t["address"].as_str().unwrap_or_default().to_string(), t))
        .collect();
    let old = st.mirrors.remove(&m.id).unwrap_or_default();
    for (address, record) in old.iter().filter(|(a, _)| !fresh.contains_key(*a)) {
        core.deliver(&mut st, "removed", address, json!({}), record, Some(record));
    }
    for (address, record) in fresh.iter().filter(|(a, r)| old.get(*a) != Some(*r)) {
        core.deliver(&mut st, "created", address, record.clone(), record, None);
    }
    st.mirrors.insert(m.id.clone(), fresh);
}

pub fn rename_mirror(core: &Core, st: &mut State, id: &str, old: &str, new: &str) {
    let Some(records) = st.mirrors.remove(id) else { return };
    let mut renamed = std::collections::BTreeMap::new();
    for record in records.values() {
        let mut record = record.clone();
        map_addresses(&mut record, &|s| match s.split_once('/') {
            Some((prefix, rest)) if prefix == old => format!("{new}/{rest}"),
            _ => s.to_owned(),
        });
        let address = record["address"].as_str().unwrap_or_default().to_owned();
        renamed.insert(address, record);
    }
    st.mirrors.insert(id.to_owned(), renamed.clone());
    for (address, record) in records {
        core.deliver(st, "removed", &address, json!({}), &record, Some(&record));
    }
    for (address, record) in renamed {
        core.deliver(st, "created", &address, record.clone(), &record, None);
    }
}

/// Event lines from a machine: applied to its mirror, passed on as seen from here.
fn mirror_events(core: &Core, m: &Machine, line: &Line, payload: &[u8]) {
    let lines: Vec<Vec<u8>> = {
        let mut mirror = lock(&line.mirror);
        mirror.2.extend_from_slice(payload);
        let whole = mirror.2.iter().rposition(|b| *b == b'\n').map_or(0, |at| at + 1);
        let done: Vec<u8> = mirror.2.drain(..whole).collect();
        done.split(|b| *b == b'\n').filter(|l| !l.is_empty()).map(<[u8]>::to_vec).collect()
    };
    for l in lines {
        let Ok(msg) = serde_json::from_slice::<Value>(&l) else { continue };
        match msg["method"].as_str() {
            Some("event") => mirror_event(core, m, msg["params"].clone()),
            Some("missed") => {
                mirror_replace(core, m, msg["params"]["terminals"].as_array().cloned().unwrap_or_default());
                core.documents_missed(&mut core.lock());
            }
            _ => {}
        }
    }
}

fn mirror_event(core: &Core, m: &Machine, p: Value) {
    let mut st = core.lock();
    let Some(name) = st.machines.iter().find(|current| current.id == m.id).map(|m| m.name.clone()) else { return };
    let p = localize_in(core, &st, &name, &[], p);
    let (kind, address, data) =
        (p["type"].as_str().unwrap_or_default(), p["terminal"].as_str().unwrap_or_default(), p["data"].clone());
    if kind == "doc" {
        // Only this machine, not docs it in turn watches on a third core.
        if p.get("machine").is_none() {
            core.doc_event(&mut st, data, Some(&name));
        }
        return;
    }
    let mirror = st.mirrors.entry(m.id.clone()).or_default();
    let (record, was) = match kind {
        "created" | "restarted" => {
            mirror.insert(address.into(), data.clone());
            (data.clone(), None)
        }
        "removed" => match mirror.remove(address) {
            Some(record) => (record.clone(), Some(record)),
            None => return,
        },
        _ => {
            let Some(record) = mirror.get_mut(address) else { return };
            let was = record.clone();
            apply(record, kind, &data);
            (record.clone(), Some(was))
        }
    };
    core.deliver(&mut st, kind, address, data, &record, was.as_ref());
}

/// An event's change, applied to a terminal record (see schema/README.md).
fn apply(record: &mut Value, kind: &str, data: &Value) {
    let set = |record: &mut Value, keys: &[&str]| keys.iter().for_each(|k| record[*k] = data[*k].clone());
    match kind {
        "finished" | "lost" => {
            record["state"] = if kind == "lost" { "live" } else { "finished" }.into();
            if kind == "lost" {
                record["pid"] = Value::Null;
            }
            set(record, &["reason", "exit_code", "signal", "finished_ms"]);
            record.as_object_mut().map(|r| r.remove("foreground"));
        }
        "status" => set(record, &["status", "records", "reported"]),
        "turn" => set(record, &["turn"]),
        "title" => set(record, &["title"]),
        "labels" => set(record, &["labels"]),
        "keep" => set(record, &["keep"]),
        "size" => set(record, &["cols", "rows"]),
        "foreground" => record["foreground"] = data.clone(),
        _ => {}
    }
}

/// What a machine may ask of terminals here that are parents of its own.
const CALLS_BACK: &[&str] = &["terminal.send", "terminal.get", "terminal.read", "terminal.wait"];

/// `personal:` Whether a route points back at the machine that made the call.
fn goes_back(to: &To, m: &Machine) -> bool {
    match to {
        To::Machine(other) => other.id == m.id,
        To::Peer { id, .. } => *id == m.id,
        To::Mesh(via) => goes_back(via, m),
    }
}

/// A call or a note from a machine we dialed. It reaches only terminals here
/// that started work there: adding a machine gives it no say over the rest.
async fn from_machine(core: Arc<Core>, m: Machine, line: Arc<Line>, message: Value) {
    let method = message["method"].as_str().unwrap_or_default();
    let mut params = message["params"].clone();
    // `personal:` A machine this core trusts may use it as a way to the rest
    // of the mesh, not only to terminals that started work there.
    let cfg = crate::config::load(&core.paths.config).unwrap_or_default();
    let trusted = cfg.mesh_depth() > 0 && cfg.mesh_trusts(&[m.id.as_str(), m.name.as_str()]);
    // `personal:` Seeing the mesh is not acting in it: a machine added here
    // may always ask what is around, even when it may not touch anything.
    let lists = method == "terminal.list" || method == "machine.list";
    if method != "notes" && (trusted || lists) {
        let routed = crate::remote::route(&core, None, method, &params).await;
        match routed {
            Ok(Some((to, params))) => {
                let result = if goes_back(&to, &m) {
                    Err(RpcError::new("denied", "that address points back to its caller", ""))
                } else {
                    crate::remote::forward_to(&core, &to, method, params).await
                };
                if let Some(id) = message.get("id") {
                    line.message(&crate::rpc::response(id.clone(), result));
                }
                return;
            }
            // A trusted machine's call is a call like a peer's, not only a
            // callback into terminals that started work there.
            Ok(None) => {
                let mut params = params.clone();
                if let Some(caller) = params["caller"].as_str() {
                    let own = format!("{}/", m.id);
                    let own_name = format!("{}/", m.name);
                    let rest = caller.strip_prefix(&own).or_else(|| caller.strip_prefix(&own_name)).unwrap_or(caller);
                    params["caller"] = format!("{}/{rest}", m.name).into();
                }
                let call = crate::api::Call { core: &core, conn: None, plain: None, machine: Some(&m) };
                let result = crate::api::dispatch(&call, method, params).await;
                if let Some(id) = message.get("id") {
                    line.message(&crate::rpc::response(id.clone(), result));
                }
                return;
            }
            Err(e) => {
                if let Some(id) = message.get("id") {
                    line.message(&crate::rpc::response(id.clone(), Err(e)));
                }
                return;
            }
        }
    }
    let key = if method == "notes" { "parent" } else { "terminal" };
    let parent = params[key].as_str().and_then(|t| {
        let st = core.lock();
        let id = st.reg.resolve(crate::addr::local(t, &core.info.id, &core.info.name).ok()?).ok()?;
        st.reg.terminals.get(&id).filter(|t| trusted || t.afar.contains(&m.id)).map(|_| id)
    });
    if method == "notes" {
        if let (Some(id), Some(text)) = (parent, params["text"].as_str()) {
            core.lock().afar_notes.entry(id).or_default().push(text.into());
            let _ = core.jobs.send(crate::notes::Job::Notes(id));
        }
        return;
    }
    let result = match parent.filter(|_| trusted || CALLS_BACK.contains(&method)) {
        None => Err(RpcError::new(
            "denied",
            format!("{} may only reach the terminals here that started its own", m.name),
            "",
        )),
        Some(id) => {
            params["terminal"] = crate::terminals::address(id).into();
            if let Some(caller) = params["caller"].as_str() {
                params["caller"] = format!("{}/{caller}", m.name).into();
            }
            let call = crate::api::Call { core: &core, conn: None, plain: None, machine: None };
            crate::api::dispatch(&call, method, params).await
        }
    };
    if let Some(id) = message.get("id") {
        line.message(&crate::rpc::response(id.clone(), result));
    }
}

/// A core that dialed in, as this core remembers it (kept across restarts).
#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
pub struct Peer {
    /// What it calls itself: its terminals are `name/t3` here.
    pub name: String,
    /// What it calls this core: terminals here are `calls_me/t7` there.
    pub calls_me: String,
    /// What it must hear and couldn't yet (notes for its terminals), oldest first.
    #[serde(default)]
    pub outbox: Vec<Value>,
}

fn save_peers(core: &Core, st: &State) {
    let bytes = serde_json::to_vec(&st.peers).expect("peers serialize");
    if let Err(e) = crate::paths::write_atomic(&core.paths.state.join("peers.json"), &bytes) {
        eprintln!("core: cannot save peers: {e}");
    }
}

/// A core dialed in on `conn`: calls to it go out on the same connection,
/// and whatever waited in its outbox goes now.
pub fn peer_joined(core: &Arc<Core>, conn: &Arc<Conn>, hello: &Value) {
    let (Some(id), Some(name)) = (hello["core"]["id"].as_str(), hello["core"]["name"].as_str()) else { return };
    let (frames, mut out) = mpsc::unbounded_channel::<Vec<u8>>();
    let line = Line::new(frames, hello);
    let c = conn.clone();
    tokio::spawn(async move {
        while let Some(frame) = out.recv().await {
            c.with(|o| o.control(frame));
        }
    });
    let mut st = core.lock();
    // Its name must mean it alone here; else it goes by its id.
    let name = crate::addr::clean_name(name);
    let taken = name == core.info.name
        || st.machines.iter().any(|m| m.name == name)
        || st.peers.iter().any(|(other, p)| other != id && p.name == name);
    let peer = st.peers.entry(id.into()).or_default();
    peer.name = if taken { id.into() } else { name };
    peer.calls_me = hello["calls_you"].as_str().unwrap_or(&core.info.name).into();
    for message in std::mem::take(&mut peer.outbox) {
        line.message(&message);
    }
    if let Some((_, old)) = st.peer_lines.insert(id.into(), (conn_key(conn), line)) {
        old.close();
    }
    save_peers(core, &st);
}

/// A call from a peer: its callers (`<its id>/t3`) as this core calls them.
pub fn from_peer(core: &Core, conn: Option<&Arc<Conn>>, mut params: Value) -> Value {
    let Some(conn) = conn else { return params };
    let st = core.lock();
    let key = conn_key(conn);
    let Some((id, (_, line))) = st.peer_lines.iter().find(|(_, (k, _))| *k == key) else { return params };
    negotiate_resize(line, &mut params);
    if let (Some(rest), Some(peer)) =
        (params["caller"].as_str().and_then(|c| c.strip_prefix(&format!("{id}/"))), st.peers.get(id))
    {
        params["caller"] = format!("{}/{rest}", peer.name).into();
    }
    params
}

/// Whether `conn` is a core that dialed in.
pub fn is_peer(core: &Core, conn: &Arc<Conn>) -> bool {
    let key = conn_key(conn);
    core.lock().peer_lines.values().any(|(k, _)| *k == key)
}

pub fn peer_left(core: &Core, conn: &Arc<Conn>) {
    let key = conn_key(conn);
    core.lock().peer_lines.retain(|_, (k, line)| {
        if *k == key {
            line.close();
        }
        *k != key
    });
}

/// A reply from a peer to one of our calls. False: not a peer's connection.
pub fn reply_in(core: &Core, conn: &Arc<Conn>, reply: Value) -> bool {
    let key = conn_key(conn);
    let line = core.lock().peer_lines.values().find(|(k, _)| *k == key).map(|(_, l)| l.clone());
    line.map(|l| l.take_reply(reply)).is_some()
}

/// Notes for a parent on a peer: over its line now, else kept until it is back.
pub fn tell_peer(core: &Core, st: &mut State, parent: &str, notes: &[String]) {
    let Some((prefix, local)) = parent.split_once('/') else { return };
    let Some(id) = st.peers.iter().find(|(id, p)| *id == prefix || p.name == prefix).map(|(id, _)| id.clone()) else {
        return;
    };
    let message = json!({"jsonrpc": "2.0", "method": "notes", "params": {"parent": local, "text": notes.join("\n")}});
    match st.peer_lines.get(&id) {
        Some((_, line)) => line.message(&message),
        None => {
            st.peers.get_mut(&id).expect("found").outbox.push(message);
            save_peers(core, st);
        }
    }
}

/// A parent here finished: machines it started work on end its children that
/// asked to die with it (once the line is back, if it is down).
pub fn orphaned_afar(core: &Core, st: &mut State, parent: u32) {
    let afar = st.reg.terminals.get(&parent).map(|t| t.afar.clone()).unwrap_or_default();
    let message = json!({"jsonrpc": "2.0", "method": "$/orphaned", "params": {"parent": format!("{}/{}", core.info.id, crate::terminals::address(parent))}});
    for id in afar {
        match st.lines.get(&id) {
            Some(LineState::Connected(line)) => line.message(&message),
            _ => st.machine_outbox.entry(id).or_default().push(message.clone()),
        }
    }
}

/// From a peer: its terminal `parent` finished. Children here that asked to
/// die with it end.
pub async fn orphaned(core: &Arc<Core>, conn: Option<&Arc<Conn>>, params: &Value) -> Result<Value, RpcError> {
    let from_peer = conn.is_some_and(|c| core.lock().peer_lines.values().any(|(k, _)| *k == conn_key(c)));
    let Some(address) = params["parent"].as_str().filter(|_| from_peer).and_then(|p| peer_address(core, p)) else {
        return Err(RpcError::new("denied", "only a core that dialed in says its terminals finished", ""));
    };
    let kids: Vec<u32> = {
        let mut st = core.lock();
        let kids: Vec<u32> = st
            .reg
            .terminals
            .values()
            .filter(|t| t.is_live() && t.die_with_parent && t.parent.as_deref() == Some(address.as_str()))
            .map(|t| t.id())
            .collect();
        st.killing.extend(&kids);
        kids
    };
    core.kill(&kids).await;
    Ok(json!({}))
}

/// An address on a core that dialed in, as parents from there are kept here
/// (`<its name>/t3`); `None` for anything else.
pub fn peer_address(core: &Core, address: &str) -> Option<String> {
    let (prefix, rest) = address.split_once('/')?;
    let st = core.lock();
    if prefix == core.info.id
        || prefix == core.info.name
        || st.machines.iter().any(|m| m.name == prefix || m.id == prefix)
    {
        return None;
    }
    st.peers.iter().find(|(id, p)| *id == prefix || p.name == prefix).map(|(_, p)| format!("{}/{rest}", p.name))
}

/// How terminals here are called on the peer `parent` lives on (`rohan`).
pub fn called_there(st: &State, parent: &str) -> Option<String> {
    let prefix = parent.split_once('/')?.0;
    st.peers.iter().find(|(id, p)| *id == prefix || p.name == prefix).map(|(_, p)| p.calls_me.clone())
}

pub fn conn_key(conn: &Arc<Conn>) -> usize {
    Arc::as_ptr(conn) as usize
}

/// Methods whose `terminal` names the terminal they act on.
const ON_A_TERMINAL: &[&str] = &[
    "terminal.attach",
    "terminal.get",
    "terminal.kill",
    "terminal.rm",
    "terminal.label",
    "terminal.keep",
    "terminal.read",
    "terminal.send",
    "terminal.keys",
    "terminal.interrupt",
    "terminal.wait",
    "terminal.put",
];

/// Where a call goes when it isn't here.
pub enum To {
    /// A machine this core added.
    Machine(Machine),
    /// A core that dialed in: only to the parents of its terminals here.
    Peer { id: String, name: String },
    /// `personal:` Somewhere past this core. The address keeps its prefix:
    /// the next hop is the one that knows that machine by that name.
    Mesh(Box<To>),
}

impl To {
    /// The peer at the end of this route, if any.
    fn peer_id(&self) -> Option<&str> {
        match self {
            To::Peer { id, .. } => Some(id),
            To::Mesh(inner) => inner.peer_id(),
            To::Machine(_) => None,
        }
    }

    /// The machine at the end of this route, if any.
    fn machine_id(&self) -> Option<&str> {
        match self {
            To::Machine(m) => Some(&m.id),
            To::Mesh(inner) => inner.machine_id(),
            To::Peer { .. } => None,
        }
    }

    /// `personal:` The outbound machine line `terminal.attach` can open a
    /// stream on (`To::Peer` is an inbound line and carries no attach streams).
    pub fn attach_machine(&self) -> Option<Machine> {
        match self {
            To::Machine(m) => Some(m.clone()),
            To::Mesh(inner) => inner.attach_machine(),
            To::Peer { .. } => None,
        }
    }
}

/// `personal:` The neighbor a mesh entry is reached through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NextHop {
    /// A machine added here, by its core id.
    Machine(String),
    /// A core that dialed in, by its core id.
    Peer(String),
}

/// `personal:` A machine past this core that a mesh learned about: its core
/// id, the name the next hop calls it, and that hop.
#[derive(Clone, Debug)]
pub struct MeshEntry {
    pub id: String,
    pub name: String,
    pub next: NextHop,
}

/// `personal:` Record what a neighbor said about the machines past it. The
/// name stored is the one that neighbor uses, so a prefix made here means the
/// same machine there.
pub fn learn_mesh(core: &Core, st: &mut State, hop: NextHop, views: &[Value]) {
    st.mesh.retain(|e| e.next != hop);
    for v in views {
        let (Some(id), Some(name)) = (v["id"].as_str(), v["name"].as_str()) else { continue };
        if id == core.info.id {
            continue; // the way back here is not another machine
        }
        let entry = MeshEntry { id: id.to_owned(), name: name.to_owned(), next: hop.clone() };
        match st.mesh.iter_mut().find(|e| e.id == id) {
            Some(old) => *old = entry,
            None => st.mesh.push(entry),
        }
    }
}

/// `personal:` The cores that dialed in and are connected now, with the name
/// each goes by here.
pub fn connected_peers(st: &State) -> Vec<(String, String)> {
    st.peer_lines.keys().filter_map(|id| st.peers.get(id).map(|p| (id.clone(), p.name.clone()))).collect()
}

/// `personal:` The peer on `conn`, if it is one.
fn peer_on(core: &Core, conn: &Arc<Conn>) -> Option<(String, String)> {
    let st = core.lock();
    peer_on_in(&st, conn)
}

/// `personal:` As `peer_on`, for callers already holding the state lock.
fn peer_on_in(st: &State, conn: &Arc<Conn>) -> Option<(String, String)> {
    let key = conn_key(conn);
    let id = st.peer_lines.iter().find(|(_, (k, _))| *k == key).map(|(id, _)| id.clone())?;
    let name = st.peers.get(&id)?.name.clone();
    Some((id, name))
}

/// `personal:` The caller on `conn`, when it is a core that dialed in.
pub fn caller_peer(core: &Core, conn: Option<&Arc<Conn>>) -> Option<(String, String)> {
    conn.and_then(|c| peer_on(core, c))
}

/// `personal:` Whether a caller may reach past this core (`mesh_trust`).
/// Local callers always can; a core that dialed in only when trusted.
fn caller_trusted(core: &Core, st: &State, conn: Option<&Arc<Conn>>) -> bool {
    let Some(conn) = conn else { return true };
    // A framed connection that is not a core that dialed in is a local
    // client (a viewer, the CLI): it may use this core's machines as always.
    let Some((id, name)) = peer_on_in(st, conn) else { return true };
    crate::config::load(&core.paths.config).unwrap_or_default().mesh_trusts(&[id.as_str(), name.as_str()])
}

/// What an address prefix names: this core (`None`), a machine, a peer, or a
/// machine a mesh reaches through one of them.
fn named(core: &Core, st: &State, prefix: &str) -> Result<Option<To>, RpcError> {
    if prefix == core.info.id || prefix == core.info.name {
        return Ok(None);
    }
    if let Some(m) = st.machines.iter().find(|m| m.name == prefix || m.id == prefix) {
        return Ok(Some(To::Machine(m.clone())));
    }
    if let Some((id, p)) = st.peers.iter().find(|(id, p)| *id == prefix || p.name == prefix) {
        return Ok(Some(To::Peer { id: id.clone(), name: p.name.clone() }));
    }
    // `personal:` A machine past this core, reached through the neighbor that
    // first showed it to us, under that neighbor's name for it.
    let found: Vec<&MeshEntry> = st.mesh.iter().filter(|e| e.id == prefix || e.name == prefix).collect();
    match found.as_slice() {
        [] => Err(crate::machines::unknown(&st.machines, prefix)),
        [entry] => match &entry.next {
            NextHop::Machine(id) => match st.machines.iter().find(|m| &m.id == id) {
                Some(m) => Ok(Some(To::Mesh(Box::new(To::Machine(m.clone()))))),
                None => Err(crate::machines::unknown(&st.machines, prefix)),
            },
            NextHop::Peer(id) => match st.peers.get(id) {
                Some(p) => {
                    let peer = To::Peer { id: id.clone(), name: p.name.clone() };
                    Ok(Some(To::Mesh(Box::new(peer))))
                }
                None => Err(crate::machines::unknown(&st.machines, prefix)),
            },
        },
        _ => Err(RpcError::new(
            "machine_unknown",
            format!("{prefix:?} names more than one machine in the mesh"),
            "use the core id `hc machine ls` shows for it",
        )),
    }
}

/// `personal:` Ask the neighbors what machines they know, and remember it.
/// A mesh address nobody here knows may simply not have been shown to us yet.
async fn discover(core: &Arc<Core>) {
    let (machines, peers) = {
        let st = core.lock();
        let connected: Vec<Machine> = st
            .machines
            .iter()
            .filter(|m| matches!(st.lines.get(&m.id), Some(LineState::Connected(_))))
            .cloned()
            .collect();
        (connected, connected_peers(&st))
    };
    // One hop out: what they added, and what those machines added.
    let params = json!({"depth": 1, "seen": [core.info.id]});
    let mut asks = Vec::new();
    for m in machines {
        let (core, params) = (core.clone(), params.clone());
        asks.push(tokio::spawn(async move {
            let ask = forward(&core, &m, "machine.list", params);
            let r = tokio::time::timeout(Duration::from_secs(3), ask).await.ok().and_then(Result::ok);
            r.map(|r| (NextHop::Machine(m.id.clone()), r))
        }));
    }
    for (id, name) in peers {
        let (core, params) = (core.clone(), params.clone());
        let to = To::Peer { id: id.clone(), name };
        asks.push(tokio::spawn(async move {
            let ask = forward_to(&core, &to, "machine.list", params);
            let r = tokio::time::timeout(Duration::from_secs(3), ask).await.ok().and_then(Result::ok);
            r.map(|r| (NextHop::Peer(id.clone()), r))
        }));
    }
    for ask in asks {
        let Ok(Some((hop, r))) = ask.await else { continue };
        let views = r["machines"].as_array().cloned().unwrap_or_default();
        let mut st = core.lock();
        learn_mesh(core, &mut st, hop, &views);
    }
}

/// Where a call goes: here (`None`), or elsewhere with the params to send,
/// addresses as they are seen there.
pub async fn route(
    core: &Arc<Core>,
    conn: Option<&Arc<Conn>>,
    method: &str,
    params: &Value,
) -> Result<Option<(To, Value)>, RpcError> {
    match route_here(core, conn, method, params) {
        // `personal:` A mesh address nobody here knows may not have been
        // shown to us yet: ask the neighbors once, then look again.
        Err(e)
            if e.kind == "machine_unknown"
                && crate::config::load(&core.paths.config).unwrap_or_default().mesh_depth() > 0 =>
        {
            discover(core).await;
            route_here(core, conn, method, params)
        }
        other => other,
    }
}

fn route_here(
    core: &Core,
    conn: Option<&Arc<Conn>>,
    method: &str,
    params: &Value,
) -> Result<Option<(To, Value)>, RpcError> {
    let cfg = crate::config::load(&core.paths.config).unwrap_or_default();
    let mesh = cfg.mesh_depth();
    let st = core.lock();
    let mut params = params.clone();
    let to = if matches!(
        method,
        "terminal.run" | "machine.exec" | "file.read" | "file.write" | "file.edit" | "doc.get" | "doc.set"
    ) {
        let Some(on) = params["on"].as_str().map(String::from) else { return Ok(None) };
        let to = named(core, &st, &on)?;
        // `personal:` In a mesh a peer is a way to another machine; the stock
        // star still refuses to start work on a core that dialed in.
        if matches!(to, Some(To::Peer { .. })) && mesh == 0 {
            return Err(crate::machines::unknown(&st.machines, &on));
        }
        // A mesh hop resolves `on` itself: keep it for it.
        if !matches!(to, Some(To::Mesh(_))) {
            params.as_object_mut().map(|p| p.remove("on"));
        }
        to
    } else if ON_A_TERMINAL.contains(&method) && params["children"].is_null() {
        let Some((prefix, local)) =
            params["terminal"].as_str().and_then(|t| t.split_once('/')).map(|(a, b)| (a.to_string(), b.to_string()))
        else {
            return Ok(None);
        };
        let to = named(core, &st, &prefix)?;
        // A mesh hop knows that prefix; a direct machine or peer sees bare.
        if !matches!(to, Some(To::Mesh(_))) {
            params["terminal"] = local.into();
        }
        to
    } else {
        None
    };
    let Some(to) = to else { return Ok(None) };
    // The stock star lets a core that dialed in take only callbacks; a mesh
    // opens the rest, because such a peer is then a way to other machines.
    if mesh == 0
        && let To::Peer { name, .. } = &to
        && !CALLS_BACK.contains(&method)
    {
        return Err(RpcError::new("denied", format!("{name} only takes send, read, get and wait from here"), ""));
    }
    // `personal:` Reaching past this core is a mesh move: a local caller may,
    // a core that dialed in needs `mesh_trust` here.
    if !caller_trusted(core, &st, conn) {
        return Err(RpcError::new(
            "denied",
            "this core is not trusted to reach other machines through this one",
            "its owner can add you to mesh_trust",
        ));
    }
    // Never send a peer's call straight back to that peer: it already has it.
    if let Some(conn) = conn
        && let Some(id) = to.peer_id()
        && peer_on_in(&st, conn).is_some_and(|(caller, _)| caller == id)
    {
        return Err(RpcError::new("denied", "that address points back to its caller", ""));
    }
    // Our callers travel by our id (the machine names them; see `from_peer`);
    // a peer names callers from here itself.
    if to.machine_id().is_some()
        && let Some(caller) = params["caller"].as_str()
    {
        params["caller"] = format!("{}/{caller}", core.info.id).into();
    }
    Ok(Some((to, params)))
}

/// Returns once the machine's line is past dialing (or `machine_timeout` passed).
pub async fn dialed(core: &Core, id: &str) {
    let wait = crate::config::load(&core.paths.config).unwrap_or_default().machine_timeout();
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let changed = core.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if !matches!(core.lock().lines.get(id), Some(LineState::Connecting)) || tokio::time::Instant::now() >= deadline
        {
            return;
        }
        let _ = tokio::time::timeout_at(deadline, changed).await;
    }
}

/// The open line to `m` (waiting while it is first dialed), or why there is none.
async fn line(core: &Core, m: &Machine) -> Result<Arc<Line>, RpcError> {
    dialed(core, &m.id).await;
    let st = core.lock();
    match st.lines.get(&m.id) {
        Some(LineState::Connected(line)) => Ok(line.clone()),
        Some(LineState::NeedsLogin { .. }) => Err(RpcError::new(
            "unavailable",
            format!("{} needs a login typed by a person", m.name),
            if cfg!(windows) {
                format!("set up its SSH keys, then: hc machine login {}", m.name)
            } else {
                format!("in a terminal: hc machine login {}", m.name)
            },
        )),
        Some(LineState::NeedsUpdate { .. }) => Err(RpcError::new(
            "needs_update",
            format!("{}'s hc speaks another protocol generation", m.name),
            format!("hc machine update {}", m.name),
        )),
        Some(LineState::Unreachable { why, .. }) => Err(RpcError::new(
            "unavailable",
            format!("{} is unreachable: {why}", m.name),
            "hc machine ls shows when it comes back",
        )),
        _ => Err(RpcError::new("unavailable", format!("still connecting to {}", m.name), "try again in a moment")),
    }
}

/// Sends a call to `m` and returns the reply with addresses as this core sees them.
/// How long a call there may take: `machine_timeout`, plus the wait it asks
/// for when it waits (`wait`, a waited `run` or `send`), and template readiness.
fn deadline_for(core: &Core, method: &str, params: &Value) -> Duration {
    let config = crate::config::load(&core.paths.config).unwrap_or_default();
    if method == "machine.exec" {
        // Its own timeout, then the time to end everything it started.
        let secs = params["timeout"].as_f64().unwrap_or(crate::exec::DEFAULT_TIMEOUT).clamp(0.0, 600.0);
        return config.machine_timeout() + Duration::from_secs_f64(secs) + Duration::from_secs(5);
    }
    let waits =
        method == "terminal.wait" || (params["wait"] == true && matches!(method, "terminal.run" | "terminal.send"));
    let wait = match params["timeout"].as_f64() {
        Some(secs) => secs,
        None => config.wait_timeout.unwrap_or(180.0),
    };
    // Only the remote core knows whether argv names one of its templates.
    let ready = if method == "terminal.run" { crate::templates::ready_timeout() } else { Duration::ZERO };
    config.machine_timeout()
        + ready
        + if waits { Duration::from_secs_f64(wait.clamp(0.0, 86_400.0)) } else { Duration::ZERO }
}

fn waits(method: &str, params: &Value) -> bool {
    method == "terminal.wait" || (params["wait"] == true && matches!(method, "terminal.run" | "terminal.send"))
}

fn line_lost(e: &RpcError) -> bool {
    e.message.starts_with("lost the line")
}

/// A wait there outlives a dropped line: once the line is back it waits
/// again, until its own deadline. A run is found again by a label it was
/// started with; its output is read once it is done.
async fn forward_waiting(core: &Core, m: &Machine, method: &str, mut params: Value) -> Result<Value, RpcError> {
    let deadline = tokio::time::Instant::now() + deadline_for(core, method, &params);
    let mark = (method == "terminal.run").then(crate::platform::random_hex);
    if let Some(mark) = &mark {
        params["labels"]["hc.run"] = mark.clone().into();
    }
    let first = match forward_once(core, m, method, params.clone()).await {
        Err(e) if line_lost(&e) && method != "terminal.send" => e,
        done => return done,
    };
    let caller = params["caller"].clone();
    let mut target = params["terminal"].as_str().map(String::from);
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(first);
        }
        dialed(core, &m.id).await;
        if line(core, m).await.is_err() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        if target.is_none()
            && let Some(mark) = &mark
        {
            let found = forward_once(core, m, "terminal.list", json!({"all": true, "labels": {"hc.run": mark}})).await;
            target = found.ok().and_then(|r| r["terminals"][0]["address"].as_str().map(String::from));
            if target.is_none() {
                return Err(first); // it never started there
            }
        }
        let Some(terminal) = target.clone() else { return Err(first) };
        let local = terminal.split_once('/').map_or(terminal.as_str(), |(_, t)| t).to_string();
        let left = deadline.saturating_duration_since(tokio::time::Instant::now()).as_secs_f64();
        let wait = json!({"terminal": local, "timeout": left, "caller": caller});
        let r = match forward_once(core, m, "terminal.wait", wait).await {
            Err(e) if line_lost(&e) => continue,
            other => other?,
        };
        return Ok(match method {
            "terminal.run" => {
                let mut record = forward_once(core, m, "terminal.get", json!({"terminal": local})).await?;
                let read = json!({"terminal": local, "lines": crate::api::FINISHED_LINES});
                let text =
                    forward_once(core, m, "terminal.read", read).await.map(|r| r["text"].clone()).unwrap_or_default();
                let id = crate::terminals::parse_id(&local).unwrap_or_default();
                record["wait"] = r;
                record["output"] = crate::api::tail(text.as_str().unwrap_or_default().to_string(), id).into();
                record
            }
            "terminal.send" => json!({"status": r["status"], "turn": r["turn"], "wait": r}),
            _ => r,
        });
    }
}

pub async fn forward(core: &Core, m: &Machine, method: &str, params: Value) -> Result<Value, RpcError> {
    match waits(method, &params) {
        true => forward_waiting(core, m, method, params).await,
        false => forward_once(core, m, method, params).await,
    }
}

async fn forward_once(core: &Core, m: &Machine, method: &str, mut params: Value) -> Result<Value, RpcError> {
    if method == "terminal.run" {
        // An older core ends a child with its parent only when asked to.
        params["die_with_parent"] = json!(params["outlive_parent"] != true);
    }
    let deadline = if method == "terminal.send" { Duration::MAX } else { deadline_for(core, method, &params) };
    let line = line(core, m).await?;
    // An older core would read no program as "nothing to run".
    let own_shell = params["argv"].as_array().is_none_or(Vec::is_empty) || params.get("line").is_some();
    if method == "terminal.run" && own_shell && !line.features.iter().any(|f| f == "shell") {
        return Err(RpcError::new(
            "needs_update",
            format!("{}'s hc is older and can't pick its own shell", m.name),
            format!("hc machine update {}", m.name),
        ));
    }
    let reply = line.call(&m.name, method, params, deadline).await.map_err(|e| match e.kind.as_str() {
        "unknown_method" => RpcError::new(
            "needs_update",
            format!("{}'s hc is older and has no {method}", m.name),
            format!("hc machine update {}", m.name),
        ),
        _ => e,
    })?;
    // Document contents are opaque: address-shaped keys belong to the client.
    // So are machines: a machine's name is its own, not an address to prefix
    // (a mesh shows them as `via` instead).
    let reply = if method.starts_with("doc.") || method == "machine.list" {
        reply
    } else {
        localize(core, &m.name, &[], reply)
    };
    // It started work there: its child may now call back and send it notes.
    if method == "terminal.run"
        && let Some(parent) = reply["parent"].as_str().and_then(crate::terminals::parse_id)
    {
        let mut st = core.lock();
        if let Some(t) = st.reg.terminals.get_mut(&parent) {
            t.afar.insert(m.id.clone());
            core.save(&st);
        }
    }
    Ok(reply)
}

pub async fn forward_to(core: &Core, to: &To, method: &str, params: Value) -> Result<Value, RpcError> {
    match to {
        To::Machine(m) => forward(core, m, method, params).await,
        To::Peer { id, name } => peer_call(core, id, name, method, params, true).await,
        // `personal:` A mesh hop names everything past it already: its reply
        // speaks the names the caller knows, so nothing is added here.
        To::Mesh(via) => match via.as_ref() {
            To::Peer { id, name } => peer_call(core, id, name, method, params, false).await,
            To::Machine(m) => forward(core, m, method, params).await,
            To::Mesh(_) => unreachable!("a mesh hop always ends at one neighbor"),
        },
    }
}

async fn peer_call(
    core: &Core,
    id: &str,
    name: &str,
    method: &str,
    params: Value,
    localize_reply: bool,
) -> Result<Value, RpcError> {
    let found = core.lock().peer_lines.get(id).map(|(_, l)| l.clone());
    let line =
        found.ok_or_else(|| RpcError::new("unavailable", format!("{name} is not connected"), "try again later"))?;
    let calls_me = core.lock().peers.get(id).map(|p| p.calls_me.clone()).unwrap_or_default();
    // Reverse calls have no incoming heartbeat monitor: leave room for a
    // walked-away draft, but still bound a stuck peer. Its config may differ:
    // never shorten the default allowance using our local cap.
    let draft = if method == "terminal.send" {
        crate::input::draft_timeout(&core.paths.config)
            .max(crate::input::default_draft_timeout())
            .max(crate::input::quiet())
            + crate::server::heartbeat_every().min(Duration::from_secs(5))
    } else {
        Duration::ZERO
    };
    let deadline = deadline_for(core, method, &params) + draft;
    let reply = line.call(name, method, params, deadline).await?;
    // A machine's name is its own: machine.list is not an address.
    Ok(if !localize_reply || method.starts_with("doc.") || method == "machine.list" {
        reply
    } else {
        localize(core, name, &[calls_me.as_str()], reply)
    })
}

/// Addresses from the core called `name`: its `t7` is `name/t7` here; ours
/// (by our id, our name, or an `alias` it calls us) are bare again.
pub fn localize(core: &Core, name: &str, aliases: &[&str], v: Value) -> Value {
    localize_in(core, &core.lock(), name, aliases, v)
}

fn map_addresses(v: &mut Value, map: &dyn Fn(&str) -> String) {
    match v {
        Value::Object(o) => {
            for (k, v) in o.iter_mut() {
                match v {
                    Value::String(s) if ["address", "parent", "terminal", "from", "removed"].contains(&k.as_str()) => {
                        *s = map(s)
                    }
                    _ => map_addresses(v, map),
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(|v| map_addresses(v, map)),
        _ => {}
    }
}

fn localize_in(core: &Core, st: &State, name: &str, aliases: &[&str], mut v: Value) -> Value {
    let map = |s: &str| match s.split_once('/') {
        None => format!("{name}/{s}"),
        Some((prefix, rest)) if prefix == core.info.id || prefix == core.info.name || aliases.contains(&prefix) => {
            rest.to_string()
        }
        Some((prefix, rest)) => match st.machines.iter().find(|m| m.id == prefix) {
            Some(m) => format!("{}/{rest}", m.name),
            None => s.to_string(),
        },
    };
    map_addresses(&mut v, &map);
    v
}

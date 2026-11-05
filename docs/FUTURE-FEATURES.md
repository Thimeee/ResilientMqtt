# ResilientMqtt — Future Features & Enhancements

> **Purpose:** the design backlog for releases after **1.0.3**. Each item says *why*, the *design*, the
> *config/API surface*, and *effort/priority*. Nothing here is a correctness fix (those shipped in 1.0.3 — see
> [CHANGELOG.md](../CHANGELOG.md)); these are **new capabilities and polish** that keep the core small while
> making the library best-in-class for large unattended fleets (e.g. 500+ banking kiosks).
>
> **Guiding principles** (from [ARCHITECTURE.md](ARCHITECTURE.md) / [ROADMAP.md](ROADMAP.md)):
> keep the **core dependency-free**; ship heavy backends as **optional extension packages**; every addition is
> **backward-compatible** (new options default to today's behaviour); prefer **reliability over feature count**.

---

## ⭐ Flagship: Universal, pluggable Offline Buffering

### Where it stands today (1.1.0 — unchanged since 1.0.3)

The buffer is the "outbox" that holds publishes when the broker is unreachable and replays them on reconnect.
Today:

- One interface — `IOfflineBuffer` (`Count` / `TryEnqueue` / `TryPeek` / `TryDequeue` / `ClearAsync`).
- Two built-ins — `InMemoryOfflineBuffer` (a `ConcurrentQueue`, drop-oldest at `MaxSize`) and `NullOfflineBuffer`
  (discards, used when disabled).
- Already pluggable in code — `builder.UseOfflineBuffer<TBuffer>()` swaps the implementation.
- Config — `OfflineBuffer.Enabled` / `MaxSize` / `DrainPauseMs`.
- Drain — single-flight (1.0.3 #16), timeout-bounded per message (1.0.3 #7), `DrainPauseMs` between messages.

**Limits:** in-memory only (lost on process restart); the interface is synchronous (awkward for I/O backends);
you must pick the backend in *code*, not *config*.

### The vision — one feature, many backends, chosen by config

Make the buffer a **provider model**: the same `IOfflineBuffer` seam, several interchangeable backends, selected
from `appsettings.json`. Core stays dependency-free; durable backends ship as extension packages.

```jsonc
"OfflineBuffer": {
  "Enabled": true,
  "Provider": "Sqlite",        // InMemory | Channel | Sqlite | Redis   (default: InMemory)
  "MaxSize": 1000,
  "DrainPauseMs": 50,
  "OverflowPolicy": "DropOldest", // DropOldest | DropNewest | Block   (Block only for Channel)

  "Sqlite": { "Path": "%MainPath%/buffer.db", "Table": "offline_messages" },
  "Redis":  { "ConnectionString": "localhost:6379", "Key": "mqtt:outbox:{ClientId}" }
}
```

### The backends

| Provider | Package | Durable? | Deps | Use it for |
|----------|---------|----------|------|-----------|
| **InMemory** (today) | core | ❌ (lost on restart) | none | telemetry (latest-wins), simplest |
| **Channel** | core | ❌ | none (`System.Threading.Channels`) | in-memory but with real async back-pressure — `Block` overflow (await instead of drop), bounded, single-reader drain. A cleaner in-memory engine than the queue. |
| **Sqlite** | `ResilientMqtt.Buffering.Sqlite` | ✅ survives restart/crash | `Microsoft.Data.Sqlite` | banking zero-loss of status / patch-progress across an agent restart during an outage |
| **Redis** | `ResilientMqtt.Buffering.Redis` | ✅ shared/durable | `StackExchange.Redis` | server hubs / multi-instance, or a shared outbox |
| **custom** | yours | — | — | `UseOfflineBuffer<T>()` — already supported, keep it |

### Design details to get right

1. **Async interface.** SQLite/Redis are I/O-bound; the current sync `TryEnqueue/TryPeek/TryDequeue` forces
   sync-over-async. Add an **optional** `IAsyncOfflineBuffer` (`EnqueueAsync/PeekAsync/DequeueAsync/CountAsync`)
   that the drain prefers when the backend implements it; keep the sync interface for in-memory and back-compat.
   *(Additive — no break.)*
2. **At-least-once semantics on the drain.** Keep today's *peek → publish → dequeue* order: a crash between
   publish and dequeue re-sends on restart (a duplicate, acceptable under QoS1). Durable backends make this a
   real guarantee. Document it.
3. **Overflow policy** becomes explicit: `DropOldest` (today), `DropNewest`, or `Block` (Channel only — apply
   back-pressure to the publisher instead of losing data).
4. **Provider auto-selection** happens in `AddResilientMqtt` from `OfflineBuffer.Provider`. If a durable
   provider is named but its extension package isn't referenced, **fail at `ValidateOnStart`** with a clear
   message ("Provider=Sqlite requires the ResilientMqtt.Buffering.Sqlite package"). Core never takes the deps.
5. **Message serialization for durable backends** — persist topic + payload + QoS + retain (+ enqueue time so
   an old message can be TTL-dropped). Reconstruct `MqttApplicationMessage` on peek.
6. **Ordering caveat** (already noted #12) — drained (old) and live (new) publishes both take `_publishLock`, so
   a new message can overtake drained ones. Fine for latest-wins; call it out for ordered-command use.

### Rollout

- **Phase 1 (core):** add `Provider` + `OverflowPolicy` options; add the `Channel` backend; keep InMemory the
  default (zero behaviour change). Add `IAsyncOfflineBuffer`.
- **Phase 2 (extension pkg):** `ResilientMqtt.Buffering.Sqlite` — the highest-value one for the kiosk fleet.
- **Phase 3 (extension pkg):** `ResilientMqtt.Buffering.Redis` for server/multi-instance.

**Priority: HIGH** (Sqlite in particular — banking zero-loss). **Effort:** core provider model = M; each backend = M.

---

## Other new features

| Feature | Why | Design sketch | Priority · Target |
|---------|-----|---------------|-------------------|
| ✅ **Statistics / ConnectionInfo API** — *core shipped in 1.1.0 (`ConnectionInfo`, `LastFailure`); message-rate counters + `OnStatistics` hook still open* | Fleet ops: surface per-kiosk MQTT health on the server. Pairs with the 1.0.3 `StillDown` signal. | A read-only `ConnectionInfo` snapshot: `State`, `LastConnectedUtc`, `ReconnectCount`, `CurrentBackoff`, `BufferedCount`, `PublishedTotal`, `DroppedTotal`, `LastError`. Expose via a property + an `OnStatistics` sampling hook. | HIGH · 1.1 |
| ✅ **`WaitForConnectionAsync(timeout)`** — *shipped 1.1.0* | Await "connected" instead of polling `IsConnected`; natural home for callers that must block until ready. | `Task<bool> WaitForConnectionAsync(TimeSpan timeout, CancellationToken ct)`; completes on the next Connected transition or times out. | MED · 1.1 |
| **`FlushAsync(timeout)`** | Drain the buffer on demand (e.g. before a planned shutdown) rather than relying on the background drain. | Force a bounded drain pass; return when empty or timed out. | MED · 1.1 |
| ✅ **Subscribe/Unsubscribe timeout** — *shipped 1.1.0 (bounded by `ConnectTimeoutSeconds`)* | The last remaining unbounded await — the SUBACK wait in `SubscribeAsync`/`ResubscribeAllAsync`. Low risk (runs right after a successful connect) but closes the gap. | Bound with `WithTimeout` using a new `SubscribeTimeoutSeconds` (default 15); a timed-out resubscribe is logged and retried on the next reconnect. | MED · 1.0.4 |
| **OpenTelemetry / metrics hook** | Standardised metrics + traces for enterprise monitoring. | Extension package `ResilientMqtt.OpenTelemetry` (already on the roadmap) emitting from the Statistics surface above. | LOW · 1.4 |
| ✅ **Opt-in non-blocking handler dispatch** — *shipped 1.1.0 as `Dispatch.Mode = Concurrent` (bounded by `MaxConcurrency`)* | Guard against a consumer writing a slow message handler that blocks the receive pump (#8 foot-gun). | `OfflineBuffer`-style option `Dispatch = Inline | Background`; `Background` runs each handler on `Task.Run` (delivery becomes fire-and-forget — document it). | LOW · 1.1 |

---

## Enhancements to existing features

| Area | Today | Enhancement | Priority |
|------|-------|-------------|----------|
| **Health check cadence** | `IntervalMinutes` / `IdleThresholdMinutes` (whole minutes) | Add `IntervalSeconds` / `IdleThresholdSeconds` for finer zombie-detection on critical links (keep minute options for back-compat). Beware ping load × fleet size. | LOW |
| **Reconnect backoff** | fixed base/max/jitter | Optional `MaxAttempts` (0 = infinite, the correct default for kiosks) for consumers that *want* to give up; expose `CurrentBackoff` via ConnectionInfo. | LOW |
| ✅ **Logging** — *shipped 1.1.0 (`ResilientMqttLogEvents`)* | string messages, `[ResilientMqtt…]` prefix | Structured `EventId`s per event class (Connected/Reconnect/PublishFailed/…) for precise filtering. | LOW |
| **Graceful shutdown** | publishes OFFLINE then DISCONNECT | Optional bounded `FlushAsync` on `DisconnectAsync` so buffered messages get a last chance to drain before close. | LOW |

---

## Deliberately NOT planned (scope discipline)

Keeping the core small — these stay out unless a concrete need appears:

- **MQTT v5 user-properties / request-response** — not exposed. (Correction: the client has always spoken MQTT 5.0 by default via MQTTnet 5; since 1.1.0 the protocol is explicit (`ProtocolVersion`) and v5 **reason codes are classified** into `LastFailure`.)
- **WebSocket transport** — direct TCP covers the fleet; raw MQTTnet is the escape hatch.
- **A max-reconnect cap as the default** — unattended kiosks *must* retry forever; a cap is opt-in only.
- **Broker/server-side features** — this is a client library (server concerns belong elsewhere).

---

## Suggested release order

1. ✅ **1.1.0 (2026-10-02)** — failure reasons + ConnectionInfo + `WaitForConnectionAsync` + subscribe timeout + sessions
   done right + mutual TLS/CA + concurrent dispatch + publish outcomes + EventIds + test suite (see CHANGELOG).
2. **1.2** — `FlushAsync`, message-rate statistics, buffering provider model (core: `Channel` backend,
   `Provider`/`OverflowPolicy` options, `IAsyncOfflineBuffer`), credential provider / hot reload.
3. **1.3** — `ResilientMqtt.Buffering.Sqlite` (banking zero-loss) + `ResilientMqtt.Buffering.Redis`.
4. **1.4+** — OpenTelemetry package, per existing roadmap.

*Companion to [CHANGELOG.md](../CHANGELOG.md) (shipped changes) and [ROADMAP.md](ROADMAP.md) (high-level vision).*

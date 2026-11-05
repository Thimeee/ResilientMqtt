> 📄 **Copy** of the consuming project's `docs/ResilientMqtt-Research.md` (Kiosk-Agent repository), kept here so the library's own history lives with the library. Copied 2026-10-05. Links to project files (agent / backend paths) only resolve in that repository; the project copy stays the one that is updated.

# ResilientMqtt — Deep Research & Audit

> **Scope:** Full line-by-line audit of the `ResilientMqtt` library
> (`D:\Self Stady\importent\libry\.net\ResilientMqtt\latest\ResilientMqtt`, **v1.0.2**, on **MQTTnet 5.1.0.1559**)
> as used by the 500-kiosk unattended agent fleet.
> **Trigger:** 3 live terminals (T0002/T0003/T0005) — all previously stable, no config change — got
> **stranded "disconnected" after a server-side network blip**. One (T0002) stayed stuck ~24 min until a
> **manual service restart**; another (T0003) self-healed after ~11 min. During the stall the log shows
> `Health check: client reports disconnected` every 60s with **no** new `Reconnect #N` line for the whole
> window.
> **Verdict:** this is a **real library bug** — a reconnect *stall* (a connect attempt with no handshake
> timeout holds the single-flight reconnect latch for 11–24+ minutes), not a config problem. Confirmed
> against three real logs (2026-09-11). Root cause, evidence, and fix below. Nothing in the library has
> been changed — this is the research the owner asked for.
>
> **Correction vs. my first pass:** I initially called this a *permanent* deadlock (no reconnect lines at
> all). The logs prove it's a **long but temporary stall** — the reconnect loop *does* run and *does*
> print `Reconnect #N`, but a single hung connect attempt freezes it for many minutes. The root cause and
> the fix are the same; the "permanent until restart" wording was wrong and is corrected here.

---

## TL;DR (honest summary)

| # | Severity | Issue | Confidence |
|---|----------|-------|------------|
| **1** | 🔴 **CRITICAL** | **Reconnect stall** — the MQTT **handshake/authenticate wait has no timeout** (only the TCP socket-connect does). A half-open reconnect hangs inside `_client.ConnectAsync` for 11–24+ min, holding the single-flight `_isReconnecting` latch, so every health-check reconnect trigger no-ops for that whole window. | **CONFIRMED** — 3 live logs |
| 2 | 🟠 HIGH | Post-connect user handler (`OnConnected`) is awaited **while holding `_connectLock` + the reconnect latch** → a slow/hung `ResumePendingAsync`/`PublishInventoryAsync` is a second deadlock path | CONFIRMED (code path) |
| 3 | 🟠 HIGH | `_reconnectCts` is read/written/disposed from multiple threads with **no synchronization** → lost shutdown-cancel + possible `ObjectDisposedException` | CONFIRMED |
| 4 | 🟡 MED | No max-attempt escalation / "still-down" surfaced event → fleet-wide outage is invisible to the server until heartbeats stop | By design, but a gap at 500-kiosk scale |
| 5 | 🟡 MED | `OfflineBuffer.Enabled=false` in the agent → status/patch-progress messages during a blip are **dropped**, not replayed | Project config, not a lib bug |
| 6 | 🟢 LOW | `CleanSession=false` + offline buffer interplay; jitter/backoff tuning for 500 clients | Mostly fine — notes only |
| **7** | 🟠 **HIGH** | **Publish has no operation timeout** — `_client.PublishAsync` (QoS1/2 awaiting PUBACK) can hang on a half-open socket **holding `_publishLock`**, which then blocks *every* later `telemetry/health` + `status` publish and the buffer drain. Same root class as #1, on the send side. | CONFIRMED (code) |
| **8** | 🟢 **LATENT** (lib) / ✅ safe here | **Long-running message handlers would block the MQTTnet receive pump + keep-alive** — handlers are `await`ed inline in `OnMessageReceivedAsync`. **VERIFIED 2026-09-13: both consumers already offload heavy work**, so this does not bite the project today — it's a library foot-gun to fix for the next user. | VERIFIED safe (agent + server) |
| 9 | 🟡 MED | **No timeout on health-check `PingAsync`** → a hung ping leaves `_isRunning=1`, silently killing the health monitor (no more ping/disconnect detection) | CONFIRMED (code) |
| 10 | 🟡 MED | **Circuit-breaker HalfOpen admits *all* concurrent publishes**, not a single probe → thundering-herd onto a just-recovered broker | CONFIRMED (code) |
| 11 | 🟡 MED | **TLS `Mode="Normal"` accepts ANY certificate** (MITM-open) — the agent ships `Normal`; banking prod must be `Hard` (thumbprint pin) | Security / config |
| 12 | 🟢 LOW | `_isShuttingDown` latched permanently by `DisconnectAsync` → client is **single-use** (no reconnect after a manual disconnect); `_state` not volatile; handlers get `CancellationToken.None` | CONFIRMED (code) |
| **13** | 🟠 **HIGH** | **A failed/initial `ConnectAsync` does not self-schedule a reconnect** — the reconnect loop only starts from a *disconnect event*, a *publish while down*, or the *health check* (which only starts after a first success). Cold-start against a down broker never retries on its own; your agent only recovers because the 30s heartbeat publish happens to trigger it. A pure-subscriber client would hang forever. | CONFIRMED (code), 2nd pass |
| 14 | 🟢 LOW | `DisconnectAsync` doesn't `Stop()` the health check → after a graceful disconnect without `Dispose`, the timer keeps firing/logging every 60s | CONFIRMED, 2nd pass |
| 15 | 🟢 LOW | health-check `Start()` on every reconnect can collide with an in-flight `OnTick` (`_isRunning` still 1) → first new tick skipped (self-heals) | CONFIRMED, 2nd pass |
| **16** | 🟡 **MED** | **No single-flight guard on `DrainOfflineBufferAsync`** — overlapping drains (two quick reconnects) both `TryPeek` the head, both publish it (**duplicate**), then one `Dequeue`s the *next* message and it's **silently dropped, never sent**. Dormant today (buffer off) but **activates the moment you do #5** (enable the buffer). | CONFIRMED (code), 3rd pass |
| 17 | 🟢 LOW | Shutdown race — `DisposeAsync` disposes `_publishLock`/`_connectLock` while a background publish/drain/reconnect may still `await` them → `ObjectDisposedException` on shutdown | CONFIRMED (code), 3rd pass |
| **18** | 🟡 **MED** (doc) | **Documented trim-safety doesn't exist** — README §19 / USAGE §16 claim an embedded `ILLink.Descriptors.xml` protects config binding under trimming; the `.csproj` has no such resource and the file isn't in the repo. Anyone who trusts the docs and enables trimming gets silent config-binding breakage. (Mitigated here — the agent publishes self-contained, untrimmed.) | CONFIRMED (docs pass) |

**The one that bit you is #1** (stall) — but **#7 and #8 would strand a kiosk the same way** under a blip
*during* a patch/transfer, and are just as important for 500 unattended terminals. #2, #3, #7, #9 are all the
same "no timeout / lock held forever" family and should be fixed together.

> **Investigation status — CONVERGED at 18 (as of 2026-09-13).** Five review passes were done: (1) first code
> sweep, (2) full client re-read, (3) concurrency/edge focus, (4) all five shipped docs, (5) a final
> end-to-end re-read of the complete `ResilientMqttClient.cs`. **Pass 5 found no new distinct issue** — the two
> things it surfaced (the in-connect ONLINE publish and the graceful OFFLINE-publish-then-skip-DISCONNECT) are
> sub-cases of #7/#12 already listed, now folded into the #7 fix. The **static surface (every `.cs` file + every
> doc) is exhausted.** Any remaining risk is runtime-only — race timing under real load, broker/MQTTnet-specific
> behaviour — which is found by the **test plan**, not by more reading. Recommendation: stop reviewing, start
> fixing.

---

## 1. 🔴 CRITICAL — Reconnect stall (the bug that stranded the 3 kiosks)

### The mechanism

**a) The MQTT handshake wait has no timeout — only the TCP socket connect does.** `BuildMqttOptions()`
([ResilientMqttClient.cs:74-123](../../../../importent/libry/.net/ResilientMqtt/latest/ResilientMqtt/ResilientMqttClient.cs))
builds the options with `WithClientId / WithTcpServer / WithCleanSession / WithKeepAlivePeriod /
WithCredentials / WithTls` — but **never `WithTimeout(...)`**.

> **Key fact (corrected against the logs):** MQTTnet 5 *does* bound the **TCP socket connect** — the logs
> show `MqttCommunicationTimedOutException: The operation has timed out` from
> `CrossPlatformSocket.ConnectAsync`, and those attempts fail fast and retry cleanly (`Reconnect #2…#8`).
> But the **MQTT-protocol handshake** — `MqttClient.Authenticate` waiting for the CONNACK after the socket
> is up — is bounded **only** by the `CancellationToken` you pass. With no timeout on that token, once the
> TCP socket connects but the broker never returns a CONNACK, `_client.ConnectAsync` **hangs until the OS
> tears the half-open socket down** (observed: 11–24+ minutes), then throws
> `MqttConnectingFailedException: Error while authenticating. Connection closed.`

**b) The single reconnect attempt uses a token that nothing will cancel.** In `TryReconnectAsync()`
(line 429) the loop calls `await ConnectAsync(token)` where `token = _reconnectCts.Token` (line 439).
`ConnectAsync` (line 126) then does:

```csharp
var result = await _client.ConnectAsync(_mqttOptions, ct);   // line 138 — NO handshake timeout → can hang for minutes
```

`_reconnectCts` is only cancelled by (i) a **new** `TryReconnectAsync` entry (line 437) or (ii) shutdown.
While one attempt is stuck in the handshake, neither happens.

**c) The single-flight latch blocks every rescue for the whole stall.** `TryReconnectAsync` guards itself:

```csharp
if (Interlocked.CompareExchange(ref _isReconnecting, 1, 0) != 0) return;   // line 433
...
finally { Interlocked.Exchange(ref _isReconnecting, 0); }                  // line 455 — only on EXIT
```

While the body is parked in the handshake await (**b**), `_isReconnecting` stays **1** and `_connectLock`
stays held for the entire 11–24 min — until the stuck connect finally throws, at which point the loop
resumes and `Reconnect #N+1` appears.

**d) The health check can't break in during the stall.** The unhealthy callback is
`() => Task.Run(TryReconnectAsync)` (line 181). Every tick, `IdleAwareHealthCheck.OnTick`
([IdleAwareHealthCheck.cs:60-65](../../../../importent/libry/.net/ResilientMqtt/latest/ResilientMqtt/Internal/IdleAwareHealthCheck.cs))
sees `!client.IsConnected`, logs **`Health check: client reports disconnected`**, and calls `onUnhealthy()`
→ `TryReconnectAsync` → **instantly returns at the line-433 guard** because `_isReconnecting == 1`. That is
why, during the stall, you see only the 60-second health-check warnings and no new `Reconnect #N`.

### The log evidence (2026-09-11, three terminals)

**T0002 — stuck 24 min, needed the manual restart** (`MainLog-...-3`):
```
18:28:56 [INF] Reconnect #1 in 1s          ← one attempt scheduled…
18:28:56 [WRN] Health check: client reports disconnected
18:29:56 [WRN] Health check: client reports disconnected   ← then ONLY this, every 60s,
  … (23 more identical lines, 18:30 → 18:52) …                 no Reconnect #2 for ~24 min
18:53:38 [INF] Application is shutting down…  ← you restarted it; then it reconnected on boot
```

**T0003 — same stall, self-healed after ~11 min** (`MainLog-...-1`):
```
18:28:57 [INF] Reconnect #1 in 1s
18:28:57 [WRN] Health check: client reports disconnected
  … health-check-only, every 60s, 18:29 → 18:40, no Reconnect #2 …
18:40:33 [ERR] ConnectAsync threw: MqttConnectingFailedException: Error while authenticating. Connection closed.
18:40:33 [INF] Reconnect #2 in 1.5s        ← the stuck handshake finally died → loop resumes
18:40:37 [INF] Connected — as terminal-T0003  ← recovered on its own
```

Earlier in the same logs, when the failure is at the **TCP layer**, the loop behaves perfectly —
`Reconnect #2…#8` each ~20–25 s apart with `MqttCommunicationTimedOutException`, recovering at 18:02:31.
The difference is entirely **socket-timeout (bounded) vs. handshake-wait (unbounded)**.

### The triggering condition (why a "network blip" does it)

After a server-side blip, the broker often hasn't yet detected the old TCP session drop (keep-alive is 40s),
so the agent reconnects with the **same client id** (`terminal-T0003`) into a broker that still holds the
previous session. The CONNECT then stalls or the broker closes mid-authenticate — and because the handshake
wait is unbounded, the attempt parks for minutes instead of failing fast. A load balancer / stateful
firewall in front of the broker makes the half-open window longer. This fits "network wala mokak hari awlak
una server eke" exactly, and explains why all three terminals hit it at once with no config change.

> **Note on my first pass:** I originally inferred "no Reconnect #N lines ⇒ permanent deadlock." The logs
> show the loop *does* resume once the stuck handshake dies (T0003). It is a **multi-minute stall**, not a
> permanent lock. During the stall you see exactly what's described above — grep the full daily log for
> `Reconnect #`: it stops at one attempt, goes quiet for 11–24 min, then jumps to the next number.

### The fix

**Bound the connect.** Wrap the MQTTnet connect in a **timeout-linked CTS** so a stuck handshake throws after
N seconds, returns `false`, releases `_connectLock` **and** the `_isReconnecting` latch (via the existing
`finally`), and lets the reconnect loop proceed to the next attempt.

```csharp
// ResilientMqttClient.ConnectAsync — replace the single connect line (138)
var connectTimeout = TimeSpan.FromSeconds(Math.Max(5, _options.Reconnect.ConnectTimeoutSeconds)); // new opt, default 15
using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
timeoutCts.CancelAfter(connectTimeout);
var result = await _client.ConnectAsync(_mqttOptions, timeoutCts.Token).ConfigureAwait(false);
```

The existing `catch (OperationCanceledException) { return false; }` (line 185) already handles the timeout
firing — it returns false and the loop schedules the next attempt. **No other change needed to break the
deadlock.** (Optionally also add `.WithTimeout(connectTimeout)` in `BuildMqttOptions` as belt-and-braces,
but the linked-CTS is the reliable one on MQTTnet 5.)

Add to `ReconnectOptions` (ResilientMqttOptions.cs):
```csharp
/// <summary>Max seconds to wait for a single connect/handshake before giving up and retrying. Default 15.</summary>
public int ConnectTimeoutSeconds { get; set; } = 15;
```

---

## 2. 🟠 HIGH — `OnConnected` handler awaited under the locks (second deadlock path)

In the `ConnectAsync` success path, the user's `OnConnected` handler is **awaited inline** (line 173,
`await RaiseOnConnectedAsync()`) **before** the lock is released and **while** `_isReconnecting` is still 1
(when reached via reconnect).

In **this project** the agent's `OnConnected` does real work:
`await ResumePendingAsync(); await PublishInventoryAsync()` (AppInitializationWorker). If either hangs — a
patch-resume that runs a script, or an inventory publish that blocks on a flaky broker with QoS1 awaiting
PUBACK — the connect never completes, `_connectLock` + `_isReconnecting` stay held, and you get the **same
stranding** as #1 even though the socket is up.

`RaiseOnConnectedAsync` already isolates *exceptions* per-handler (lines 553-569) — good — but it does **not**
bound *time*.

**Fix options (library side, pick one):**
- Run post-connect user handlers on a background task (like the buffer drain already is, line 177):
  `_ = Task.Run(RaiseOnConnectedAsync)` — but then ordering vs. buffer-drain changes; document it.
- Or apply a timeout to `RaiseOnConnectedAsync` and log if a handler exceeds it.

**Fix option (project side, no lib change):** make the agent's `OnConnected` handler return fast — kick
`ResumePendingAsync`/`PublishInventoryAsync` onto their own `Task.Run` and don't block the connect callback.
This is the safer immediate move and needs no library release.

> **✅ VERIFIED 2026-09-13 — already done in the agent.** `AppInitializationWorker.OnMqttConnectedAsync`
> ([AppInitializationWorker.cs:214-243](../agent/monitoring_agent/HostedService/AppInitializationWorker.cs))
> already runs `ResumePendingAsync`/`PublishInventoryAsync`/`ResyncUnackedAsync` via `_ = Task.Run(...)`; the
> only awaited work is the 5 fast `SubscribeAsync` calls (the broker has just CONNACK'd, so it's responsive).
> No project change needed — #2 is handled like #8. (Residual, minor: `SubscribeAsync`/`ResubscribeAllAsync`
> SUBACKs aren't timeout-bounded — a future 1.0.4 could add a subscribe timeout, but the risk is negligible
> since it runs immediately after a successful connect.)

---

## 3. 🟠 HIGH — `_reconnectCts` thread-safety

`_reconnectCts` (field, line 30) is:
- cancelled + reassigned in `TryReconnectAsync` (lines 437-438),
- cancelled in `DisconnectAsync` (line 463) and `DisposeAsync` (line 508),
- disposed in `DisposeAsync` (line 520),

all from **different threads** (two `Task.Run(TryReconnectAsync)` sources + the shutdown caller) with **no
lock**. Two real consequences:

1. **Lost shutdown cancel:** if `DisconnectAsync` calls `_reconnectCts.Cancel()` in the window between
   line 437 (`Cancel()` old) and line 438 (assign new) in `TryReconnectAsync`, the shutdown cancels the
   *old, already-dead* CTS and the freshly-created one keeps a reconnect loop alive during shutdown.
2. **`ObjectDisposedException`:** `DisposeAsync` disposes the CTS while a background `TryReconnectAsync` may
   still call `.Token`/`.Cancel()` on it.

**Fix:** guard all `_reconnectCts` access with a small lock (or `Interlocked.Exchange` the reference), and
null-check/swallow `ObjectDisposedException` on the shutdown path. Fixing #1 with a linked CTS per-attempt
(scoped `using`) also shrinks this surface.

---

## 4. 🟡 MEDIUM — No "still-down" escalation at fleet scale

The reconnect loop is intentionally **infinite** (correct for unattended kiosks — we *want* them to keep
trying). But there is **no event/metric** for "this client has been down for > X / > N attempts". With 500
kiosks, a broker/LB problem is only visible on the server when heartbeats dry up — there's no direct signal
from the agent side.

**Improvement:** expose the `StateChanged` event (already present, line 46) + `_reconnectAttempt` so the host
can log/alert after a threshold. Low effort, high operational value for the bank. No correctness impact.

---

## 5. 🟡 MEDIUM — Offline buffer is OFF in the agent

Agent `appsettings.json` ships `Mqtt.OfflineBuffer.Enabled=false`. During a blip, **any** publish
(`telemetry/health`, `status`, **patch progress**) is dropped, not replayed — `PublishBytesAsync` enqueues to
a `NullOfflineBuffer` that discards (NullOfflineBuffer.cs:13). For 30s heartbeats that's acceptable (next one
covers it). For **patch-progress / status transitions** it means the server can miss an event across a blip.

**Recommendation:** enable a **small** in-memory buffer (e.g. `MaxSize=200`) so status/patch messages survive
a short blip and drain on reconnect (the drain path is solid — lines 277-306). Don't make it huge: it's
in-memory, lost on process restart, and you don't want a flood after a long outage. Project config only.

---

## 6. 🟢 Notes / smaller items

- **`async void OnTick`** (IdleAwareHealthCheck.cs:50) — acceptable (standard timer pattern; the body is
  fully try/caught), but by convention worth a `// intentional async void` comment.
- **Circuit breaker + reconnect interaction:** the breaker guards the *publish* path only (line 242), not the
  connect path — correct, but means a wedged broker doesn't trip any breaker on connect. Fine given #1's fix.
- **Backoff/jitter for 500 clients:** `ExponentialBackoffStrategy` uses full jitter in `[baseDelay,
  exponential]` (ExponentialBackoffStrategy.cs:36-43) with Base 1s / Max 60s — **good**; this is exactly what
  spreads a 500-kiosk reconnect storm after a server blip. Keep `UseJitter=true`. No change.
- **`CleanSession=false`** (good for banking — broker retains subs + QoS1/2) pairs well with resubscribe on
  reconnect (ResubscribeAllAsync, lines 362-379). Consistent. No change.

---

## 7. 🟠 HIGH — Publish path can hang holding `_publishLock` (no publish timeout)

`PublishBytesAsync` (lines 223-274) acquires `_publishLock` (line 257) then does
`await _client.PublishAsync(message, ct)` (line 260). For QoS1/2 that awaits a PUBACK/PUBCOMP. **No timeout**
— and the `ct` is whatever the caller passed, which for the agent's heartbeat/status publishes is usually
`CancellationToken.None`. On a half-open socket the publish awaits indefinitely, **holding `_publishLock`**.
Because every `telemetry/health`, `status`, and patch-progress publish — **and** the offline-buffer drain
(line 289) — all take the same single `_publishLock`, one stuck publish **serializes the whole agent to a
halt**. This is the #1 bug's twin on the send side, and at 30-second heartbeats it means the server sees the
kiosk go silent.

**Fix:** bound the publish with a linked-CTS timeout (mirror of #1):
```csharp
var pubTimeout = TimeSpan.FromSeconds(Math.Max(5, _options.PublishTimeoutSeconds)); // new opt, default 15
using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
cts.CancelAfter(pubTimeout);
await _client.PublishAsync(message, cts.Token).ConfigureAwait(false);
```
The existing `catch` already records a breaker failure and buffers the message — so a timed-out publish is
treated exactly like a failed one, which is correct. Add `PublishTimeoutSeconds` (default 15) to options.

---

## 8. 🟠 HIGH — Long-running message handlers block the receive pump + keep-alive

`OnMessageReceivedAsync` (lines 382-403) runs each matching handler with
`await entry.Handler.HandleAsync(...)` **inline**. MQTTnet awaits the `ApplicationMessageReceivedAsync`
delegate before it reads the next packet **and before it answers the broker's keep-alive PINGREQ**. So any
handler that blocks — and in this project `control/patch` triggers a full patch apply and `control/files`
triggers multi-GB up/downloads — **freezes all inbound MQTT and lets the keep-alive lapse**, so the broker
disconnects the kiosk in the middle of its own patch/transfer.

> The 2026-09-11 logs hint at this: T0002 performed a 60 MB upload, and several `control/files` operations
> ran seconds before disconnects. A blocking handler is a plausible contributor alongside the network blip.

**Fix — two places, pick per concern:**
- **Library:** in `OnMessageReceivedAsync`, dispatch handlers so a slow one can't block the pump (e.g.
  `_ = Task.Run(() => entry.Handler.HandleAsync(topic, payload, token))`), accepting that delivery becomes
  fire-and-forget (document it; most MQTT apps want this). Or expose an option.
- **Project:** handlers must **return immediately** and run heavy work on their own task.

> **✅ VERIFIED 2026-09-13 — both consumers are already safe, no project change needed:**
> - **Agent `PatchHandler`** — `_ = Task.Run(() => _patchService.ApplyAsync(...))`, returns `Task.CompletedTask`
>   ([PatchHandler.cs:37](../agent/monitoring_agent.Infrastructure/Handlers/PatchHandler.cs)). `cancelPatch`
>   likewise fire-and-forget.
> - **Agent `FilesHandler`** — `upload`/`download` (the multi-GB paths) are `_ = Task.Run(...)` fire-and-forget
>   ([FilesHandler.cs:39-40](../agent/monitoring_agent.Infrastructure/Handlers/FilesHandler.cs)); only `browse`/
>   `delete` (fast, in-memory) are awaited inline. Both paths have their own try/catch.
> - **Server (one client, `terminal/+/…` wildcards)** — *every* handler (`HealthHandler`, `StatusHandler`,
>   `PatchProgressHandler`, `LiveHealthHandler`, `FilesResponseHandler`, `InventoryHandler`) deserializes then
>   calls `_dispatcher.OnXxx(...)`, which does a single `_channel.Writer.TryWrite(env)` on a **bounded
>   50 000-slot channel with `DropOldest`** and returns instantly
>   ([TelemetryDispatcher.cs:70-74](../backend/MonitoringServer/MonitoringServer.Infrastructure/Realtime/TelemetryDispatcher.cs)).
>   A single background consumer does all store/SignalR/DB work — the MQTT receive pump is never blocked, and
>   under overload stale heartbeats are dropped in favour of freshness.
>
> **Conclusion:** #8 is a real library foot-gun but is **fully mitigated in this project** by disciplined
> handlers. Fix it in the library for robustness/other users, but it is **not** on the critical path for your
> fleet. It does **not** need a project code change.

---

## 9. 🟡 MEDIUM — Health-check ping has no timeout

`IdleAwareHealthCheck.OnTick` (IdleAwareHealthCheck.cs:72) calls `await client.PingAsync()` with **no
timeout** under the `_isRunning` single-flight latch (line 52). If a ping hangs on a half-open socket,
`_isRunning` stays 1 and **every future tick returns immediately** — the health monitor is silently dead, so
the one mechanism that detects a wedged-but-"connected" socket stops working. Pass a `CancellationToken` with
a few-second `CancelAfter` to `PingAsync`.

---

## 10. 🟡 MEDIUM — Circuit-breaker HalfOpen admits everything

In `CircuitBreaker.TryAcquire` (CircuitBreaker.cs:59-61) both `Closed` and `HalfOpen` return `true` for **all**
callers. A proper half-open should let **one** probe through and hold the rest until it succeeds/fails. As
written, the instant the reset timeout elapses, every queued + new publish floods the just-recovered broker at
once. Low blast-radius per kiosk, but multiplied across 500 kiosks recovering together it's a real surge.
**Fix:** allow a single in-flight probe in HalfOpen (a `_probeInFlight` flag).

---

## 11. 🟡 MEDIUM (security) — `Tls.Mode="Normal"` trusts any certificate

`BuildMqttOptions` TLS (lines 99-120): `"Normal"` installs `WithCertificateValidationHandler(_ => true)` —
accepts **any** server certificate, i.e. no protection against a man-in-the-middle on the broker link. The
agent ships `MqttTlsMode='Normal'`. For a bank LAN it's "works", but **production must use `"Hard"`** (pins
the broker thumbprint — the code path at lines 108-116 is correct). Track this with the go-live security flip
(signature-on + certs). Library is fine; this is a config/hardening note.

---

## 12. 🟢 LOW — smaller correctness/robustness notes

- **Client is single-use after `DisconnectAsync`.** `DisconnectAsync`/`DisposeAsync` set `_isShuttingDown=1`
  permanently (lines 461/507); `ConnectAsync` then early-returns `false` (line 129). Fine for the agent (it
  only disconnects at shutdown), but any future code that calls `DisconnectAsync` for a *transient* reason
  would permanently kill the client. Worth a one-line XML-doc warning.
- **`_state` is a plain field** written from connect/disconnect/reconnect threads and read by `State` with no
  barrier (line 36, 45, 540-544) → possible stale reads of the public `State`. Make it `volatile` or lock.
- **Handlers get `CancellationToken.None`** (line 394) → inbound work can't be cancelled on shutdown/disconnect.
- **Every matching subscription fires** for a received topic (lines 389-391) — if patterns overlap (a wildcard
  plus an exact topic) a message is delivered to both handlers. Intended for pub/sub, but document it so nobody
  adds overlapping subscriptions by accident.
- **Buffered-vs-live ordering:** drained (older) messages and new live publishes both contend for
  `_publishLock` with no sequencing, so a new heartbeat can overtake a drained one. Fine for latest-wins
  telemetry; note it if you ever buffer ordered commands.

---

## 13. 🟠 HIGH — A failed/initial connect doesn't schedule a reconnect (2nd-pass find)

`ConnectAsync`'s failure paths just return `false`:
```csharp
catch (OperationCanceledException) { return false; }                 // line 185
catch (Exception ex) { ...; SetState(Disconnected); return false; }   // line 186-191
```
Nothing here calls `TryReconnectAsync`. The reconnect loop is only ever started by:
- `OnDisconnectedAsync` (line 424) — but that requires the client to have **been connected** (MQTTnet only
  raises `DisconnectedAsync` after a successful connect), and
- `PublishBytesAsync` when `!IsConnected` (line 252-253), and
- the health check callback — but `IdleAwareHealthCheck.Start` is only called **on a successful connect**
  (line 180-181), so it never runs after a cold failure.

So if the broker is **down when the agent boots**, the auto-start connect fails and **no retry is ever
scheduled**. This agent survives only by accident: the `Worker` heartbeat publishes every 30 s, and the
publish-while-disconnected path (line 252) kicks a reconnect. A client that only *subscribes* (no periodic
publish) would sit disconnected forever.

**Fix:** in `TryReconnectAsync`'s trigger points, also start the loop after a failed connect — simplest is, in
the auto-start hosted service and in `ConnectAsync`'s failure path, schedule `TryReconnectAsync` when
`Reconnect.Enabled`. Cleanest: have `ConnectAsync` (when called outside the reconnect loop) kick
`TryReconnectAsync` on failure.

---

## 14. 🟢 LOW — `DisconnectAsync` doesn't stop the health check (2nd-pass find)

Only `DisposeAsync` calls `_healthCheck.Stop()` (line 509). After a graceful `DisconnectAsync` that isn't
followed by `Dispose`, the timer keeps ticking every 60 s, seeing `!IsConnected`, logging
`Health check: client reports disconnected`, and calling `onUnhealthy` → `TryReconnectAsync` → bails on
`_isShuttingDown`. Harmless but noisy. **Fix:** call `_healthCheck.Stop()` in `DisconnectAsync` too.

---

## 15. 🟢 LOW — health-check `Start()` vs. in-flight `OnTick` (2nd-pass find)

`IdleAwareHealthCheck.Start` is called on every successful (re)connect; it disposes the old timer and creates
a new one, but an already-running `OnTick` holds `_isRunning=1` and disposing the timer doesn't stop it. The
first tick of the new timer may no-op on the stale latch. Cosmetic, self-heals next interval. **Fix:** reset
`_isRunning=0` in `Start` (or accept it — lowest priority).

---

## 16. 🟡 MEDIUM — overlapping buffer drains duplicate + drop messages (3rd-pass find)

`DrainOfflineBufferAsync` (lines 277-306) has **no single-flight guard**. It's launched
`_ = Task.Run(() => DrainOfflineBufferAsync(CancellationToken.None))` on **every** successful connect
(line 176-177). A quick disconnect→reconnect can start a second drain while the first is still looping (it
`await Task.Delay(DrainPauseMs)` between messages and waits on `_publishLock`). The loop does:
```csharp
if (!_buffer.TryPeek(out var msg) ...) break;   // A and B both peek msg1
await _publishLock...; await _client.PublishAsync(msg, ct);   // A publishes msg1, B publishes msg1 (DUP)
_buffer.TryDequeue(out _);                        // A dequeues msg1, B dequeues msg2 (msg2 LOST)
```
`TryPeek` then `TryDequeue` is **not atomic across two drainers**, so the head is double-sent and the *next*
message is removed without ever being published.

Today the agent runs with the buffer **off**, so this is dormant — but **recommendation #5 is to turn the
buffer on**, and that is exactly when this bug wakes up, and it would **silently lose a buffered patch/status
message** across a reconnect. Fix #5 and #16 together.

**Fix:** single-flight the drain with an `Interlocked` latch (like `_isReconnecting`), and make peek+publish+
dequeue safe — simplest is `TryPeek` → publish → `TryDequeue` guarded so only one drainer runs at a time:
```csharp
if (Interlocked.CompareExchange(ref _isDraining, 1, 0) != 0) return;
try { /* existing loop */ } finally { Interlocked.Exchange(ref _isDraining, 0); }
```

---

## 17. 🟢 LOW — shutdown race on disposed semaphores (3rd-pass find)

`DisposeAsync` (lines 504-522) disposes `_publishLock` and `_connectLock`. A background publish / drain /
reconnect task started just before shutdown may still be about to `await _publishLock.WaitAsync(ct)`
(line 257, **outside** the try/catch) → `ObjectDisposedException`. Shutdown-only and mostly cosmetic, but a
clean library should: set `_disposed`/`_isShuttingDown` first, give in-flight tasks a moment (or track and
await them), and swallow `ObjectDisposedException` on the shutdown path. Pairs with the #3 CTS guard.

---

## 18. 🟡 MEDIUM (doc) — documented trim-safety doesn't exist + doc/behaviour mismatches (4th-pass find)

A full read of the shipped docs (`README.md`, `docs/USAGE.md`, `docs/ARCHITECTURE.md`, `docs/ROADMAP.md`,
`docs/EXTENSIONS.md`) surfaced one concrete gap and two corroborations:

**Build-blocker found while implementing (2026-09-13):** the `.csproj` references `<None Include="USAGE.md">`
and `<PackageReadmeFile>USAGE.md</PackageReadmeFile>`, but the file now lives at `docs/USAGE.md`. A full
`dotnet build`/`dotnet pack` fails with `MSB3030: Could not copy … USAGE.md` — so **1.0.3 cannot be packed
until this path is corrected** (compile-only succeeds, which is why it went unnoticed). One-line fix in the
csproj (`docs\USAGE.md`), folded into #18/Phase E but required before the pack step in §6 rollout.

**The gap — trim-safety is promised but absent.** README §19 and USAGE §16 state the library *"ships with an
`ILLink.Descriptors.xml` embedded resource that tells the trimmer NOT to strip the options classes"*, and the
README feature matrix lists *"Trim-safe configuration ✅ Built-in"*. But `ResilientMqtt.csproj` contains **no
`<EmbeddedResource Include="ILLink.Descriptors.xml" />`** (it only embeds `USAGE.md` + `icon.jpeg`) and the XML
file is **not in the repository**. So a consumer who reads "it's built-in, you're fine" and enables trimming
will hit the exact silent config-binding breakage the docs warn about. **Fix:** either add a real
`ILLink.Descriptors.xml` (or `[DynamicDependency]`/`DynamicallyAccessedMembers` attributes on the options
binding) and embed it, **or** correct the docs to say trimming is unsupported. For *this* project it's
mitigated (the agent publishes self-contained and untrimmed per the guidance), so it's not on the critical
path — but it's a false guarantee to fix before calling 1.0.3 "best-in-class".

**Corroboration 1 — #13 is a bug, not a design choice.** README §20's "Reconnect state machine" diagram shows
*any* `ConnectAsync` failure feeding back into "retry forever until shutdown". The code only does that for a
failure *inside* the reconnect loop or *after* a first success — a cold-start failure doesn't retry. The docs
describe the behaviour #13 should have, which confirms the author intended it to retry.

**Corroboration 2 — the "fully thread-safe" claim is overstated.** FAQ §22 answers *"Is `IResilientMqttClient`
thread-safe? Yes."* Findings #3 (`_reconnectCts`), #12 (`_state`), #16 (drain), and #17 (dispose) show several
unsynchronised multi-thread paths. Fixing those makes the FAQ's promise actually true.

**Also noted (not bugs):** the ROADMAP lists `PingAsync()`, `WaitForConnectionAsync()`, `FlushAsync()`,
Statistics/ConnectionInfo as planned **v1.1** items — so the #4 "still-down / observability" improvement aligns
with the library's own roadmap (ConnectionInfo/Statistics), and a public `WaitForConnectionAsync()` would be a
natural home for the cold-start fix in #13.

---

## Structure assessment — should we rewrite it? **No.**

You asked whether the library's *structure* needs changing. My honest read after the full pass: **the
architecture is sound — keep it, harden it.** What's good and worth preserving:

- Clean seams: `IRetryStrategy`, `IHealthCheck`, `IOfflineBuffer`, `ICircuitBreaker` are all pluggable with
  Null-object defaults (`NullHealthCheck`/`NullOfflineBuffer`/`NullCircuitBreaker`) — this is exactly how a
  resilience library should be factored, and it's why a SQLite-backed persistent buffer could drop in later.
- Proper options pattern + `ValidateOnStart` + a fluent builder + an optional hosted service.
- `CleanSession=false` + resubscribe-on-connect + jittered backoff are the *right* choices for banking/fleet.

**Every issue above is a behaviour/robustness bug inside that good structure — not a structural flaw.** The
theme is singular: **"an await with no timeout while a lock or a latch is held."** That one pattern explains
#1, #2, #3, #7, #9. Fixing it (bounded awaits + release the latch) makes the library genuinely fleet-grade
without touching the shape of the code. A rewrite would add risk and throw away a clean design. So:
**strengthen in place, bump to 1.0.3.**

---

## What ResilientMqtt has cost this project — and the future risk

**Already happened (2026-09-11, confirmed in the logs):**
- 3 kiosks (T0002/T0003/T0005) went "disconnected" after one server-side network blip.
- T0002 stayed dark **~24 minutes** and only came back on a **manual service restart** → during that window
  the bank had no telemetry, no remote control, and couldn't push a patch to that terminal.
- T0003 self-healed but only after **~11 minutes** of no connectivity.
- Root cause = #1 (unbounded connect handshake holding the reconnect latch).

**What can still happen at 500-kiosk scale if left unfixed:**
- **Mass stranding on every WAN blip.** The same blip hits many branches at once; each can stall 10–25 min.
  Without #1's fix, a 30-second network hiccup becomes a 20-minute fleet outage — and staff "fix" it by
  restarting services, branch by branch.
- **Stranded *mid-patch* (#7).** If a blip lands while a kiosk is applying a patch or transferring a file, a
  progress/status publish can hang holding `_publishLock`, so the kiosk goes silent — the server shows it
  stuck "InProgress" and the resume/rollback logic is starved of the progress messages it needs. (The
  receive-pump variant, #8, is already neutralised — heavy work runs off-thread in both consumers.)
- **Invisible outages (#4).** Today the only signal is "heartbeats stopped"; there's no agent-side "I've been
  down N minutes" alert, so a partial fleet outage can go unnoticed until someone looks.
- **Dropped status/patch events across blips (#5)** because the offline buffer is off.
- **A thundering-herd re-storm (#10)** when the broker recovers and 500 breakers flip HalfOpen at once.

None of these require the *network* to be reliable — they're about the agent **recovering cleanly and
observably** when the network isn't. That's the whole job of this library, so it's worth getting right.

---

## The 1.0.3 remediation plan — fix everything, publish a best-in-class build

**Design principle for the whole release:** every change is **additive + backward-compatible**. New options
default to the *current* behaviour's safe value, nothing is renamed or removed, and the public API in
`IResilientMqttClient` is untouched. An app on 1.0.2 that upgrades and changes no config gets *strictly safer*
behaviour, never different semantics. That is what makes it safe to drop the new DLL under both the agent and
the server.

**The one refactor that kills most bugs:** a small private helper that bounds any MQTTnet call with a linked
CTS. #1, #7, #9 (and the graceful-disconnect publish) all become one-liners through it:
```csharp
// ResilientMqttClient — single bounded-await helper
private static async Task<T> WithTimeout<T>(
    Func<CancellationToken, Task<T>> op, TimeSpan timeout, CancellationToken ct)
{
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
    cts.CancelAfter(timeout);
    return await op(cts.Token).ConfigureAwait(false);
}
```

### Phase A — connection resilience (the core fixes: #1, #3, #13)

| Issue | File | Change |
|-------|------|--------|
| **#1** connect handshake hang | `ResilientMqttClient.ConnectAsync` (line 138) | Wrap the connect in `WithTimeout(..., ConnectTimeoutSeconds)`. Existing `catch(OperationCanceledException){return false}` already turns a timeout into a clean retry. |
| **#3** `_reconnectCts` races | `ResilientMqttClient` (lines 30/437-438/463/508/520) | Guard all access behind a small `lock (_ctsGate)`; swap via a local; swallow `ObjectDisposedException` on the shutdown path. The per-attempt scoped CTS from #1 shrinks the window further. |
| **#13** cold-start never retries | `ResilientMqttClient.ConnectAsync` failure path + auto-start | When a connect invoked **outside** the reconnect loop fails and `Reconnect.Enabled`, kick `_ = Task.Run(TryReconnectAsync)`. (Add an `isReconnectLoop` flag so the loop doesn't recurse into itself.) |

New option: `ReconnectOptions.ConnectTimeoutSeconds = 15`.

### Phase B — publish + send-path resilience (#7, and the graceful-disconnect publish)

| Issue | File | Change |
|-------|------|--------|
| **#7** publish hang holds `_publishLock` | `PublishBytesAsync` (line 260) + `DrainOfflineBufferAsync` (line 292) + the in-connect **ONLINE** publish (line 161) + the graceful **OFFLINE** publish (line 479) | Route **every** `_client.PublishAsync` through `WithTimeout(..., PublishTimeoutSeconds)`. The existing `catch` records a breaker failure and buffers — a timed-out publish is treated as a failed one (correct). Note the ONLINE publish runs under `_connectLock`, so an unbounded one also hangs the connect → the timeout protects that path too. In `DisconnectAsync`, a throwing OFFLINE publish currently skips the clean DISCONNECT (line 487) — bound it *and* send the DISCONNECT in a `finally` so graceful shutdown still closes cleanly. |

New option: top-level `PublishTimeoutSeconds = 15` (or under a new `PublishOptions`).

### Phase C — health check (#9, #15) and lifecycle (#14)

| Issue | File | Change |
|-------|------|--------|
| **#9** ping can hang | `IdleAwareHealthCheck.OnTick` (line 72) | `await client.PingAsync(ct)` with a `CancelAfter(PingTimeoutSeconds)` linked token. |
| **#15** Start vs in-flight tick | `IdleAwareHealthCheck.Start` | `Interlocked.Exchange(ref _isRunning, 0)` when (re)starting. |
| **#14** health check not stopped on disconnect | `ResilientMqttClient.DisconnectAsync` | Call `_healthCheck.Stop()` on the graceful path too. |

New option: `HealthCheckOptions.PingTimeoutSeconds = 10`.

### Phase D — operational hardening (#4, #10, #2) — still backward-compatible

| Issue | File | Change |
|-------|------|--------|
| **#4** no "still-down" signal | `ResilientMqttClient.TryReconnectAsync` | Raise the already-exposed `StateChanged` (+ a new optional `ReconnectAttempt`/`DownSince` surface, or an `OnStillDown` event) after N attempts / T minutes so the host can alert. Purely additive. |
| **#10** HalfOpen admits all | `CircuitBreaker.TryAcquire` | In `HalfOpen`, allow a single probe (`_probeInFlight` flag); others get `false` until the probe resolves. |
| **#16** overlapping drains dup/drop | `ResilientMqttClient.DrainOfflineBufferAsync` | Single-flight the drain with an `Interlocked` latch. **Must ship before/with #5** (enabling the buffer) or the buffer will silently lose a message across a reconnect. |
| **#17** shutdown race on disposed locks | `ResilientMqttClient.DisposeAsync` / `PublishBytesAsync` | Order disposal after in-flight tasks settle; swallow `ObjectDisposedException` on the shutdown path. |
| **#2** `OnConnected` awaited under locks | (library) document + optionally offer `RaiseConnectedOnBackground` option; **(project)** make the agent handler return fast | Library stays safe-by-default; project-side mitigation below. |

### Phase E — correctness polish (#11, #12)

- **#11 (security):** no code change — document that prod must use `Tls.Mode="Hard"`; the agent flips to `Hard`
  at go-live (tracked with the signature-on + certs task). Optionally log a **warning at startup** when
  `Tls.Enabled && Mode=="Normal"` so an insecure prod config is loud.
- **#12:** make `_state` `volatile`; XML-doc the "single-use after `DisconnectAsync`" contract; pass a real
  cancellation token (tied to shutdown) into `HandleAsync` instead of `CancellationToken.None`; XML-doc the
  "all matching subscriptions fire" semantics.
- **#18 (docs/trimming):** either add a real `ILLink.Descriptors.xml` (embed it in the csproj) or
  `[DynamicallyAccessedMembers]` attributes on the options binding so trim-safety is actually true — **or**
  correct README §19 / USAGE §16 and the feature matrix to say trimming is unsupported. Also fix the two
  doc/behaviour mismatches (reconnect state-machine diagram vs. #13; the "fully thread-safe" FAQ vs.
  #3/#12/#16/#17) once the code fixes land.

### New options — complete list (all default to safe/no-change)

```csharp
ReconnectOptions.ConnectTimeoutSeconds = 15;   // #1, #13
PublishTimeoutSeconds                  = 15;   // #7  (top-level or PublishOptions)
HealthCheckOptions.PingTimeoutSeconds  = 10;   // #9
ReconnectOptions.StillDownAfterSeconds = 120;  // #4  (0 = disabled → no behaviour change)
```
`ValidateOnStart` (ResilientMqttServiceCollectionExtensions.cs:82) should validate these are > 0.

### Test plan (prove each fix before publishing)

1. **#1 / #13** — point at a TCP listener that accepts then never sends a CONNACK (a `nc -l` / a stub) → assert
   `ConnectAsync` returns false within ~`ConnectTimeoutSeconds`, `_isReconnecting` clears, and `Reconnect #N`
   keeps advancing. Boot with broker down → assert it retries and connects when the broker comes up.
2. **#7** — publish QoS1 to a broker that black-holes PUBACK → assert the publish times out, the message is
   buffered, and a *second* publish is not blocked.
3. **#9** — wedge a ping → assert the health check recovers on the next tick (doesn't die).
4. **#3** — stress: hammer connect/disconnect on multiple threads + Dispose → no `ObjectDisposedException`,
   no leaked reconnect loop after shutdown.
5. **#10** — force the breaker Open→HalfOpen with many queued publishes → assert only one probe goes out.
6. **Regression** — run the existing agent + server against the new DLL in a lab: blip the network (pull the
   broker for 30 s) and confirm **all** terminals reconnect within a backoff cycle with no manual restart.

### Versioning, publish & rollout

1. Bump `<Version>` to **1.0.3** in `ResilientMqtt.csproj`; add a `CHANGELOG`/release note listing #1–#15.
2. `dotnet build -c Release` + `dotnet pack` → produces `ResilientMqtt.1.0.3.nupkg` (+ snupkg symbols;
   `GeneratePackageOnBuild` is already on).
3. Push to your NuGet feed (or update the local `PackageReference` for the agent + server).
4. Bump the `PackageReference` to `1.0.3` in **both** the agent and the `MonitoringServer` backend, rebuild.
5. **Project-side #2 mitigation** (ship with the same agent build): make `OnConnected` return fast — run
   `ResumePendingAsync`/`PublishInventoryAsync` on their own task so the connect callback never blocks.
6. **Redeploy:** agent via the self-update pipeline; backend via `swap-server.ps1`. No broker/agent-config
   change is needed for the library fixes — the new defaults are safe.
7. **Optional config once 1.0.3 is in:** enable a small offline buffer (`OfflineBuffer.Enabled=true`,
   `MaxSize≈200`) for #5 — **but only after #16 is in the build** (otherwise the buffer can duplicate/drop a
   message across a reconnect) — and set `Tls.Mode="Hard"` at go-live for #11.

### Suggested commit sequence (small, reviewable)

- `fix(reconnect): bound connect with timeout + schedule retry on cold/failed connect (#1,#13)`
- `fix(reconnect): guard _reconnectCts across threads (#3)`
- `fix(publish): bound publish + drain + graceful-disconnect sends with timeout (#7)`
- `fix(health): timeout ping, reset run-latch on start, stop on disconnect (#9,#14,#15)`
- `feat(resilience): still-down signal + single-probe half-open (#4,#10)`
- `fix(buffer): single-flight drain to stop duplicate-send + dropped message (#16)`
- `chore: safe shutdown ordering + swallow ObjectDisposedException (#17)`
- `chore: volatile state, real handler ct, xml-docs, startup warn on insecure TLS (#11,#12)`
- `fix(trim): add real ILLink descriptors + correct docs/thread-safety claims (#18)`
- `chore: bump to 1.0.3 + changelog`

---

> I have **not** modified the library or the project. This is the plan you asked for. Say the word and I'll
> start writing it — my suggested order is **Phase A → B → C** (the "kiosk goes dark" fixes) first, then
> **D → E**, committing per the sequence above so you can review each step. I'll also do the project-side #2
> mitigation in the same pass.

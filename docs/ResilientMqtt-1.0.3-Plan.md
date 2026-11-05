> 📄 **Copy** of the consuming project's `docs/ResilientMqtt-1.0.3-Plan.md` (Kiosk-Agent repository), kept here so the library's own history lives with the library. Copied 2026-10-05. Links to project files (agent / backend paths) only resolve in that repository; the project copy stays the one that is updated.

# ResilientMqtt 1.0.3 — Full Remediation & Hardening Plan

> **Goal:** fix all 18 findings from the audit ([ResilientMqtt-Research.md](ResilientMqtt-Research.md)) and
> ship a genuinely best-in-class 1.0.3 — **without changing the library's structure** and **without any
> breaking change**. Every new option defaults to the current safe behaviour, so dropping the new DLL under the
> agent and the server changes nothing until you opt in.
>
> **Library:** `D:\Self Stady\importent\libry\.net\ResilientMqtt\latest\ResilientMqtt` · v1.0.2 · MQTTnet 5.1.0.1559
> **Consumers:** the kiosk agent **and** the MonitoringServer backend (both use `IResilientMqttClient`).
> **Owner executes; Claude can write the code on request. Nothing is changed yet.**

---

## 0. How to read this plan

- **Section 1** groups the 18 issues by root cause — fix a *theme*, not 18 unrelated things.
- **Section 2** is the honest **fix / defer / ignore** decision for every issue, with reasons.
- **Section 3** is the **phase-by-phase work** (A–E) with per-issue fix, file, effort, risk, and test.
- **Section 4** is the complete **new-options** surface (all default to no-change).
- **Section 5** is the **test plan**; **Section 6** versioning/publish/rollout; **Section 7** the commit sequence.
- **Section 8** is a one-screen **execution checklist**.

**Can all 18 be fixed? Yes — every one is fixable.** 14 get a code fix, 1 is verified-already-safe (#8), 2 are
config/doc actions (#11, #18), and a few low-severity ones are *optional polish* you may consciously defer
(called out explicitly in Section 2 — "defer" ≠ "ignore forever", it means "safe to ship 1.0.3 without it").

---

## 1. Root-cause grouping — 18 issues, 5 themes

Most of the list collapses into a few root causes. Fixing the root fixes several findings at once.

| Theme | Root cause | Issues | Fix in one place |
|-------|-----------|--------|------------------|
| **T1. Unbounded awaits under a lock/latch** | no timeout on connect/publish/ping while holding `_connectLock`/`_publishLock`/`_isReconnecting` | **#1, #7, #9**, contributes to #2 | a single `WithTimeout(...)` helper |
| **T2. Reconnect can't make progress** | latch not released / loop not started / CTS races | **#1, #3, #13, #2** | bounded connect (T1) + start-on-failure + CTS lock |
| **T3. Concurrency correctness** | shared state touched from many threads without guards | **#3, #12, #16, #17** | small `lock`s / `Interlocked` latches / `volatile` |
| **T4. Recovery hygiene / observability** | health-check lifecycle gaps, no outage signal, breaker flood | **#9, #10, #14, #15, #4** | stop/ reset health check, single-probe half-open, StillDown signal |
| **T5. Security & docs truthfulness** | insecure default option, promised-but-absent trim safety | **#11, #18** | config guidance + real trim descriptors/doc fix |
| **Verified safe (no action)** | — | **#8** | handlers already offload heavy work in both consumers |

---

## 2. Decision table — fix / defer / ignore (with reasons)

| # | Sev | Decision | Why |
|---|-----|----------|-----|
| **1** | 🔴 | **FIX (Phase A)** | The confirmed bug that stranded the kiosks. Non-negotiable. |
| **2** | 🟠 | **FIX — project side now + library guard** | Project: make agent `OnConnected` non-blocking (free, immediate). Library: optional background-raise guard. |
| **3** | 🟠 | **FIX (Phase A)** | Thread-safety of reconnect CTS; cheap, same code area as #1. |
| **7** | 🟠 | **FIX (Phase B)** | Publish hang freezes all telemetry; twin of #1 on the send side. |
| **13** | 🟠 | **FIX (Phase A)** | Cold-start never retries; the author intended it to (docs prove it). |
| **4** | 🟡 | **FIX (Phase D)** | High operational value at 500 kiosks; additive event. Aligns with roadmap v1.1. |
| **5** | 🟡 | **FIX = config, but ONLY after #16** | Enable a small buffer — gated on #16 landing or it can drop a message. |
| **9** | 🟡 | **FIX (Phase C)** | A hung ping silently kills the health monitor — the thing that triggers reconnect. |
| **10** | 🟡 | **FIX (Phase D)** | HalfOpen flood matters at fleet scale; small change. |
| **11** | 🟡 | **CONFIG action (go-live)** | No code bug — flip TLS to `Hard` + optional startup warning. Tracked with certs task. |
| **16** | 🟡 | **FIX (Phase D) — MUST precede #5** | Overlapping-drain dup/drop; dormant until the buffer is on. |
| **18** | 🟡 | **FIX = docs/trim (Phase E)** | Correct the false trim-safety claim (or add real descriptors). |
| **12** | 🟢 | **FIX (Phase E) — cheap polish** | `volatile` state, real handler ct, xml-docs. Low risk, makes the "thread-safe" FAQ true. |
| **14** | 🟢 | **FIX (Phase C)** | One line — stop health check on graceful disconnect. |
| **15** | 🟢 | **DEFER-OK (Phase C if touching the file)** | Cosmetic; self-heals next tick. Fix it while in `IdleAwareHealthCheck`, else safe to skip. |
| **17** | 🟢 | **DEFER-OK (Phase A/E)** | Shutdown-only `ObjectDisposedException`, already swallowed in most paths. Nice-to-have. |
| **8** | 🟢 | **NO ACTION for the project** | Verified: both consumers offload heavy work. Library foot-gun note only. |

**Nothing is "ignored and forgotten."** The only items you may consciously ship 1.0.3 *without* are the two
cosmetic LOWs (#15, #17) — and even those are trivial to include if you're already editing those files. Every
other finding is either fixed in 1.0.3 or is a deliberate config/go-live action with a reason.

> **Why we do NOT "ignore" low-severity issues silently:** a resilience library's whole value is trust. A
> known LOW left undocumented becomes tomorrow's 2 a.m. mystery. So each one is either fixed or explicitly
> marked "defer-OK, here's why" — never dropped without a trace.

---

## 3. The work, phase by phase

**The unifying refactor (do first, enables T1):** one private helper in `ResilientMqttClient`.
```csharp
private static async Task<T> WithTimeout<T>(
    Func<CancellationToken, Task<T>> op, TimeSpan timeout, CancellationToken ct)
{
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
    cts.CancelAfter(timeout);
    return await op(cts.Token).ConfigureAwait(false);
}
```

### Phase A — connection resilience  *(the “kiosk goes dark” core — #1, #3, #13)*

| # | File · anchor | Fix | Effort | Risk |
|---|---------------|-----|--------|------|
| 1 | `ResilientMqttClient.ConnectAsync` (line 138) | `await WithTimeout(t => _client.ConnectAsync(_mqttOptions, t), connectTimeout, ct)`. Existing `catch(OperationCanceledException){return false}` turns the timeout into a clean retry. | S | Low |
| 3 | `ResilientMqttClient` (fields + 437-438/463/508/520) | Guard all `_reconnectCts` access with a `lock (_ctsGate)`; swap via local; swallow `ObjectDisposedException` on shutdown. | S | Low |
| 13 | `ConnectAsync` failure path + `ResilientMqttHostedService` | When a connect **outside** the reconnect loop fails and `Reconnect.Enabled`, `_ = Task.Run(TryReconnectAsync)`. Add an `isReconnectLoop` bool param to `ConnectAsync` so the loop doesn't re-trigger itself. | M | Low-Med |

New option: `ReconnectOptions.ConnectTimeoutSeconds = 15`.
**Exit test:** CONNACK black-hole stub → connect fails in ~15 s, latch clears, `Reconnect #N` advances; boot with broker down → retries and connects when broker returns.

### Phase B — publish / send path  *(#7)*

| # | File · anchor | Fix | Effort | Risk |
|---|---------------|-----|--------|------|
| 7 | `PublishBytesAsync` (260), `DrainOfflineBufferAsync` (292), in-connect ONLINE publish (161), graceful OFFLINE publish (479) | Route **every** `_client.PublishAsync` through `WithTimeout(..., PublishTimeoutSeconds)`. In `DisconnectAsync`, bound the OFFLINE publish **and** send the DISCONNECT in a `finally` so a throwing publish still closes cleanly. | M | Low |

New option: `PublishTimeoutSeconds = 15` (top-level).
**Exit test:** PUBACK black-hole → publish times out, message buffered, a second publish is not blocked.

### Phase C — health check & lifecycle  *(#9, #14, #15)*

| # | File · anchor | Fix | Effort | Risk |
|---|---------------|-----|--------|------|
| 9 | `IdleAwareHealthCheck.OnTick` (72) | `await client.PingAsync(t)` with `CancelAfter(PingTimeoutSeconds)`. | S | Low |
| 14 | `ResilientMqttClient.DisconnectAsync` | Call `_healthCheck.Stop()` on the graceful path. | S | Low |
| 15 | `IdleAwareHealthCheck.Start` | `Interlocked.Exchange(ref _isRunning, 0)` on (re)start. | S | Low |

New option: `HealthCheckOptions.PingTimeoutSeconds = 10`.
**Exit test:** wedge a ping → health check recovers on the next tick instead of dying.

### Phase D — observability & fleet hygiene  *(#4, #10, #16)*

| # | File · anchor | Fix | Effort | Risk |
|---|---------------|-----|--------|------|
| 16 | `DrainOfflineBufferAsync` | Single-flight with `Interlocked` `_isDraining` latch (ship **before** enabling the buffer). | S | Low |
| 10 | `CircuitBreaker.TryAcquire` | In `HalfOpen`, admit a single probe (`_probeInFlight`); others get `false` until it resolves. | S | Low |
| 4 | `TryReconnectAsync` | After `StillDownAfterSeconds`/N attempts, raise `StateChanged` (or a new `OnStillDown`/`DownSince`) so the host can alert. Additive. | M | Low |

New option: `ReconnectOptions.StillDownAfterSeconds = 120` (0 = disabled → no behaviour change).
**Exit test:** force Open→HalfOpen with many queued publishes → only one probe goes out; simulate a long outage → the still-down signal fires once.

### Phase E — security, correctness polish & docs  *(#11, #12, #17, #18)*

| # | File · anchor | Fix | Effort | Risk |
|---|---------------|-----|--------|------|
| 11 | `BuildMqttOptions` / startup | No code bug — log a **warning** when `Tls.Enabled && Mode=="Normal"`; flip to `Hard` at go-live (certs task). | S | None |
| 12 | `ResilientMqttClient` | `volatile` `_state`; pass a shutdown-linked token into `HandleAsync` instead of `CancellationToken.None`; xml-doc "single-use after `DisconnectAsync`" + "all matching subscriptions fire". | S | Low |
| 17 | `DisposeAsync` / `PublishBytesAsync` | Order disposal after in-flight tasks settle; swallow `ObjectDisposedException` on shutdown. | S | Low |
| 18 | `ResilientMqtt.csproj` + `README`/`USAGE` | Add a real `ILLink.Descriptors.xml` (embed it) **or** `[DynamicallyAccessedMembers]` on the options binding — **or** correct the docs to state trimming is unsupported. Fix the state-machine diagram (vs #13) and the "fully thread-safe" FAQ (vs #3/#12/#16/#17). | M | Low |

**Project-side #2 (ships with the same agent build, no library release needed):** make the agent `OnConnected`
return fast — run `ResumePendingAsync`/`PublishInventoryAsync` on their own `Task.Run` so the connect callback
never blocks.

---

## 4. Complete new-options surface (all default to current behaviour)

```jsonc
"Mqtt": {
  "PublishTimeoutSeconds": 15,            // #7
  "Reconnect": {
    "ConnectTimeoutSeconds": 15,          // #1, #13
    "StillDownAfterSeconds": 120          // #4  (0 = off)
  },
  "HealthCheck": {
    "PingTimeoutSeconds": 10              // #9
  }
}
```
Add `Validate(...)` rules in `ResilientMqttServiceCollectionExtensions.ValidateOnStart` that these are > 0 (and
`StillDownAfterSeconds >= 0`). **No existing key changes meaning; nothing is removed or renamed.**

---

## 5. Test plan (prove each fix before publishing)

| Test | Proves | How |
|------|--------|-----|
| CONNACK black-hole | #1, #13 | TCP listener accepts then never CONNACKs → connect returns false in ~`ConnectTimeoutSeconds`, latch clears, `Reconnect #N` advances. Boot broker-down → retries then connects. |
| PUBACK black-hole | #7 | Broker swallows PUBACK → publish times out, buffers, second publish not blocked. |
| Wedged ping | #9 | Health monitor recovers next tick, doesn't die. |
| Thread stress | #3, #17 | Hammer connect/disconnect on N threads + Dispose → no `ObjectDisposedException`, no leaked loop after shutdown. |
| Breaker HalfOpen flood | #10 | Many queued publishes at reset → exactly one probe. |
| Buffer drain race | #16 | Two fast reconnects with a full buffer → no duplicate, no dropped message. |
| **Lab blip (integration)** | #1/#2/#7/#13 together | Agent + server on the new DLL; pull the broker 30 s → **all** terminals reconnect within a backoff cycle, **no manual restart**. |

---

## 6. Versioning, publish & rollout

1. Bump `<Version>` → **1.0.3** in `ResilientMqtt.csproj`; write a CHANGELOG listing #1–#18.
2. `dotnet build -c Release` + `dotnet pack` → `ResilientMqtt.1.0.3.nupkg` (+ snupkg; `GeneratePackageOnBuild` already on).
3. Push to the NuGet feed (or update the local `PackageReference`).
4. Bump `PackageReference` → `1.0.3` in **both** the agent and `MonitoringServer`; rebuild.
5. Ship the project-side **#2** mitigation in the same agent build.
6. **Redeploy:** agent via the self-update pipeline; backend via `swap-server.ps1`. No broker/agent-config change
   is needed for the library fixes — the new defaults are safe.
7. **Opt-in config afterwards:** `OfflineBuffer.Enabled=true` + `MaxSize≈200` for #5 **(only once #16 is in)**;
   `Tls.Mode="Hard"` at go-live for #11.

---

## 7. Commit sequence (small, reviewable)

```
fix(reconnect): bound connect with timeout + retry on cold/failed connect  (#1,#13)
fix(reconnect): guard _reconnectCts across threads                          (#3)
fix(publish):   bound publish + drain + online/offline sends with timeout   (#7)
fix(health):    timeout ping, reset run-latch on start, stop on disconnect  (#9,#14,#15)
fix(buffer):    single-flight drain (stops duplicate-send + dropped msg)    (#16)
feat(resilience): still-down signal + single-probe half-open                (#4,#10)
chore: volatile state, real handler ct, safe shutdown, xml-docs, TLS warn   (#11,#12,#17)
fix(trim): real ILLink descriptors + correct docs/thread-safety claims      (#18)
chore: bump to 1.0.3 + changelog
```
Plus, in the agent repo: `fix(agent): make OnConnected non-blocking (#2)`.

---

## 8. Execution checklist

- [x] Add `WithTimeout` helper
- [x] **A** — #1 connect timeout · #3 CTS guard · #13 start-on-failed-connect  *(compiles clean)*
- [x] **B** — #7 publish/drain/online/offline timeouts + DISCONNECT-guarded  *(compiles clean)*
- [x] **C** — #9 ping timeout · #14 stop-on-disconnect · #15 run-latch reset  *(compiles clean)*
- [x] **D** — #16 single-flight drain · #10 single-probe half-open · #4 still-down signal  *(compiles clean)*
- [x] **E** — #11 TLS warn · #12 volatile/ct/xml-docs · #17 safe shutdown · #18 trim descriptors + USAGE.md build fix  *(full build + pack OK)*
- [x] Project — #2 non-blocking `OnConnected` (agent) — **verified already done** (Task.Run heavy work)
- [x] Version bumped to 1.0.3; **`ResilientMqtt.1.0.3.nupkg` builds + packs, 0 errors**
- [x] `ValidateOnStart` covers the 4 new options *(added — ConnectTimeout/PublishTimeout/PingTimeout > 0, StillDown >= 0)*
- [ ] **Integration lab blip test** — agent + server, pull broker 30 s, all reconnect, no restart
- [ ] Push 1.0.3 + CHANGELOG · bump both consumers' PackageReference · rebuild · redeploy
- [ ] (Opt-in) enable small offline buffer after #16 · flip TLS `Hard` at go-live

---

---

## 9. Future updates (deferred — NOT in 1.0.3)

These are enhancements, not correctness fixes. 1.0.3 is complete and solid without them; they add operational
value for the 500-kiosk fleet and are scheduled for a later release. (Verdicts from the 2026-09-14 review.)

| Item | What | Value | Target |
|------|------|-------|--------|
| **Statistics / ConnectionInfo API** | Expose counters: reconnect count, last-connected time, buffered/dropped message counts, current backoff. Pairs with the #4 StillDown signal. | HIGH — surface per-kiosk MQTT health on the server dashboard | **1.1** (library; on the library's own roadmap) |
| **SqliteOfflineBuffer** | Implement `IOfflineBuffer` on the agent's existing `monitoring.db` so status/patch-progress survive a process restart during an outage (telemetry stays in-memory — latest-wins). Library already supports `UseOfflineBuffer<T>()`. | HIGH for banking zero-loss | **project-side**, after 1.0.3 |
| `WaitForConnectionAsync(timeout)` + `FlushAsync()` | Convenience: await connection instead of polling; force-drain buffer before shutdown. | MED — agent uses `OnConnected`, so minor | 1.1 |
| Subscribe/Unsubscribe timeout | Bound the SUBACK wait — the last remaining unbounded await (low risk: runs right after a successful connect). | MED — completeness | 1.0.4 |

**Deliberately skipped for this project (bloat):** MQTT v5 user-properties/reason-codes (3.1.1 is universal),
WebSocket transport (direct TCP), a max-reconnect-attempts cap (kiosks *want* infinite retry), faster
health-check interval (1 min is right at 500× scale).

---

*Companion to [ResilientMqtt-Research.md](ResilientMqtt-Research.md) (the 18-issue audit). 1.0.3 library code is
complete and builds+packs (0 errors); remaining work is runtime testing + publishing + consumer bump + deploy.*

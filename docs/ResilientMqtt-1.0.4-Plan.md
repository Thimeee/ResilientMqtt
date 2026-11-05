> 📄 **Copy** of the consuming project's `docs/ResilientMqtt-1.0.4-Plan.md` (Kiosk-Agent repository), kept here so the library's own history lives with the library. Copied 2026-10-05. Links to project files (agent / backend paths) only resolve in that repository; the project copy stays the one that is updated.

# ResilientMqtt 1.0.4 — Investigation & Plan

> **Status (2026-10-02): LIBRARY BUILT — released as `ResilientMqtt 1.1.0`** (user decision D3: a big update → minor
> version, not 1.0.4). Branch `v1.1.0` in the library repo (uncommitted — owner commits + pushes + publishes to nuget.org).
> 133 tests green (unit + in-process broker), verified again against the lab Mosquitto (AuthRejected, ACL refusals,
> persistent session, SessionTakenOver). `ResilientMqtt.1.1.0.nupkg` + `.snupkg` packed. Decisions taken: D1 = 1-day
> default session · D2 = scope as below · D4 = local package source for testing · D5 = branch created by Claude.
> **Project:** `CleanSession = true` on agent + server (P4). Project changes (bump + Step 2c) = next, separate approval.
> Extra findings while building: **#42** — disposing without `DisconnectAsync` dropped the graceful OFFLINE status.
> Second review (user: "are you sure nothing is left?"): **#43** offline unsubscribe never reached a resumed session;
> **#44** a resumed session's queue, delivered before `OnConnected` subscribed, was acknowledged and LOST (proven 0/3) —
> both side effects of #19 making sessions real; fixed (held + replayed in order) and tested; plus a concurrent-dispatch
> back-pressure limit (`Dispatch.MaxPending`).
> **2026-10-05 — honesty pass + two independent code reviews** (user: "is it true you found everything?" — it was not):
> TLS end-to-end tests (Hard / Ca / None / Normal / mutual TLS / plain port), the untested paths, and reviews of the
> 1.1.0 diff found **#45–#66** — notably held messages acknowledged too early (lost on a stop), a replay crossing
> connections, a full hold queue dropping messages, PUBACK order, a V311 zombie reset discarding the Will, an unknown
> `Tls.Mode` crashing startup, and several shutdown/set-up races. Hold/replay was redesigned (per-attempt epoch, manual
> ack in order, back-pressure). Fixes proven by running their tests against the pre-fix code. What is still NOT covered
> by a test is listed in the library README §10a "Test coverage" (incl. load — run the lab blip test).
> **Library:** `D:\Self Stady\importent\libry\.net\ResilientMqtt\latest\ResilientMqtt` · branch `v1.0.3` (clean, in sync
> with `origin/v1.0.3`) · MQTTnet 5.1.0.1559.
> **Why now:** v1.1.0 Step 2c (error-aware self-heal) needs the library to say *why* a connection failed. Before adding
> that, the user asked for a full investigation so 1.0.4 is a solid release, not a single patch.
> **Companion docs:** [ResilientMqtt-Research.md](ResilientMqtt-Research.md) (the 18-issue audit → 1.0.3) and
> [ResilientMqtt-1.0.3-Plan.md](ResilientMqtt-1.0.3-Plan.md). Finding numbers continue from #18.

---

## 0. How this investigation was done (2026-10-02)

The 1.0.3 investigation stopped at reading the code ("static surface exhausted — remaining risk is runtime-only").
This time the runtime was tested as well:

1. **Code:** every library file re-read (client, options, interface, DI, hosted service, breaker, backoff, health
   check, buffers, csproj, docs).
2. **Consumers:** how the agent and the server use it (`Program.cs`, `AppInitializationWorker`, `HealthProbeService`,
   `MqttSubscriptionWorker`, `MqttCommandPublisher`, appsettings).
3. **Live logs:** agent `C:\MonitoringAgent\App\Logs` (09-27 → 10-02) and server `C:\MonitoringOnpremServer\Backend\Logs`.
4. **Empirical probes:** throwaway console apps in the scratchpad, on MQTTnet 5.1.0.1559, run against the lab broker
   (192.168.1.190, ports 1883/8883). They used probe client ids and the probe topic `terminal/PROBE/...`, and cleaned
   up their sessions. They checked what MQTTnet *actually* reports for each failure, plus session, ACL and TLS behaviour.
5. **MQTTnet source:** the keep-alive loop and the connect-failure path (`MqttClient.cs`, master).

### What MQTTnet 5 actually reports — measured, not assumed

| Case | What `ConnectAsync` does | `DisconnectedAsync` also fires? |
|------|--------------------------|----------------------------------|
| Wrong password / unknown user / no credentials | **returns** `NotAuthorized` (no exception) | yes — `wasConnected=False`, `connectResult=NotAuthorized` |
| Port closed (broker service down) | throws `MqttCommunicationException` → `SocketException(ConnectionRefused)` after ~2 s | yes |
| DNS failure | `MqttCommunicationException` → `SocketException(HostNotFound)` | yes |
| Black-hole IP / no route | our timeout → `OperationCanceledException("MQTT connect canceled")` | yes |
| TLS: certificate rejected (Hard wrong thumbprint / OS validation of a self-signed cert) | `MqttCommunicationException` → `AuthenticationException` | yes |
| TLS client → plain port | `MqttCommunicationException` → `IOException("unexpected EOF…")` | yes |
| Non-MQTT endpoint | `MqttConnectingFailedException` → `MqttProtocolViolationException` | yes |
| SUBSCRIBE to a topic the ACL denies | **returns** item `NotAuthorized` — no exception, client stays connected | — |
| QoS1 PUBLISH to a topic the ACL denies | **returns** `IsSuccess=false, NotAuthorized` — no exception | — |
| QoS0 PUBLISH to a denied topic | `IsSuccess=true` (the protocol gives no feedback) | — |
| **Defaults** | `ProtocolVersion = V500`, `SessionExpiryInterval = 0`, `Timeout = 100 s` | — |
| **Keep-alive** | sends PINGREQ only when nothing was **sent** for `KeepAlivePeriod`; ping timeout = keep-alive / 2 | — |

---

## 1. New findings (#19 – #41)

| # | Sev | Finding | Evidence |
|---|-----|---------|----------|
| **19** | 🔴 | **The clients silently speak MQTT 5, so `CleanSession=false` does nothing.** The library never sets a protocol version or a session expiry, and MQTTnet 5 defaults to **MQTT 5.0 with `SessionExpiryInterval=0`**. Under MQTT 5 that means the broker **deletes the session as soon as the connection closes**. Nothing is queued for an offline client, and subscriptions are not kept. README says "uses v3.1.1 semantics" and "Combined with CleanSession:false → no missed patches even after long outages". **Both are false.** | Probe on the lab broker: current config → `sessionPresent=False`, publish while offline → `NoMatchingSubscribers`, 0 messages after reconnect. With `SessionExpiry=300` or with V311, the same test gives `sessionPresent=True` and the message is delivered. |
| **20** | 🟠 | **The failure reason never leaves the library** (needed by 2c). A refused sign-in is only logged. Network, TLS and DNS exceptions are only logged. A connect **timeout is not logged at all**. The disconnect `Reason`/`Exception` is only logged. | Code + the table above. |
| **21** | 🟠 | **`OnDisconnected` fires on *every failed connect attempt*, not once per drop.** MQTTnet raises `DisconnectedAsync` after a failed connect, with `ClientWasConnected=false`. The library treats each one as a real drop: it logs "Disconnected — Reason: UnspecifiedError", fires the user's `OnDisconnected`, flips `State` and starts a no-op reconnect. The interface doc says "fires when the connection drops (before reconnect attempts begin)". | Agent logs: 10-01 attempts #72→#89 and 09-27 #368→#382 each show "Disconnected" + agent "MQTT disconnected" every ~50 s for hours. MQTTnet source confirms. |
| **22** | 🟡 | **A connect timeout is invisible and leaves `State` stuck at `Connecting`.** `catch (OperationCanceledException) { return false; }` has no log and no state reset. | Code. Logs show silent gaps between `Reconnect #N` lines (= 15 s timeouts). |
| **23** | 🟡 | **Refused subscriptions are silent.** If the broker ACL refuses a SUBSCRIBE, the library still logs "Subscribed —", and `ResubscribeAllAsync` ignores the result. A kiosk with a wrong per-terminal ACL would **never receive commands**, and nothing anywhere says so. | Probe: `$SYS/#` → `NotAuthorized`, no exception. |
| **24** | 🟡 | **Refused QoS1 publishes count as success.** The library calls `RecordSuccess()` + `NotifyActivity()` on a `NotAuthorized` PUBACK. The message is lost and nothing is logged. | Probe: QoS1 to a denied topic → `IsSuccess=false`. |
| **25** | 🟡 | **A "zombie" connection is never detected on a client that publishes regularly.** MQTTnet pings only when nothing was *sent* for the keep-alive. The library's health check also counts *sent* publishes as activity. The agent sends a heartbeat every 30 s with keep-alive 40 s, so **neither layer ever pings**. If the broker→kiosk direction dies, commands stop arriving and nothing notices. Separately, when a ping fails while MQTTnet still says "connected", `onUnhealthy` → `TryReconnectAsync` does nothing (its loop needs `!IsConnected`). | MQTTnet source + library code. |
| **26** | 🟡 | **Lost wake-up when the reconnect loop exits.** A drop between "connected" and the latch release (or during `OnConnected`) finds the latch still held → no reconnect. Recovery then waits for the next health-check tick (≤ 60 s) or the next publish. | Code. |
| **27** | 🟡 | **Every reconnect sends every SUBSCRIBE twice.** The library re-subscribes automatically, and *both* consumers subscribe again in `OnConnected` (the library's own doc says you only need to add NEW ones). The broker re-sends retained messages for each SUBSCRIBE, so the server receives **every retained `status` twice per reconnect** (1,000 for 500 kiosks). | Agent log: "Resubscribing to 5 topic(s)" followed by 5 more "Subscribed —". |
| **28** | 🟢 | **The breaker's HalfOpen probe can leak.** When the probe caller exits without recording success or failure (disconnected path, caller-cancelled lock wait), `_probeInFlight` stays true. Every publish then reads "circuit OPEN" until the next reconnect — and if the client stays connected, until it restarts. | Code (side effect of the 1.0.3 #10 fix). |
| **29** | 🟢 | **The backlog buffered while the breaker was Open is drained only on a reconnect.** If the breaker closes without a reconnect, the backlog sits there. | Code. |
| **30** | 🟢 | **On a graceful stop, the OFFLINE status can be skipped by an Open breaker.** The clean DISCONNECT then suppresses the LWT, so the retained status stays "online" after a normal stop. `_client.DisconnectAsync` is also unbounded. | Code. |
| **31** | 🟢 | **Subscribe / Unsubscribe / Resubscribe are the last unbounded awaits.** Resubscribe runs under `_connectLock` + the reconnect latch. | Code (listed as a 1.0.4 backlog item in the 1.0.3 plan). |
| **32** | 🟢 | **Polish:** "Publish failed — buffering" is logged even when the buffer is off (the message is dropped). A caller-cancelled publish counts as a broker failure. `StateChanged` handler exceptions are swallowed with no log, and the handler runs inline on MQTTnet's thread. Validation does not check `OfflineBuffer.MaxSize > 0`, `KeepAliveSeconds >= 0` or `MaxDelay >= BaseDelay`. `OnConnected` is still awaited under the connect lock (document it). | Code. |
| **33** | 🟡 | **Docs that are not true:** README "v3.1.1 semantics" (twice), "no missed patches even after long outages", the `CleanSession` rows in README/USAGE/XML-doc, and the FAQ "persists QoS1+ across reconnects". | README:990, 1113, 1151, 1354, 1371; USAGE:184. |

**General-purpose gaps** — found by asking "would this library be the best choice for *any* system, not only ours?"
(user direction 2026-10-02: the library must be best-in-class on its own, even for features this project doesn't use):

| # | Sev | Gap | Why it matters to other systems |
|---|-----|-----|---------------------------------|
| **34** | 🟡 | **The raw MQTTnet `IMqttClient` is registered as a global DI singleton** (`services.TryAddSingleton(_ => factory.CreateMqttClient())`). | A host that also uses MQTTnet directly gets *our* client injected (or injects its own into ours). You can't run two ResilientMqtt clients side by side either. The client should be private to the library. |
| **35** | 🟡 | **Inbound handlers block the receive pump** (#8 from 1.0.3, safe in our project only because our handlers offload). | Any other user who writes a slow handler stops all inbound traffic **and** the keep-alive → the broker drops the client. A best-in-class library needs a safe dispatch option. |
| **36** | 🟢 | **Wrong credentials are retried at full speed forever** (backoff max 60 s). | A fleet with a rotated password hammers the broker's auth with no benefit. Common practice: a longer delay after an auth rejection. |
| **37** | 🟡 | **TLS is half-done:** only server-certificate checks (OS / accept-any / thumbprint pin). No **client certificate (mutual TLS)**, no "trust this CA file". | Many enterprise / cloud brokers (AWS IoT, Azure IoT Hub, bank PKI) *require* mutual TLS or a private CA. Without it the library can't be used there. |
| **38** | 🟡 | **Persistent session + random default `ClientId`** (`resilient-mqtt-client-{Guid}`). Once #19 makes persistent sessions real, every restart with the default id leaves an abandoned session (and its queue) on the broker. | Silent broker memory growth. Needs a warning and a sane expiry. |
| **39** | 🟢 | **No way to know whether a publish actually went out.** `PublishAsync` returns `Task` — sent, buffered, dropped (buffer off), refused, circuit open all look the same. | Command/request systems need the outcome (e.g. "reboot sent" vs "queued" vs "not sent"). |
| **40** | 🟢 | **No `WaitForConnectionAsync`** — callers poll `IsConnected` or hook `OnConnected`. | On the library's own ROADMAP; tiny and widely needed. |
| **41** | 🟢 | **Logs have no `EventId`s.** | Operators of other systems filter / alert on log event ids (Seq, ELK, Event Log). |

**Project-side findings** (not library bugs, found along the way):

| # | Finding | Action |
|---|---------|--------|
| P1 | Server `Mqtt:Tls:TrustedThumbprint` = `2D2F6C33…`, but the broker's 8883 certificate is **`F798460B…`**. TLS is disabled today, so nothing breaks yet — but enabling `Mode=Hard` as configured would fail. | Fix at TLS go-live (config). |
| P2 | A server restart issued T0001 **two beacon tokens 3 s apart** (16:12:37 and 16:12:40) — `TerminalStateChanged(online)` fired twice. At 500 kiosks that is 1,000 DB writes + publishes after every restart. | 2c: the issuer skips a terminal that got a token in the last 30 s. |
| P3 | `HealthProbeService` tracks connection state through events because "IResilientMqttClient exposes no IsConnected" (wrong — it does), and it inherits the #21 noise. | 2c: use the library's `ConnectionInfo`. |
| P4 | The earlier 2c argument "keep `CleanSession=false` so the broker queues a patch command" was **wrong**: per #19, nothing is queued today. **Decided (user, 2026-10-02): the project uses `CleanSession=true` (agent + server).** Our pipelines already recover on their own after a terminal comes back (patch reconnect-dispatch, resync of unacked statuses, transfer reconcile), and nothing stale (a reboot or delete) can run hours later. | 2c: set it explicitly in both `appsettings.json` + `ProtocolVersion=V500`. |

---

## 2. Design principles for 1.0.4

1. **General-purpose first.** The library is fixed and completed as a stand-alone product. Our project is one
   consumer; features we don't use (persistent sessions, mutual TLS, concurrent dispatch, publish results) are still
   done properly, because another system will need them.
2. **No breaking API.** Nothing public is removed or renamed. New interface members get **default implementations**,
   so other `IResilientMqttClient` implementations (fakes, decorators) still compile.
3. **Defaults keep today's behaviour,** except where today's behaviour contradicts the library's own docs (a bug).
   Every such case is listed in §4.
4. **Every claim is tested** against a real broker (in-process MQTTnet.Server in the test project) — the 1.0.3 lesson
   was that reading the code was not enough.

---

## 3. The work — phases

### Phase L-A — Failure visibility *(#20, #21, #22)*

- **`ResilientMqttFailureKind`** (enum): `None, AuthRejected, TlsFailure, BrokerUnavailable, Network, Timeout,
  SessionTakenOver, ProtocolError, Unknown`.
- **`ResilientMqttFailure`** (record): `Kind`, `Detail`, `ResultCode?`, `ExceptionType?`, `AtUtc`, `DuringConnect`.
- **`FailureClassifier`** (internal, pure), built from the table measured in §0:

| Input | Kind |
|-------|------|
| CONNACK `NotAuthorized` / `BadUserNameOrPassword` / `Banned` / `BadAuthenticationMethod`; disconnect `NotAuthorized` | **AuthRejected** |
| inner `AuthenticationException`; `IOException` EOF *during a TLS connect* | **TlsFailure** |
| `SocketException(ConnectionRefused)`; CONNACK `ServerUnavailable` / `ServerBusy` / `QuotaExceeded` / `ConnectionRateExceeded` / `UseAnotherServer` / `ServerMoved`; disconnect `ServerShuttingDown` / `AdministrativeAction` / `ServerBusy` | **BrokerUnavailable** |
| our connect timeout (not a shutdown); disconnect `KeepAliveTimeout`; `MqttCommunicationTimedOutException`; `SocketException(TimedOut)` | **Timeout** |
| `SocketException(HostNotFound / TryAgain / NetworkUnreachable / HostUnreachable / ConnectionReset / ConnectionAborted)`; other communication errors | **Network** |
| disconnect `SessionTakenOver` (another client used the same id — e.g. a cloned machine) | **SessionTakenOver** |
| `MqttProtocolViolationException`; other CONNACK codes (`ClientIdentifierNotValid`, `UnsupportedProtocolVersion`, …) | **ProtocolError** |

- **`LastFailure`** (set on every failed connect / unexpected drop, cleared on success).
- **`ConnectionInfo`** snapshot: `State`, `IsConnected`, `ConnectedSinceUtc`, `DownSinceUtc`, `ReconnectAttempt`,
  `TotalConnects` (flap counter), `LastFailure`, `BufferedCount`, `RejectedSubscriptions`, `DeniedPublishes`,
  `ProtocolVersion`, `SessionPresent`.
- **New events with details** (the old no-argument `OnConnected` / `OnDisconnected` stay):
  - `ConnectionLost(ResilientMqttConnectionLostArgs)` — `Failure`, `WasConnectedFor`. Once per real drop.
  - `ConnectionRestored(ResilientMqttConnectionRestoredArgs)` — `Downtime`, `Attempts`, `SessionPresent`, `LastFailure`.
- **#21:** a failed attempt (`ClientWasConnected=false`) only records the failure → `OnDisconnected` fires **once per
  real drop**, as its doc says.
- **#22:** a connect timeout logs a Warning and leaves the right state (`Reconnecting` in the loop, else `Disconnected`).
- One log line per failed attempt with its kind ("Reconnect #12 failed — AuthRejected (NotAuthorized)"); stack traces
  only for `Unknown`.

### Phase L-B — Protocol & session done right *(#19, #33, #38)*

- New options **`ProtocolVersion`** (`V500` default = today; `V311` allowed) and **`SessionExpirySeconds`** (nullable).
- **`CleanSession=false` finally means what the docs promise:** on MQTT 5, if `SessionExpirySeconds` is not set, the
  library uses a **finite default persistent session (see D1)** instead of MQTTnet's silent `0`. Setting
  `SessionExpirySeconds: 0` explicitly keeps the old behaviour. `CleanSession=true` → clean start, no session (our project).
- **#38:** a Warning when a persistent session is used with the auto-generated `ClientId` ("each restart leaves a
  session on the broker — set a stable ClientId").
- **Resubscribe without duplicate retained messages:** on MQTT 5 the library re-subscribes with
  `RetainHandling = SendAtSubscribeIfNewSubscriptionOnly`, so a reconnect with `SessionPresent=true` doesn't replay every
  retained message. (On 3.1.1 the idempotent-subscribe rule in L-C limits it to once.)
- One startup Information line with the effective mode ("MQTT 5.0 · clean start · no session" / "MQTT 5.0 · persistent
  session 1 day").
- Validation for both options. **All docs corrected** (README "v3.1.1 semantics", "no missed patches", the
  `CleanSession` rows, FAQ, XML docs).

### Phase L-C — Subscribe / publish truthfulness *(#23, #24, #27, #31, #39)*

- SUBSCRIBE results checked (also in resubscribe). A refused filter → Error log + `ConnectionInfo.RejectedSubscriptions`.
  No exception (compatible).
- **Idempotent subscribe (#27):** a filter + QoS already active *on this connection* → only the handler is updated; no
  second wire SUBSCRIBE. Tracked per connection, cleared on disconnect.
- QoS1/2 PUBLISH results checked: `NotAuthorized` etc. → Warning (rate-limited per topic) + `DeniedPublishes`. Not
  buffered (it would fail again) and not counted against the breaker (it's not a connectivity problem).
- **`PublishWithResultAsync(...)` (#39)** → `ResilientMqttPublishOutcome { Sent, Buffered, Dropped, Denied,
  CircuitOpen, Failed }`. The existing `PublishAsync` overloads stay and call it.
- Subscribe / unsubscribe / resubscribe bounded by `ConnectTimeoutSeconds` (#31).

### Phase L-D — Liveness *(#25, #26)*

- The health check counts **inbound** traffic as "alive" (received messages, QoS1+ acks, ping replies). A plain QoS0
  send no longer counts → a QoS0-only publisher gets one tiny ping every `IdleThresholdMinutes`.
- A ping failing **while MQTTnet still says connected** → bounded force-close → the normal drop → reconnect path runs,
  `LastFailure = Timeout ("no reply from broker")`.
- **#26:** after the reconnect loop releases its latch it re-checks `!IsConnected && !shuttingDown` and runs again.

### Phase L-E — Breaker & shutdown edges *(#28, #29, #30, #32)*

- HalfOpen probe released on **every** exit path.
- Breaker HalfOpen → Closed with a backlog → start the (single-flight) drain.
- Graceful OFFLINE status publish **bypasses the breaker**; `DisconnectAsync` bounded.
- #32 polish: honest "dropped" vs "buffered" logs; a caller cancel isn't a broker failure; `StateChanged` exceptions
  logged; validation for `MaxSize > 0`, `KeepAliveSeconds >= 0`, `MaxDelay >= BaseDelay`; XML doc that `OnConnected`
  runs under the connect lock.

### Phase L-F — General-purpose completeness *(#34 – #37, #40, #41)*

| # | Change | Default |
|---|--------|---------|
| 34 | The MQTTnet client is created **privately** by the library (no global `IMqttClient` in the container). | same behaviour, no DI leak |
| 35 | New `Dispatch` options: `Mode = Inline` (today) \| `Concurrent` (handlers run off the receive pump, `MaxConcurrency`, default 8). Handler exceptions isolated + logged in both modes; shutdown waits for in-flight handlers (bounded). | `Inline` |
| 36 | `Reconnect.AuthRejectedDelaySeconds` — after an `AuthRejected`, wait at least this long before the next try. | `0` = off (today) |
| 37 | TLS: `ClientCertificatePath` + `ClientCertificatePassword` (PFX) or `ClientCertificateThumbprint` (from the Windows store) for **mutual TLS**; `CaCertificatePath` (trust a private CA, full chain validation) as a new `Mode="Ca"`. Startup validation that the files exist / the cert has a private key. | off |
| 40 | `WaitForConnectionAsync(TimeSpan timeout, CancellationToken)` → `bool`. | — |
| 41 | Every log line gets a stable `EventId` (documented table in README). | — |

### Phase L-G — Tests *(new `ResilientMqtt.Tests`, xUnit)*

Test-only dependency: **MQTTnet.Server 5.1.0.1559** (in-process broker). Nothing new ships with the package.

| Area | Tests |
|------|-------|
| Classifier | every row of the §3 table, with the exact exception shapes measured in §0 |
| Real broker | wrong password → `AuthRejected`, `OnDisconnected` not fired per attempt (#21) · closed port → `BrokerUnavailable` · silent listener → `Timeout` in `ConnectTimeoutSeconds` + correct state (#22) · persistent session really queues, `0` really doesn't (#19) · random-id warning (#38) · ACL-refused subscribe → `RejectedSubscriptions` (#23) · refused QoS1 → `Denied` (#24, #39) · one SUBSCRIBE per filter per reconnect + no duplicate retained (#27) · zombie (broker stops answering) → forced reconnect (#25) · drop during `OnConnected` still reconnects (#26) · `ConnectionLost`/`ConnectionRestored` args · `Concurrent` dispatch doesn't block keep-alive (#35) · auth delay (#36) · mutual TLS + CA mode with test certs (#37) · `WaitForConnectionAsync` (#40) |
| Units | breaker probe release (#28) · drain after breaker close (#29) · graceful OFFLINE with breaker open (#30) · backoff · options validation · DI: two hosts' containers don't share a client (#34) |

### Phase L-H — Version, docs, package

- Library branch **`v1.0.4`** from `v1.0.3` (created by me, **not committed** — you commit).
- Version per D3. `CHANGELOG.md`; README + USAGE rewritten where wrong, new sections (failure kinds, `ConnectionInfo`,
  events, sessions, dispatch, mutual TLS, publish outcomes, EventId table); ROADMAP/FUTURE-FEATURES updated (items now
  done, items still deferred).
- `dotnet build -c Release` + pack → `.nupkg` + `.snupkg`. **Publishing to nuget.org is yours.**

---

## 4. Behaviour changes (everything else is additive / opt-in)

| Change | Who notices | Why it is right |
|--------|-------------|-----------------|
| `CleanSession=false` on MQTT 5 now really keeps a session (finite default, D1) (#19) | users who set `false` — **not our project** (we move to `true`) | it's what the option and the docs always promised; `SessionExpirySeconds: 0` restores the old effect |
| `OnDisconnected` once per real drop (#21) | log noise disappears | the documented contract |
| QoS0-only clients ping every ~2 min (#25) | e.g. our agent | a few bytes; it's the health check's whole purpose |
| Duplicate SUBSCRIBE / retained replay suppressed (#27) | server receives retained statuses once | the duplicate was accidental |
| Refused QoS1 publish = logged + `Denied`, not success (#24) | nobody normally | it was silent data loss |
| Graceful OFFLINE bypasses the breaker (#30) | nobody normally | fixes a stuck "online" after a normal stop |

---

## 5. Deferred to the next minor release (explicit, not forgotten)

Big features from the library's own FUTURE-FEATURES/ROADMAP that are **new products**, not fixes:
pluggable **durable offline buffers** (SQLite / Redis providers chosen from config) · **credential provider / hot
reload** (apply rotated credentials without a restart) · **multiple broker endpoints / failover** · **OpenTelemetry
metrics** · MQTT 5 extras (user properties, request/response, topic aliases) · **named multiple clients** in one DI
container.

---

## 6. After the library: Step 2c in the project (separate approval)

1. Agent + server → the new ResilientMqtt version; `CleanSession=true` + `ProtocolVersion=V500` set explicitly (P4).
2. Agent `HealthProbeService` uses `ConnectionInfo` / `ConnectionLost` instead of its own event tracking (P3).
3. **`SelfHealPolicy` error-aware:** restart only for `Network` / `Timeout` / `Unknown`; **never** for `AuthRejected` /
   `TlsFailure` / `BrokerUnavailable` / `ProtocolError` / `SessionTakenOver` — escalate only.
4. The beacon carries `failureKind` + `failureDetail` → plain-language "Live connection lost" detail per kind (e.g.
   "The kiosk's sign-in was refused — its connection settings need to be re-applied. (IT: MQTT credential
   rotated/revoked)", "Another device is using this terminal's identity. (IT: duplicate TerminalId / cloned kiosk)").
5. Server "Live data paused" alert: the IT hint names the cause from `LastFailure`.
6. P2 beacon-token de-dup. 7. Tests, living docs, deploy, lab tests (wrong password → no restart; firewall block →
   restart allowed; mosquitto stopped → "message service down"; same TerminalId on two machines → SessionTakenOver).

---

## 7. Decisions needed

| # | Decision | Options | Recommendation |
|---|----------|---------|----------------|
| **D1** | Default session length when `CleanSession=false` and `SessionExpirySeconds` is not set (MQTT 5) | (a) **1 day** (86 400 s) · (b) never expire (exact 3.1.1 parity; risks abandoned sessions forever) · (c) keep 0 + warning only (docs stay a promise the library doesn't keep) | **(a) 1 day** |
| **D2** | Scope | L-A … L-H as above, §5 deferred | **as above** |
| **D3** | Version number | (a) **1.0.4** as you said · (b) **1.1.0** — SemVer says *new public API = minor*; the library ROADMAP already calls these "v1.1" items | **(b) 1.1.0** (your call — 1.0.4 is fine too) |
| **D4** | Getting the package into the project before nuget.org | local folder package source on *your machine* only (not committed) for testing → you push to nuget.org before release | **yes** |
| **D5** | Library branch | I create `v1.0.4` / `v1.1.0` from `v1.0.3` without committing | **I create it** |

## 8. Order of work

L-A → L-B → L-C → L-D → L-E → L-F (each: build + its tests green) → L-G full suite → L-H docs/pack → **stop: your
review, commit, nuget push** → Step 2c plan approval → 2c build.

*Nothing in this document has been implemented. Updated 2026-10-02 after the user's direction "best library for any
system; project uses CleanSession=true".*

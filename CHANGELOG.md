# Changelog

All notable changes to **ResilientMqtt** are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/); this project uses [SemVer](https://semver.org/).

> **For AI agents / new developers:** this file is the fast answer to *"what was wrong before, and what
> changed?"* Each entry names the concrete defect, the fix, and the new option (if any). All 1.0.3 changes are
> **backward-compatible** — new options default to the previous safe behaviour, no public API was removed or
> renamed. A full engineering write-up of each item lives in the consuming project's
> `docs/ResilientMqtt-Research.md` (18-issue audit) and `docs/ResilientMqtt-1.0.3-Plan.md`.

---

## [1.1.0] — 2026-10-02 *(release candidates `1.1.0-rc.N` while the test campaign runs; final after it passes)*

Summary of this version: [docs/RELEASE-1.1.0.md](docs/RELEASE-1.1.0.md). Current state: [docs/STATUS.md](docs/STATUS.md).

A "tell me **why**, and be truthful" release, made after a full investigation that — unlike 1.0.3 — also **ran** the
library: probe apps against a real Mosquitto broker, the MQTTnet 5 source, and months of live fleet logs. It found
that several behaviours the docs promised did not happen at runtime. Every one is fixed, a test suite now proves it,
and the library gained the pieces a general-purpose client needs (failure reasons, mutual TLS, safe dispatch, publish
outcomes). **No public member was removed or renamed**; new interface members have default implementations, so other
`IResilientMqttClient` implementations (fakes, decorators) keep compiling. Findings are numbered #19–#71, continuing the
1.0.3 audit (full write-up: the consuming project's `docs/ResilientMqtt-1.0.4-Plan.md`). #45–#66 came from two
independent code reviews of the 1.1.0 diff itself, after the owner asked "are you sure nothing is left?".

### Fixed

- **(#19) `CleanSession = false` silently did nothing.** MQTTnet 5 speaks MQTT 5.0 by default with
  `SessionExpiryInterval = 0`, and the library never set either — so the broker deleted the session on every disconnect
  and **queued nothing** for an offline client, contrary to the docs ("no missed patches even after long outages").
  Proven on Mosquitto: `sessionPresent=False`, publish while offline → `NoMatchingSubscribers`. **Fix:** the protocol
  version and session expiry are now set explicitly; `CleanSession = false` gets a real session (default **1 day**,
  `SessionExpirySeconds` to change, `0` to keep the old effect). The startup log states the effective mode.
- **(#21) `OnDisconnected` fired on every failed reconnect attempt** (MQTTnet raises `DisconnectedAsync` after a failed
  connect too, with `ClientWasConnected = false`). Live logs showed it every ~50 s for hours. Now once per real drop.
- **(#22) A connect timeout was silent** (no log) and left `State = Connecting`. Now logged (event 1003) with a
  truthful state; the timeout is detected even when MQTTnet wraps the cancellation in another exception.
- **(#23) A subscription refused by the broker (ACL) was logged as "Subscribed"** — the client would never receive
  those messages and nothing said so. Now logged as an error (1102) and listed in `ConnectionInfo.RejectedSubscriptions`.
- **(#24) A QoS1/2 publish refused by the broker counted as a success** (breaker success, health activity, no log).
  Now `Denied`, logged (1202, rate-limited per topic), counted, not buffered, no breaker penalty.
- **(#25) "Zombie" connections were never detected on clients that publish regularly.** MQTTnet pings only when nothing
  was *sent*, and the health check counted *sent* publishes as activity — a client publishing QoS0 telemetry never
  verified the broker → client direction. And a failed ping while MQTTnet still said "connected" did nothing. Now only
  inbound traffic counts as alive, and an unanswered ping resets the connection (→ normal reconnect, `Timeout`).
- **(#26) Lost wake-up:** a drop during `OnConnected` / right after connecting found the reconnect latch taken and was
  not retried until the next health tick. The loop now re-checks after releasing its latch.
- **(#27) Every reconnect sent each SUBSCRIBE twice** when the app re-subscribed in `OnConnected` (the library restores
  subscriptions itself) — the broker then replayed every retained message twice. Subscribing to an already-active
  filter now only swaps the handler; on MQTT 5 restored subscriptions use retain-handling "new subscriptions only".
- **(#28) The circuit-breaker HalfOpen probe could leak** (probe holder left while disconnected / cancelled), blocking
  every publish until the next reconnect. The probe is released on every exit path.
- **(#29) The backlog buffered while the breaker was open waited for a reconnect.** It drains as soon as the breaker closes.
- **(#30) A graceful stop with the breaker open skipped the OFFLINE status**, then the clean DISCONNECT suppressed the
  Will — the retained status stayed "online". The graceful status now bypasses the breaker; `DisconnectAsync` is bounded.
- **(#31) Subscribe / unsubscribe / resubscribe had no timeout** (the last unbounded awaits; resubscribe runs under the
  connect lock). Bounded by `ConnectTimeoutSeconds`.
- **(#42) Disposing without `DisconnectAsync` dropped the graceful OFFLINE status** (the publish path saw "disposed"
  first). Found while writing the tests; resources are now marked disposed only after the disconnect.
- **(#44) Messages a resumed session delivered right after connecting could be lost.** With a persistent session the
  broker sends its queue immediately after the CONNACK — before an app that subscribes in `OnConnected` has done so;
  those messages matched no handler, were acknowledged and silently dropped (proven: 0 of 3 commands arrived with a
  realistic `OnConnected`). Messages that match no subscription during connection set-up are now held and replayed
  once `OnConnected` finishes, **in arrival order** (a live message arriving during the replay queues behind it).
  Found in a second review after the first 1.1.0 build — a side effect of #19 making sessions real.
- **(#43) Unsubscribing while offline left the subscription in the resumed broker session** (the UNSUBSCRIBE was never
  sent). It is now remembered and applied on the next connect when the session is resumed.
- **Review pass 2 + 3 (#45–#66)** — two independent code reviews of the 1.1.0 changes themselves:
  - **#53 Held messages were acknowledged the moment they were queued**, so a crash / failed set-up / overflow lost them
    for good. Now held QoS1/2 messages are acknowledged only after they reached a handler (manual ack), and anything set
    aside stays **unacknowledged → redelivered by the broker on the next session** (proven: the test fails on the old code).
  - **#63/#64 A replay from an older connection could consume the next session's messages, or acknowledge a packet id on
    the wrong connection.** The hold queue is now owned per connect attempt (epoch); a new attempt sets older items aside
    unacknowledged, and acks are sent only in their own epoch while connected.
  - **#65 A full hold queue dropped messages** (and unacknowledged drops could exhaust the broker's in-flight window). Now
    back-pressure: the next message waits for room; nothing is dropped. **#66 PUBACK order** is kept — once anything is
    held, everything after it queues behind it and is acknowledged in arrival order.
  - **#51 Held messages were replayed into an app that was shutting down** — no longer; they stay unacknowledged.
  - **#55 A `DisconnectAsync` during connection set-up** could be followed by the ONLINE status or a restarted health
    timer. The health timer is started/stopped under one lock with the shutdown check, and ONLINE is skipped once shutdown
    started. **#52** a saturated `Concurrent` dispatcher no longer blocks shutdown (the wait is cancelled).
  - **#54** a caller token cancelled during set-up no longer leaves a half-initialised connection; **#49** `ConnectAsync`
    returns `false` if the set-up was cut short; **#56** a drop during `OnConnected` can't leave a stale "ready" signal;
    **#46** `WaitForConnectionAsync` returns only when subscriptions are restored and `OnConnected` finished;
    **#45** state transitions are atomic; **#47/#50** a post-CONNACK or shutdown cancellation is not recorded as a
    connect timeout / broker failure.
  - **#57 MQTT 3.1.1 zombie reset discarded the Will** (a 3.1.1 DISCONNECT always does, and MQTTnet can't close without
    one) — the server kept showing the client online. The Will payload is now published by the client itself first.
  - **#48 TLS classification:** a reset / EOF while connecting with TLS is `TlsFailure` (brokers differ: Mosquitto closes,
    others reset); a bare "connection closed" with no reason is reported as `Unknown` with the likely causes (missing /
    rejected client certificate under TLS 1.3, refused 3.1.1 sign-in, connection limit, IP block) instead of guessing.
  - **#58 An unknown or empty `Tls.Mode` crashed startup** (1.0.3 fell back to OS validation) — it falls back again, with
    a warning; `ValidateOnStart()` still reports it. **#59** `CleanSession=true` + `SessionExpirySeconds>0` no longer logs
    "persistent session" (clean start discards it at every connect) and warns. **#60** the denied-topic log map is
    bounded. **#61/#62** simpler back-pressure (one semaphore, no polling) and one shared TLS-mode parser.
  - **#67** `DisconnectAsync` / dispose now abort an in-flight connect at once (it used to wait for `ConnectTimeout`
    when the caller passed no token).
  - **#68 (`rc.2`) Publish failures could flood the log:** while a link stalled, every failed publish wrote one ERROR
    with a stack trace (130 in a 4-minute soak). `PublishFailed` (1201) is now logged at most once a minute and says
    how many were skipped; new event **1205 `PublishFailuresNotLogged`** reports the rest once publishing works again.
    Only the log line is limited: buffering and outcomes are unchanged. To measure failures, use the outcomes, not a
    count of 1201 events.
  - **#69 (`rc.3`)** The same flood with the circuit open: `PublishBlockedCircuitOpen` (1204) wrote one WARNING per
    publish. Now one per minute with a count.
  - **#70 (`rc.3`)** The count of unlogged lines was lost if the client stopped before publishing recovered. It is now
    reported by `DisconnectAsync` / dispose. (A hard process exit or crash cannot report it.)
  - **#71 (`rc.3`)** During a long outage a full offline buffer wrote one WARNING per dropped message
    (`BufferOverflow`, 1303). Now one per minute with a count; new event **1304 `BufferDropsNotLogged`** reports the
    rest when the buffer starts draining.
- **(#32) Polish:** "Publish failed — buffering" was logged even with the buffer off (the message was dropped); a
  caller-cancelled publish counted as a broker failure; `StateChanged` exceptions were swallowed without a log;
  validation now covers `MaxSize`, `KeepAliveSeconds`, `MaxDelay >= BaseDelay` and every new option.
- **(#33) Docs that weren't true** — "v3.1.1 semantics", "no missed patches", the `CleanSession` rows and FAQ — corrected.

### Added

- **(#20) Failure reasons:** `ResilientMqttFailureKind` (`AuthRejected`, `TlsFailure`, `BrokerUnavailable`, `Network`,
  `Timeout`, `SessionTakenOver`, `ProtocolError`, `Unknown`), `ResilientMqttFailure` (kind, detail, code, exception
  type, time, during-connect), **`LastFailure`**, and a **`ConnectionInfo`** snapshot (state, since/down-since,
  attempts, flap counter, buffered, refused subscriptions, denied publishes, protocol, session present).
- **Events with details:** `ConnectionLost(Failure, WasConnectedFor)` and `ConnectionRestored(Downtime, Attempts,
  SessionPresent, LastFailure)`.
- **(#39) `PublishWithResultAsync` / `PublishBytesWithResultAsync`** → `Sent` / `Buffered` / `Dropped` / `Denied`.
- **(#40) `WaitForConnectionAsync(timeout)`.**
- **Options:** `ProtocolVersion` (`V500` default / `V311`), `SessionExpirySeconds`,
  `Reconnect.AuthRejectedDelaySeconds` (#36 — don't hammer the broker with wrong credentials), `Dispatch.Mode`
  `Inline` | `Concurrent` + `MaxConcurrency` + `ShutdownWaitSeconds` (#35 — a slow handler can no longer stall the
  receive loop and keep-alive; `Dispatch.MaxPending` bounds the backlog with back-pressure instead of unbounded
  memory), TLS `Mode = "Ca"` + `CaCertificatePath` + `CheckCertificateName` (private CA, full chain
  validation) and **mutual TLS** `ClientCertificatePath`/`Password` or `ClientCertificateThumbprint`/`Store` (#37).
- **(#41) Stable log `EventId`s** for every log line (incl. 1503 MessageUnmatched · 1504 HeldMessagesReplayed ·
  1505 DispatchBackPressure) (`ResilientMqttLogEvents`, table in README §10a).
- **(#38)** Warning when a persistent session is combined with the auto-generated (changing) `ClientId`.
- **Test campaign kit** (`tests/README-TESTING.md`, `tests/run-all.ps1`): network chaos via Toxiproxy (latency, black
  hole, reset, cut, slow link, fragmentation), cross-broker (Mosquitto / EMQX / HiveMQ), Mosquitto auth / ACL / TLS /
  mutual TLS / restart / 3.1.1 Will (`ResilientMqtt.ChaosTests`, 36 tests), a 500-client reconnect storm (network cut +
  broker restart) and a multi-hour soak with rotating faults and QoS1 gap / leak checks (`ResilientMqtt.LoadTest`).
- **Tests:** `tests/ResilientMqtt.Tests` — 146 unit + integration tests (in-process MQTTnet broker, real sockets:
  refused sign-in, closed port, silent broker, sessions, takeover, ACL refusals, retained messages, zombie detection
  through a freezable proxy, lost wake-up, dispatch modes, breaker, graceful status, TLS CA validation, resumed-session
  queue delivery + order, offline unsubscribe, TLS end to end incl. mutual TLS, back-pressure, shutdown races).
  What is NOT covered is listed in README §10a "Test coverage". Test-only
  dependency `MQTTnet.Server`; excluded from the library build and package.

### Changed

- **(#34) The raw MQTTnet `IMqttClient` is no longer registered in the host's DI container** — each ResilientMqtt
  client owns its MQTTnet client, so it can't collide with a host that uses MQTTnet directly.
- `ValidateOnStart()` reports **one clear message per problem** instead of a single generic message.
- Expected connection failures log **one line with the kind** (no stack trace); stack traces only for `Unknown`.
- TLS `Hard` thumbprint comparison ignores spaces, colons and the invisible mark Windows adds when copying.
- Health check: only inbound traffic counts as activity (see #25) — QoS0-only clients ping every `IdleThresholdMinutes`.

### Upgrade notes (1.0.x → 1.1.0)

No code changes needed. Check one setting: if you run `CleanSession = false` on MQTT 5, the broker now keeps your
session (and queues QoS1/2 messages) for 1 day while you are offline — that is what the option always promised. If you
relied on nothing being queued, set `CleanSession: true` (or `SessionExpirySeconds: 0`), and use a stable `ClientId`
with any persistent session.

---

## [1.0.3] — 2026-09-14

A resilience + correctness hardening release. Trigger: three live kiosks were stranded "disconnected" after a
server-side network blip and only recovered on a manual service restart. Root cause (#1) and 17 related
findings are fixed. **The unifying theme: every "await with no timeout while holding a lock/latch" is now
bounded, so a hang becomes a recoverable failure.**

### Fixed

- **(#1) Reconnect stall — the stranding bug.** `ConnectAsync` had no timeout on the MQTT CONNECT/CONNACK
  handshake (only the TCP socket connect was bounded). A half-open reconnect hung for 11–24+ minutes holding
  the single-flight `_isReconnecting` latch, so every health-check-driven reconnect no-op'd until the socket
  finally died or the process was restarted. **Fix:** the connect is now bounded by a linked-CTS timeout
  (`Reconnect.ConnectTimeoutSeconds`, default 15) — a stalled handshake fails fast and the loop retries.
- **(#7) Publish could hang holding `_publishLock`.** A QoS1/2 publish awaiting PUBACK on a half-open socket
  had no timeout; because every publish + the buffer drain share `_publishLock`, one stuck send froze all
  telemetry/status. **Fix:** every `_client.PublishAsync` is bounded by `PublishTimeoutSeconds` (default 15);
  a timeout is treated as a failed publish (buffered). Also covers the in-connect ONLINE publish and the
  graceful OFFLINE publish; a failed OFFLINE publish can no longer skip the clean DISCONNECT.
- **(#9) Health-check ping had no timeout.** A hung `PingAsync` left the run-latch set, silently disabling the
  monitor that triggers reconnect. **Fix:** ping is bounded by `HealthCheck.PingTimeoutSeconds` (default 10).
- **(#13) A failed/cold-start connect never scheduled a reconnect.** If the broker was down at boot, the
  auto-start connect failed and nothing retried (the app only recovered incidentally, via a later publish).
  **Fix:** a failed connect made outside the reconnect loop now starts the loop.
- **(#3) `_reconnectCts` was accessed from multiple threads without synchronization** (lost shutdown-cancel,
  possible `ObjectDisposedException`). **Fix:** all access is serialized via a lock; disposal is guarded.
- **(#16) Overlapping buffer drains could duplicate-send and drop messages.** Two quick reconnects could start
  two drains that both peeked the head (duplicate) then each dequeued (dropping the next). Dormant while the
  buffer is off, but active the moment it is enabled. **Fix:** the drain is now single-flight.
- **(#10) Circuit breaker admitted every publish the instant it went HalfOpen** (thundering-herd onto a
  just-recovered broker). **Fix:** HalfOpen now admits a single probe; the rest wait for it to resolve.
- **(#14) The health-check timer kept running after a graceful `DisconnectAsync`** (log noise). **Fix:**
  `DisconnectAsync` now stops the health check.
- **(#15) On reconnect, the health-check could skip its first tick** due to a stale run-latch. **Fix:** the
  latch is reset in `Start`.
- **(#17) Shutdown could throw `ObjectDisposedException`** when a background publish/drain/reconnect was still
  awaiting a semaphore being disposed. **Fix:** disposal is ordered and the exception is swallowed on shutdown.
- **(#18) Build was broken and documented trim-safety did not exist.** The csproj referenced `USAGE.md` at the
  root (it lives under `docs/`), so `dotnet build`/`pack` failed with MSB3030; and the README/USAGE promised an
  embedded `ILLink.Descriptors.xml` that was absent. **Fix:** csproj path corrected (packs cleanly now) and a
  real `ILLink.Descriptors.xml` is embedded, so config binding survives IL trimming as documented.

### Added

- **(#4) `StillDown` event + `Reconnect.StillDownAfterSeconds` (default 120).** Fires once per outage when the
  client has been unable to reconnect for the configured duration, so a host can alert on a stuck client.
  `0` disables it (no behaviour change). Fired on a background task; handlers should be quick.
- **`Reconnect.ConnectTimeoutSeconds` (15)**, **`PublishTimeoutSeconds` (15)**,
  **`HealthCheck.PingTimeoutSeconds` (10)** — see Fixed above.
- **`ValidateOnStart()` now validates the new timeouts** (`ConnectTimeoutSeconds`/`PublishTimeoutSeconds`/
  `PingTimeoutSeconds` must be > 0; `StillDownAfterSeconds` >= 0).

### Changed

- **(#11) TLS `Mode="Normal"` now logs a startup warning** that it accepts any certificate (MITM-open). The
  mode is unchanged and still fully supported for dev / internal LAN — production should use `Mode="Hard"`.
- **(#12) Correctness polish:** `State` is now `volatile`; inbound message handlers receive a real
  cancellation token (cancelled on dispose) instead of `CancellationToken.None`; added XML-doc for the
  "single-use after `DisconnectAsync`" contract and the "all matching subscriptions fire" dispatch rule.

### Verified (no change needed)

- **(#8) Long-running message handlers do not block the receive pump in either consumer** (the agent and the
  server both offload heavy work off the callback). The library's inline-await remains a foot-gun for other
  users — see `docs/FUTURE-FEATURES.md` for a possible opt-in non-blocking dispatch.

---

## [1.0.2] — prior

Baseline: auto-reconnect (exponential backoff + jitter), circuit breaker, in-memory offline buffer,
broker-side LWT, idle-aware health check, auto-resubscribe, DI + hosted-service integration. See `README.md`.

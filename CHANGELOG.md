# Changelog

All notable changes to **ResilientMqtt** are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/); this project uses [SemVer](https://semver.org/).

> **For AI agents / new developers:** this file is the fast answer to *"what was wrong before, and what
> changed?"* Each entry names the concrete defect, the fix, and the new option (if any). All 1.0.3 changes are
> **backward-compatible** — new options default to the previous safe behaviour, no public API was removed or
> renamed. A full engineering write-up of each item lives in the consuming project's
> `docs/ResilientMqtt-Research.md` (18-issue audit) and `docs/ResilientMqtt-1.0.3-Plan.md`.

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

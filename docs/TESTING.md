# ResilientMqtt — Testing guide

> **What this is:** the complete picture of how ResilientMqtt is tested — why the tests exist, what was built, what
> every test proves, how the test environment works, how to run it (on this PC or a separate test PC), how long it
> takes, how to read the results, and what is still not covered.
> **Version:** written for `1.1.0-rc.1` (2026-10-05). Paths are relative to the library root.
> **Quick runbook:** `tests/README-TESTING.md` · **Release history:** `CHANGELOG.md` · **Findings that drove the tests:**
> `docs/ResilientMqtt-Research.md` (1.0.3 audit), `docs/ResilientMqtt-1.0.4-Plan.md` (1.1.0 findings #19–#67).

---

## 1. Why there is a test campaign at all

| Version | How it was verified | What happened |
|---------|--------------------|---------------|
| 1.0.2 → 1.0.3 | Code reading only (an 18-issue audit) | Declared "complete" — yet running it later showed `CleanSession = false` did nothing, refusals were silent, `OnDisconnected` fired per retry… |
| 1.1.0 (investigation) | Probe apps against a **real** Mosquitto, the MQTTnet 5 source, live fleet logs | Findings #19–#42 |
| 1.1.0 (second look) | The owner asked "are you sure nothing is left?" — another review | #43, #44 (queued commands could be **lost**) |
| 1.1.0 (honesty pass) | TLS end-to-end tests + **two independent code reviews** of the diff | #45–#66 (held messages acked too early, replay across connections, PUBACK order, V311 Will, shutdown races…) |
| 1.1.0-rc.1 | Fault injection, then a **Docker test kit** (real brokers, network chaos, load, soak) | #67 + 6 bugs in the kit itself, found by a smoke run before the kit left this PC |

**Lesson:** reading code finds some bugs; *running* it under real failures finds the rest. So the library is
published only after the whole campaign below passes. While it runs, the package is versioned `1.1.0-rc.N`
(a version number already in a NuGet cache must never be re-used for different content).

---

## 2. The test layers

| # | Layer | Where | Needs | Runs in | Proves |
|---|-------|-------|-------|---------|--------|
| L1 | **Unit tests** | `tests/ResilientMqtt.Tests/Unit/` | nothing | `dotnet test` (seconds) | Pure logic: failure classifier, options/session math, validator, circuit breaker, TLS validation helpers |
| L2 | **Integration tests** | `tests/ResilientMqtt.Tests/Integration/` | nothing (an in-process MQTTnet broker on a random port) | `dotnet test` (~35 s) | The real client over real sockets: connect/refuse/timeout, sessions, refusals, retained, zombie detection, hold/replay, dispatch, breaker, graceful status, TLS + mutual TLS |
| L3 | **Fault injection** | `tests/ResilientMqtt.Tests/Integration/FaultInjectionTests.cs` | nothing | `dotnet test` | Rare paths forced deterministically by a decorator around the MQTTnet client (late CONNACK, wrapped cancel, cancel mid-set-up, full hold queue, cert-store mTLS) |
| L4 | **Chaos + cross-broker + TLS** | `tests/ResilientMqtt.ChaosTests/` | Docker | `run-all.ps1` | Real brokers (Mosquitto, EMQX, HiveMQ) + real network faults through Toxiproxy |
| L5 | **Reconnect storm** | `tests/ResilientMqtt.LoadTest` (`storm`) | Docker | `run-all.ps1` | Hundreds of clients survive a network cut and a broker restart |
| L6 | **Soak** | `tests/ResilientMqtt.LoadTest` (`soak`) | Docker | `run-all.ps1` | Hours of rotating faults: no QoS1 loss either way, no leaks |
| L7 | **Real kiosk lab tests** | consuming project (Kiosk-Agent, Step 2c) | lab kiosks | by hand | The agent + server end to end (wrong password → no restart, firewall block, broker stop, duplicate TerminalId) |

L1–L3 run on every `dotnet test tests/ResilientMqtt.Tests` (they are part of normal development). L4–L6 are the
**campaign**, run with one script.

---

## 3. What was built (files)

```
tests/
├── ResilientMqtt.Tests/                 L1–L3 (xUnit, test-only dependency MQTTnet.Server)
│   ├── Infrastructure/TestInfrastructure.cs   in-process TestBroker (refuse sign-in / subscribe / publish hooks,
│   │                                          TLS + mutual TLS endpoint), SilentListener, FreezableProxy (zombie),
│   │                                          FaultyMqttClient (fault-injection decorator), ListLogger, TestClients
│   ├── Infrastructure/TestPki.cs              throwaway CA / broker / client certificates
│   ├── Unit/*.cs                              classifier, options, validator, breaker, TLS
│   └── Integration/*.cs                       connection, messaging, sessions, hold/replay, TLS, review findings…
├── ResilientMqtt.ChaosTests/            L4 (skipped unless RMQTT_CHAOS=1)
│   ├── ChaosFixture.cs                         creates the Toxiproxy proxies, resets toxics per test
│   ├── NetworkChaosTests.cs                    7 faults × 3 brokers
│   ├── CrossBrokerTests.cs                     3 behaviours × 3 brokers
│   └── MosquittoTests.cs                       auth, ACL, TLS, mutual TLS, restart, MQTT 3.1.1 Will
├── ResilientMqtt.LoadTest/              L5 + L6 console harness
│   ├── Program.cs                              commands: certs · storm · soak
│   ├── StormScenario.cs  ·  SoakScenario.cs  ·  Support.cs (counting logger, bit set, stats, cert generator, report)
├── Shared/ChaosEnvironment.cs           broker targets, ports, Toxiproxy API client, client factory (shared by L4–L6)
├── chaos/
│   ├── docker-compose.yml                      Mosquitto + Toxiproxy (+ EMQX + HiveMQ with --profile full)
│   └── mosquitto/mosquitto.conf, acl           broker config used by the tests
├── run-all.ps1                          runs the campaign, writes results + zip
└── README-TESTING.md                    short runbook
```

The test projects are excluded from the library build and from the NuGet package (`ResilientMqtt.csproj`:
`<Compile Remove="tests\**" />`).

---

## 4. Test catalogue — what each test proves

Numbers like **#44** refer to findings in `docs/ResilientMqtt-1.0.4-Plan.md` / `CHANGELOG.md`.

### L1 — Unit (`Unit/`, 45 test methods)

| Group | Tests | Proves |
|-------|-------|--------|
| **FailureClassifier** (22) | `Refused_sign_in_is_AuthRejected`, `Broker_not_accepting_is_BrokerUnavailable`, `Other_connack_codes_are_ProtocolError`, `Port_closed_is_BrokerUnavailable`, `Socket_network_errors_are_Network`, `Socket_timeout_is_Timeout`, `Rejected_certificate_is_TlsFailure`, `Eof_during_a_tls_connect_is_TlsFailure_but_plain_eof_is_Network`, `A_reset_while_connecting_with_tls_is_TlsFailure_without_tls_it_is_Network`, `A_bare_connection_closed_while_connecting_is_Unknown_with_the_likely_causes`, `A_refused_port_stays_BrokerUnavailable_even_with_tls`, `Non_mqtt_endpoint_is_ProtocolError`, `Broker_disconnect_reasons_map`, `Unspecified_disconnect_uses_the_exception`, … | Every way MQTTnet 5 reports a failure (shapes **measured** against a real broker) maps to the right `ResilientMqttFailureKind` (#20, #48) |
| **Session & protocol options** (5) | `CleanSession_false_without_expiry_gets_the_one_day_default`, `An_explicit_expiry_always_wins`, `Protocol_version_parses`, … | `CleanSession=false` really becomes a session (#19) |
| **Validator** (5) | `Each_problem_gets_its_own_message`, `Tls_modes_are_checked`, `ValidateOnStart_surfaces_the_specific_message`, `The_mqttnet_client_is_not_leaked_into_the_container` | Clear configuration errors; no DI leak (#34) |
| **Circuit breaker** (5) | `Half_open_admits_exactly_one_probe`, `An_abandoned_probe_frees_the_slot`, `Success_reports_when_it_closes_the_circuit`, … | #10, #28, #29 |
| **TLS helpers** (8) | `A_certificate_issued_by_the_trusted_ca_is_accepted`, `A_name_mismatch_is_rejected_unless_name_checking_is_off`, `Thumbprints_are_normalised`, `Unknown_tls_mode_does_not_crash_startup`, … | CA validation, thumbprint normalisation, 1.0.3-compatible TLS mode fallback (#37, #58) |

### L2 — Integration against an in-process broker (`Integration/`)

| File | Tests | Proves |
|------|-------|--------|
| **ConnectionTests** (15) | healthy `ConnectionInfo`; session requested correctly (V5 / V311 / clean); persistent vs clean session queueing; random-ClientId warning; wrong password → `AuthRejected` **without `OnDisconnected` per retry**; auth back-off; closed port; silent broker → `Timeout` + truthful state; cold start + `WaitForConnectionAsync`; kick → one `ConnectionLost` + `ConnectionRestored`; `SessionTakenOver`; drop during `OnConnected` still reconnects; **zombie detected while publishing QoS0** | #13, #19–#22, #25, #26, #36, #38, #40 |
| **MessagingTests** (10) | refused subscription reported; publish outcomes Sent/Denied; Dropped vs Buffered + drain; one SUBSCRIBE per filter per connection; no retained replay on a resumed session; Concurrent vs Inline dispatch; graceful OFFLINE with breaker open; dispose without disconnect still sends it; backlog drained when the breaker closes | #23, #24, #27, #29, #30, #35, #39, #42 |
| **PersistentSessionTests** (3) | queued messages reach handlers registered in `OnConnected`; a live message never overtakes the queued ones; offline unsubscribe applied to the resumed session | #43, #44 |
| **HoldReplayTests** (5) | held + restored subscriptions keep arrival order; reconnect during a slow replay loses nothing; shutdown during set-up never replays into the stopping app and keeps the messages; bounded dispose with saturated handlers; `ConnectAsync` false when the set-up was cut short | #49, #51, #52, #63–#66 |
| **ReviewFindingTests** (7) | held messages survive a stop before `OnConnected` finishes; replayed messages are acknowledged (no redelivery); disconnect during set-up ends with the graceful status and no health timer; no stale "ready"; V311 zombie reset publishes the Will; unknown TLS mode falls back with a warning; CleanSession=true + expiry warns | #53, #55–#59 |
| **CoverageGapTests** (5) | Concurrent back-pressure loses nothing; dispose waits for handlers; V311 zombie reset; V311 persistent session; `WaitForConnectionAsync` waits for `OnConnected` | #35, #46, #57 |
| **TlsTests** (8) | **real TLS endpoint**: `Hard` right/wrong thumbprint; `Ca` right/wrong CA; name check on/off; `None` rejects a private CA; `Normal` + warning; **mutual TLS** with / without a client certificate; wrong PFX password fails fast; TLS against a plain listener → `TlsFailure` | #11, #37, #48 |

### L3 — Fault injection (`FaultInjectionTests`, 5)

| Test | Forced situation | Proves |
|------|-----------------|--------|
| `A_connack_that_lands_right_at_the_timeout_boundary_counts_as_connected` | CONNACK arrives, the code resumes after the timer fired | not misreported as a timeout (#47) |
| `A_shutdown_cancel_wrapped_by_mqttnet_is_not_recorded_as_a_failure` | connect hangs; dispose; MQTTnet wraps the cancel | no failure recorded, shutdown not delayed (#50, #67) |
| `A_caller_token_cancelled_during_set_up_does_not_leave_a_half_initialised_connection` | slow SUBACK + caller token fires mid-set-up | `OnConnected` still runs, client ready (#54) |
| `A_full_hold_queue_applies_back_pressure_and_drops_nothing` | 8 queued messages, hold limit 3 | all 8 delivered in order (#65) |
| `Mutual_tls_with_a_client_certificate_from_the_certificate_store` | client cert in CurrentUser\My (removed afterwards) | thumbprint-from-store mutual TLS (#37) |

Several fixes were also proven the other way round: their test was run against a copy of the **pre-fix** code and
**failed** there (#44 order: `c1,c4,c2,c3`; #53: 0 of 3 commands; #57: Will never published).

### L4 — Chaos, cross-broker and TLS on real brokers (`ResilientMqtt.ChaosTests`, 36)

**Network chaos** (`NetworkChaosTests`, 7 tests × Mosquitto / EMQX / HiveMQ), faults injected by Toxiproxy between the
client and the broker:

| Test | Fault | Pass when |
|------|-------|-----------|
| `Latency_within_the_timeouts_still_works` | +1.5 s latency | connects and publishes |
| `Latency_beyond_the_connect_timeout_is_Timeout_and_recovers_when_it_clears` | +5 s latency, 2 s connect timeout | `Timeout`, then connects once the latency is removed |
| `A_black_hole_on_a_live_connection_is_detected_and_recovers` | broker → client data dropped, socket kept open | `ConnectionLost` with `Timeout`, recovers |
| `A_connection_reset_is_Network_and_recovers` | TCP reset | `Network`, recovers |
| `Broker_unreachable_is_BrokerUnavailable_then_restored` | proxy disabled | a broker/network-side kind (never auth/TLS), `ConnectionRestored` |
| `A_slow_link_never_hangs_a_publish_beyond_its_timeout` | 4 KB/s, 200 KB publish | bounded by `PublishTimeoutSeconds`, client still usable |
| `Fragmented_packets_arrive_intact_and_in_order` | packets sliced to ~7 bytes | 50 messages intact and in order |

**Cross-broker** (`CrossBrokerTests`, 3 × 3 brokers): persistent session queueing · duplicate ClientId →
`SessionTakenOver` · no retained replay on a resumed session.

**Mosquitto (production broker)** (`MosquittoTests`, 6): wrong password → `AuthRejected` · ACL (publish → `Denied`;
subscribe behaviour recorded — see §8) · TLS `Ca` / `Hard` right + wrong / `None` / TLS on the plain port · mutual TLS
with and without a client certificate · broker container restart → session resumed · MQTT 3.1.1 zombie reset → the
server receives the Will.

### L5 — Reconnect storm (`LoadTest storm`)

| Phase | What happens |
|-------|--------------|
| 1 | N clients (default **500**, kiosk-like: keep-alive 40 s, clean session, real jittered back-off 1–60 s) connect in batches of 50 |
| 2 | 15 s of QoS0 telemetry (like the agent heartbeat) |
| 3 | **Outage** for `-OutageSeconds` (default 30): `proxy` mode = network cut via Toxiproxy; `restart` mode = Mosquitto container stopped and started |
| 4 | Wait until every client is back (up to 5 min) |
| 5 | A "server" client sends one QoS1 command to every client — proves subscriptions were restored |

**PASS** = all dropped, **stuck = 0**, slowest reconnect ≤ 180 s, **missing commands = 0**. Report:
`storm-proxy.md/.json`, `storm-restart.md/.json` (p50 / p95 / p99 / max reconnect time, broker CPU/memory, process
memory/threads, library log-event counts, last warnings).

### L6 — Soak (`LoadTest soak`)

| Element | Setting |
|---------|---------|
| Clients | default **100**; persistent sessions; offline buffer on (2000); half subscribe in `OnConnected` (exercises #44) |
| Kiosk → server | every client publishes a QoS1 **sequence number** every 5 s; a verifier records every number in a bit set |
| Server → kiosk | every minute the verifier sends a numbered QoS1 **command** to every client |
| Faults | one at a time, every `-FaultEveryMinutes` (default 10), rotating: latency 3 s ×60 s · black hole ×90 s · connection reset ×5 s · network cut ×60 s · slow link 2 KB/s ×60 s · **broker restart** |
| Sampling | every minute → `soak-metrics.csv` (connected, working set, GC heap, threads, handles, totals, last fault) |
| End | faults stop, everyone reconnects, 2 min to drain, then the verdict |

**PASS** = every client connected at the end, **0 sequence gaps**, **0 command gaps**, 0 dropped/denied, working set and
thread count growth ≤ 25 % (first 10 % vs last 10 % of samples after a warm-up). Duplicates are reported, not failed
(QoS1 is at-least-once).

---

## 5. How the test environment works

`tests/chaos/docker-compose.yml` (project name `rmqtt-test`; all host ports 188xx so nothing clashes with a broker on 1883):

| Service | Image | Host port(s) | Role |
|---------|-------|-------------|------|
| `mosquitto` | `eclipse-mosquitto:2` | 18831 plain · 18832 TLS · 18833 mutual TLS | production broker; user `test`/`test`; ACL allows `chaos/#`, `load/#`, `status/#` only |
| `toxiproxy` | `ghcr.io/shopify/toxiproxy:2.9.0` | 18474 API · 18901 → mosquitto · 18902 → emqx · 18903 → hivemq | injects network faults |
| `emqx` | `emqx/emqx` | 18841 | cross-broker (profile `full`) |
| `hivemq` | `hivemq/hivemq-ce` | 18851 | cross-broker (profile `full`) |

- **Certificates:** `LoadTest certs` generates a throwaway CA, a broker certificate (`localhost`, `mosquitto`,
  127.0.0.1) and a client PFX into `tests/chaos/certs/` (git-ignored) at every run.
- **Mosquitto start-up:** files bind-mounted from Windows are not readable by the container's `mosquitto` user, so the
  entrypoint copies config + ACL + certificates into `/m`, creates the password file there and hands it to that user.
  (Restart-safe: it does not use `mosquitto_passwd -c`, which refuses an existing file on Mosquitto 2.1.)
- **Faults:** the tests call the Toxiproxy HTTP API (latency, `timeout` = black hole, `reset_peer`, `bandwidth`,
  `slicer`, proxy disable) and `docker restart/stop/start` for broker faults.
- **Clients in L4–L6** are built through the library's internal constructor (`InternalsVisibleTo`) so a test can use a
  fast health check / retry where the public options only allow minutes.

---

## 6. How to run

### On the development PC (L1–L3)

```powershell
dotnet test tests/ResilientMqtt.Tests        # ~35 s, no Docker needed
```

### The campaign (L1–L6) — on a separate test PC

Requirements: Windows 10/11, **.NET 10 SDK**, **Docker Desktop running**, 4+ cores / 8+ GB RAM, internet for the
first run (Docker images + NuGet). The script keeps the PC awake while it runs.

1. Copy the library folder (or the `ResilientMqtt-<version>-testkit.zip`) to the test PC and extract it.
2. Open **Windows PowerShell**, then:

```powershell
cd <folder>\ResilientMqtt\tests
powershell -ExecutionPolicy Bypass -File .\run-all.ps1 -Smoke          # ~15–20 min sanity check — always first
```

3. When the smoke run passes, choose how long the soak should be — **the soak duration is a parameter, not fixed**:

| Goal | Command | Approx. time |
|------|---------|-------------|
| Everything, with a **5 h** soak (recommended first full run) | `.\run-all.ps1 -SoakHours 5` | ~6 h |
| Everything, with the full **24 h** soak | `.\run-all.ps1` | ~25 h |
| **Soak only**, any length (e.g. 5 h, later 24 or 48 h) | `.\run-all.ps1 -SoakOnly -SoakHours 5` | 5 h + ~10 min |
| Soak in minutes (quick experiment) | `.\run-all.ps1 -SoakOnly -SoakMinutes 30` | ~40 min |
| Everything except the soak | `.\run-all.ps1 -SkipSoak` | ~1 h |
| Mosquitto only (skip EMQX/HiveMQ) | add `-SkipFull` | |
| Bigger / smaller storm | `-Clients 1000` · `-OutageSeconds 60` | |
| Faults more / less often in the soak | `-FaultEveryMinutes 5` | |

A suggested progression: **Smoke → `-SoakHours 5` → `-SoakOnly -SoakHours 24`** (and longer later if wanted). When the
soak starts, the script prints the expected finish time.

All options: `-Clients` · `-OutageSeconds` · `-SoakHours` · `-SoakMinutes` · `-SoakClients` · `-FaultEveryMinutes` ·
`-Smoke` · `-SoakOnly` · `-SkipFull` · `-SkipUnit` · `-SkipChaos` · `-SkipStorm` · `-SkipSoak` · `-KeepEnvironment`.

---

## 7. Reading the results

Everything lands in `tests\test-results\<timestamp>\` and a **`<timestamp>.zip`** next to it — bring the zip back.

| File | Content |
|------|---------|
| `SUMMARY.md` | one line per stage: PASS / FAIL, minutes, machine |
| `unit.trx` · `chaos.trx` | test results (open in Visual Studio, or any TRX viewer); chaos tests log the measured failure kinds |
| `storm-proxy.md` · `storm-restart.md` (+ `.json`) | reconnect percentiles, stuck clients (with their last failure), missing commands, broker + process stats |
| `soak.md` (+ `.json`) | gaps both ways, duplicates, dropped, memory/thread growth, faults injected, log-event counts |
| `soak-metrics.csv` | one row per minute — open in Excel to see memory / connected clients over time |
| `rmqtt-*.log` · `run-all.log` | broker / proxy container logs and the whole console output |

**What to look at first:** `SUMMARY.md`; then for any FAIL its report. A storm FAIL lists the stuck clients and *why*
(their `LastFailure`); a soak FAIL lists which clients have gaps and which sequence numbers / commands are missing;
memory growth shows in `soak-metrics.csv`.

---

## 8. Known limits and broker behaviours discovered

| Item | Detail |
|------|--------|
| **Mosquitto `acl_file` grants refused subscriptions** | measured: SUBACK `GrantedQoS0`, delivery silently filtered — no client can detect it. Mosquitto's **dynamic-security plugin** (used in production), EMQX and HiveMQ refuse in the SUBACK, which the library reports. Publish refusals are reported either way. |
| **Docker Desktop hides "connection refused"** | its port forwarder accepts the TCP connection and then closes it, so a stopped listener behind Docker looks like a close, not a refusal; the chaos test accepts any broker/network-side kind there. A truly refused port → `BrokerUnavailable` is proven by the unit tests and against a real Mosquitto. |
| **QoS 0 refusals** | invisible by protocol (no acknowledgement). |
| **A publish that times out mid-send resets the connection** | measured on the test PC (slow-link chaos test): MQTTnet cannot abandon a half-written packet, so it closes the connection; the library treats it as a normal drop (`ConnectionLost`) and reconnects (~0.3 s there). Very large payloads on very slow links therefore cost a reconnect — size `PublishTimeoutSeconds` for the biggest message and the slowest link. |
| **At-least-once** | QoS1 duplicates around reconnects are normal; handlers must be idempotent. |
| **Not covered anywhere yet** | the denied-topic log-map cap (#60, trivial); WebSocket transport (not supported). |
| **First run of EMQX / HiveMQ** | the kit's smoke run on the development PC used Mosquitto only; the cross-broker tests first run on the test PC — a broker-specific difference shows up as a failing test with the measured kind in its output, and is then either fixed in the library or documented here. |

---

## 9. Workflow until 1.1.0 is published

```
rc.1 kit ──► test PC: -Smoke ──► results zip ──► review here
                                   │ FAIL → fix → rc.2 → repeat
                                   ▼ PASS
            test PC: -SoakHours 5 (full) ──► review ──► -SoakOnly -SoakHours 24 ──► review
                                   ▼ all PASS
            Version 1.1.0 (final) → pack → owner commits + pushes branch v1.1.0 → nuget.org publish
            → the consuming project switches from 1.1.0-rc.N to 1.1.0 and commits its Step 2c
```

Each fix found by the campaign gets a test in L1–L3 where possible, so it can never come back silently.

---

## 10. Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Docker is not running` | start Docker Desktop, wait for "Engine running" |
| `port 188xx did not open` | `docker compose -p rmqtt-test -f tests\chaos\docker-compose.yml logs` |
| Leftover containers after an aborted run | `docker compose -p rmqtt-test -f tests\chaos\docker-compose.yml --profile full down -v` |
| A 188xx port already in use | change it in `tests/chaos/docker-compose.yml` **and** `tests/Shared/ChaosEnvironment.cs` |
| Script blocked by policy | run it as shown: `powershell -ExecutionPolicy Bypass -File .\run-all.ps1` |

---

## 11. History of this kit

| Date | Change |
|------|--------|
| 2026-10-05 | **Smoke on the test PC** (16 cores / 16 GB): all stages PASS — unit 138/138 · chaos (Mosquitto) 16/16 · storm proxy max 22.7 s · storm restart max 7.0 s · soak 0 gaps, memory +8.8 %. Observation documented in §8: a timed-out publish resets the connection (by design of MQTTnet). Next: full run with `-SoakHours 5` (first run of EMQX / HiveMQ). |
| 2026-10-05 | Kit created (rc.1). Smoke run on the development PC (Mosquitto only) found and fixed 6 kit bugs before hand-over: Mosquitto password-file permissions, Mosquitto crash on restart (`mosquitto_passwd -c` on 2.1), Docker forwarder hiding "refused", soak publishing to the wrong topic, fault rotation counted at the end, overlapping faults colliding in Toxiproxy. Smoke result: unit PASS · chaos 16/16 · storm (proxy) PASS max 12.9 s · storm (restart) PASS max 7.1 s · soak 4 min PASS, 0 gaps. `-SoakOnly` + printed soak finish time added. |

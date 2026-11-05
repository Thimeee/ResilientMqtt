# ResilientMqtt — test campaign

Everything needed to prove the library on a separate test PC: unit + integration tests, network chaos, cross-broker
and TLS tests against real brokers, a reconnect storm with hundreds of clients, and a long soak with injected faults.

> **Full guide** (why, every test, how the environment works, reading results, limits): [`docs/TESTING.md`](../docs/TESTING.md).
> Soak length is a parameter: `-SoakHours 5`, `-SoakMinutes 30`, or soak only: `-SoakOnly -SoakHours 24`.

| Folder | What |
|--------|------|
| `ResilientMqtt.Tests/` | Unit + integration tests against an in-process MQTTnet broker (also run by a plain `dotnet test`). |
| `ResilientMqtt.ChaosTests/` | Network chaos (Toxiproxy), cross-broker (Mosquitto / EMQX / HiveMQ), Mosquitto auth / ACL / TLS / mutual TLS / restart / MQTT 3.1.1 Will. **Skipped** unless `RMQTT_CHAOS=1` (set by `run-all.ps1`). |
| `ResilientMqtt.LoadTest/` | `storm` (reconnect storm) and `soak` (hours with rotating faults) harness + the test-PKI generator. |
| `chaos/` | `docker-compose.yml` + Mosquitto config. All host ports are 188xx — no clash with a broker on 1883. |
| `Shared/` | Toxiproxy client + environment settings used by both projects. |
| `run-all.ps1` | Runs the whole campaign and zips the results. |

## Requirements (test PC)

- Windows 10/11, **.NET 10 SDK**, **Docker Desktop** (running). 4+ cores, 8+ GB RAM recommended.
- Internet access for the first run (Docker images: `eclipse-mosquitto:2`, `ghcr.io/shopify/toxiproxy:2.9.0`,
  `emqx/emqx`, `hivemq/hivemq-ce`; NuGet packages).
- The PC must not sleep during the soak (`run-all.ps1` keeps it awake while it runs; also plug in a laptop).

## Run

```powershell
cd <library folder>\tests
powershell -ExecutionPolicy Bypass -File .\run-all.ps1 -Smoke      # ~15 min sanity check first
powershell -ExecutionPolicy Bypass -File .\run-all.ps1             # full campaign (24 h soak)
```

Options: `-SoakHours 8` · `-SoakMinutes 30` · `-Clients 300` · `-SkipFull` (Mosquitto only) · `-SkipSoak` ·
`-SkipStorm` · `-SkipChaos` · `-SkipUnit` · `-KeepEnvironment` (leave containers running).

Results: `tests\test-results\<timestamp>\` and **`<timestamp>.zip`** (bring this back):
`SUMMARY.md` (one line per stage), `unit.trx`, `chaos.trx`, `storm-proxy.md/.json`, `storm-restart.md/.json`,
`soak.md/.json`, `soak-metrics.csv` (per-minute connected / memory / threads), broker logs, `run-all.log`.

## Stages and pass criteria

| Stage | Pass when |
|-------|-----------|
| Unit + integration | all tests pass |
| Chaos + cross-broker + TLS | all tests pass on every broker in the run (a broker-specific difference shows up as a failing test with the measured kind in its output) |
| Storm — network cut | every client drops, **every client reconnects** (stuck = 0), slowest reconnect ≤ 180 s, **every client receives a command afterwards** |
| Storm — broker restart | same, with the Mosquitto container stopped and started |
| Soak | **no QoS1 gap** in either direction (kiosk → server sequence numbers and server → kiosk commands), every client connected at the end, nothing dropped, working set and thread count growth ≤ 25 % after warm-up |

Duplicates are reported but allowed (QoS1 is at-least-once).

## Faults injected

Toxiproxy between the clients and the broker: latency, black hole (broker → client data dropped, socket kept open),
connection reset, network cut (proxy disabled), slow link (bandwidth), fragmented packets (slicer); plus broker
container restart / stop + start via Docker.

## Troubleshooting

- `Docker is not running` → start Docker Desktop, wait until it says "running".
- `port ... did not open` → `docker compose -p rmqtt-test -f chaos\docker-compose.yml logs`.
- A port in 188xx already used → change it in `chaos/docker-compose.yml` **and** `Shared/ChaosEnvironment.cs`.
- Leftover containers after an aborted run → `docker compose -p rmqtt-test -f chaos\docker-compose.yml --profile full down -v`.

# ResilientMqtt Status: where the library stands

> **Read this first when you come back to the library.** Keep it current: update it after every library change,
> test run or release. **Last updated: 2026-11-06.**
> What changed and why: [CHANGELOG](../CHANGELOG.md). How to use it: [USAGE](USAGE.md). How it is tested: [TESTING](TESTING.md).

---

## Now

| Item | State |
|------|-------|
| **Version** | `1.1.0-rc.3`, a release candidate. Final `1.1.0` is not released. |
| **Git** | Branch `v1.1.0`, tag `v1.1.0-rc.3` = commit `763ddb3`, pushed. One branch per version. |
| **nuget.org** | `1.1.0-rc.3` published as a **prerelease** (2026-11-06). Latest stable is still `1.0.3`. |
| **`rc.2`** | Only a git tag (`4fd0dba`, with #68 + #60). Never published. |
| **Platform** | .NET 10, MQTTnet 5.1.0.1559. Vulnerability scan clean (`dotnet list package --vulnerable --include-transitive`, 2026-11-05). |
| **Used by** | Kiosk Agent + Monitoring Server (`Kiosk-Agent-` repository): still on `1.0.3`, moving to the rc in its Step 2c. That project uses `CleanSession=true`. |

---

## Test state

| Layer | State | Last run |
|-------|-------|----------|
| L1–L3 unit, integration, fault injection | ✅ 146/146 | 2026-11-05, `rc.3` |
| L4 chaos + TLS: Mosquitto, EMQX, HiveMQ | ✅ 36/36 (first EMQX + HiveMQ run) | 2026-11-05, `rc.3` |
| L5 storm: 500 clients, network cut | ✅ all back, slowest 57.7 s, 0 commands missing | 2026-11-05, `rc.3` |
| L5 storm: 500 clients, broker restart | ✅ all back, slowest 59.9 s, 0 commands missing | 2026-11-05, `rc.3` |
| L6 soak: 100 clients, 20 min, 6 faults | ✅ 0 gaps, memory +21.7 % (flat from minute 14) | 2026-11-05, `rc.3` |
| L6 soak: **multi-hour** | ⏳ **not done**. An earlier 5 h run on `rc.1` was closed before it finished; no results | |
| L7 real kiosk lab | ⏳ not done, happens in the project's Step 2c | |

All of this ran on the development PC except where [TESTING §11](TESTING.md#11-history-of-this-kit) says otherwise.

---

## Open items before final `1.1.0`

1. **Multi-hour soak** with a fresh kit, not closed until it finishes (the QoS1 gap verdict is only written at the end):
   `.\run-all.ps1 -SoakOnly -SoakHours 5`.
2. **Handle rise in the soak:** 518 → 704 in 20 minutes, not a pass criterion and not explained. A forced-GC measurement
   showed no leak on the reconnect path (400 reconnect cycles: 425 → 431; 40 failed connects: no growth). The soak also
   has persistent sessions, the offline buffer and Toxiproxy, which that measurement did not. Watch `handles` in `soak-metrics.csv`.
3. **Real kiosk lab test.**
4. Then: version `1.1.0`, final publish, and the project moves from the rc to it.

---

## Known limits

Full list: [TESTING §8](TESTING.md#8-known-limits-and-broker-behaviours-discovered).

- Storms and soak ran against Mosquitto only.
- A publish that times out mid-send resets the connection; it reconnects by itself.
- QoS1 can deliver duplicates; QoS0 refusals cannot be detected.
- Mosquitto `acl_file` grants refused subscriptions (its dynamic-security plugin refuses them correctly).
- The count of unlogged lines is lost on a crash or hard process exit.
- Not in the library: durable offline buffer (SQLite/Redis; pluggable today), `FlushAsync`, OpenTelemetry, MQTT over
  WebSockets. See [FUTURE-FEATURES](FUTURE-FEATURES.md) and [ROADMAP](ROADMAP.md).

Small improvements left on purpose, not worth the risk during an rc: `WaitForHandlersAsync` polls with `Task.Delay(25)`
during dispose; the health check uses `System.Threading.Timer`.

---

## How to release

The owner commits, tags and publishes. The order matters:

```powershell
git add -A
git commit -m "..."
git tag v<version>
git push origin <branch> v<version>
dotnet build ResilientMqtt.csproj -c Release --no-incremental    # builds the package
# Check: the nuspec's <repository commit="..."> must equal the tagged commit
dotnet nuget push bin\Release\ResilientMqtt.<version>.nupkg --api-key <key> --source https://api.nuget.org/v3/index.json
```

**Traps, each one has already happened:**

- **Every Release build rewrites the `.nupkg`** (`GeneratePackageOnBuild`), whatever the code state. A package in `bin\Release`
  may hold code its version number or tag does not.
- **`dotnet pack` can skip the rebuild** as "up to date", leaving the **previous commit** in the package. Use
  `dotnet build --no-incremental` as above.
- **A nuget.org version number can never be reused.** Use `-rc.N` while testing.
- **Push only the file you just built.** `bin\Release` also holds old `1.1.0`, `rc.1` and `rc.2` packages: never push those.
- No `.slnx` at the root on purpose: it would make plain `dotnet build` / `dotnet pack` ambiguous, so tests run with
  `dotnet test tests/ResilientMqtt.Tests`.

---

## Changing the library

1. Plan first; add a test and see it **fail on the old code**, then fix.
2. Run `dotnet test tests/ResilientMqtt.Tests -c Release`; for bigger changes, the campaign: `tests\run-all.ps1` ([TESTING §6](TESTING.md#6-how-to-run)).
3. Update [CHANGELOG](../CHANGELOG.md), this file, `RELEASE-<version>.md`, and README / USAGE / TESTING where they are affected. A new version gets its own `RELEASE-<version>.md`.
4. New `EventId`s: add them to `ResilientMqttLogEvents` + the README table. Ids never change meaning.
5. Public API stays additive (default interface members).
6. Finding numbers continue from **#71**. The audit history is in [ResilientMqtt-1.0.4-Plan.md](ResilientMqtt-1.0.4-Plan.md).

The chaos tests need Docker and are **skipped** in a plain `dotnet test` run: a green run does not include them.

---

## Documents

| Doc | What |
|-----|------|
| [RELEASE-1.1.0](RELEASE-1.1.0.md) | What this version is: what is new, what was fixed, upgrading from 1.0.x |
| [README](../README.md) | What the library is, full guide, EventId table, honest test-coverage list (§10a) |
| [USAGE](USAGE.md) | Every option, recipes (ships inside the NuGet package) |
| [CHANGELOG](../CHANGELOG.md) | Every change with its finding number |
| [TESTING](TESTING.md) | Test layers, every test, how to run, results history |
| [ROADMAP](ROADMAP.md) · [FUTURE-FEATURES](FUTURE-FEATURES.md) | What comes next |
| [ARCHITECTURE](ARCHITECTURE.md) · [EXTENSIONS](EXTENSIONS.md) | Short, high-level principles from before 1.1.0 (no 1.1.0 detail) |
| [Research](ResilientMqtt-Research.md) · [1.0.3 plan](ResilientMqtt-1.0.3-Plan.md) · [1.1.0 investigation](ResilientMqtt-1.0.4-Plan.md) | Audit history (copies from the project) |

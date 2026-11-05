# ResilientMqtt

> **Production-grade MQTT client for .NET 10** — built to survive network drops, broker outages, and process crashes without losing messages.
> One library. Any project type. Every resilience feature toggleable.

[![NuGet](https://img.shields.io/badge/NuGet-ResilientMqtt-blue)]() [![.NET](https://img.shields.io/badge/.NET-10.0-purple)]() [![License](https://img.shields.io/badge/License-MIT-green)]()

---

## Table of Contents

1. [What is ResilientMqtt?](#1-what-is-resilientmqtt)
2. [When to use it (and when not to)](#2-when-to-use-it-and-when-not-to)
3. [Why use it — real-world benefits](#3-why-use-it--real-world-benefits)
4. [ResilientMqtt vs raw MQTTnet](#4-resilientmqtt-vs-raw-mqttnet)
5. [Installation](#5-installation)
6. [Quick start (5 minutes)](#6-quick-start-5-minutes)
7. [Configuration — 3 ways](#7-configuration--3-ways)
8. [Publishing messages](#8-publishing-messages)
9. [Subscribing to messages](#9-subscribing-to-messages)
10. [Lifecycle events (OnConnected / OnDisconnected)](#10-lifecycle-events-onconnected--ondisconnected)
    - 10a. [Connection health — why did it fail? (1.1.0)](#10a-connection-health--why-did-it-fail-110)
11. [Complete Worker Service example](#11-complete-worker-service-example)
12. [Topic prefix behaviour](#12-topic-prefix-behaviour)
13. [Where do log messages go?](#13-where-do-log-messages-go)
14. [Feature toggles](#14-feature-toggles)
15. [Custom offline buffer (banking persistence)](#15-custom-offline-buffer-banking-persistence)
16. [Configuration reference](#16-configuration-reference)
17. [Real-world scenarios & patterns](#17-real-world-scenarios--patterns)
18. [Decision matrix — should I use this?](#18-decision-matrix--should-i-use-this)
19. [⚠️ Critical: IL Trimming gotcha](#19-️-critical-il-trimming-gotcha)
20. [How it works internally](#20-how-it-works-internally)
21. [Troubleshooting](#21-troubleshooting)
22. [FAQ](#22-faq)
23. [Migration from earlier versions](#23-migration-from-earlier-versions)

---

## 1. What is ResilientMqtt?

A thin, opinionated wrapper over [MQTTnet](https://github.com/dotnet/MQTTnet) that handles the **operational realities** of MQTT in production: brokers go down, networks blink, processes crash. The library makes sure your messages don't go down with them.

### What it gives you

- ✅ **`IResilientMqttClient`** — single interface for publish/subscribe
- ✅ **Auto-reconnect** with exponential backoff + jitter
- ✅ **Circuit breaker** on the publish path
- ✅ **Offline message buffer** (in-memory by default, pluggable for SQLite/file)
- ✅ **Broker-side Last Will & Testament** (real LWT, not fake publish)
- ✅ **Idle-aware health checks** (no needless pings)
- ✅ **Per-handler exception isolation**
- ✅ **Connection lifecycle events** (`OnConnected` / `OnDisconnected`) — clean, no polling
- ✅ **Classified failure reasons** (`LastFailure`, `ConnectionInfo`, `ConnectionLost` / `ConnectionRestored`) — *1.1.0*
- ✅ **MQTT 5 sessions that do what they say**, mutual TLS + private-CA trust, concurrent dispatch, publish outcomes — *1.1.0*
- ✅ **Topic prefix auto-application** (multi-tenant friendly)
- ✅ **JSON, raw string, and binary** publish overloads
- ✅ **Optional hosted service** for auto-start/stop
- ✅ Built-in **options validation**
- ✅ **DI-first** — works with ASP.NET, Worker Services, Blazor, WPF, Console, MAUI

### What it deliberately doesn't do

- ❌ String-based "magic" subscription wiring from config (explicit code is better)
- ❌ Reflection-heavy mapping (keeps trim-safety realistic)
- ❌ Hide MQTTnet — it's still there if you need low-level access

---

## 2. When to use it (and when not to)

### ✅ Use ResilientMqtt when

| Scenario | Why |
|---|---|
| **Banking branch agents** on flaky WAN links | Message loss = compliance issue |
| **IoT field devices** on cellular/4G | Network outages are routine |
| **CDK / ATM / kiosk telemetry** | Status reporting must not lie about online state |
| **Server hubs aggregating many edge clients** | Need stable broker connection with health monitoring |
| **Microservices using MQTT for events** | Built-in reliability avoids reinventing wheel |
| **Dashboards / monitoring UIs** | `OnConnected` / `OnDisconnected` events drive UI state cleanly |
| **Any production system** where you'd otherwise write your own reconnect / buffer / circuit-breaker | This library is what you'd build anyway, already battle-tested |

### ❌ Don't use ResilientMqtt when

| Scenario | Use instead |
|---|---|
| **Local development experiments** | Raw MQTTnet — outages don't happen on localhost |
| **One-shot CLI tools** (publish a single message, exit) | Raw MQTTnet — reconnect logic is pure overhead |
| **You need MQTT 5 user properties / reason codes today** | Raw MQTTnet directly (or open an issue) |
| **Your broker is in-process / embedded** | Direct method calls are simpler than MQTT |
| **You don't care about message loss** (logs, throwaway metrics) | Raw MQTTnet with `AtMostOnce` QoS |
| **You need request/response RPC over MQTT** | A purpose-built library like `MQTTnet.Extensions.Rpc` |

### 🟡 Maybe use it when

| Scenario | Consider |
|---|---|
| **High-throughput streaming** (1000+ msg/sec sustained) | Profile first — circuit breaker overhead is tiny but not zero |
| **Tightly resource-constrained device** (Raspberry Pi Zero, ESP32) | Disable features you don't need; or roll your own |
| **Single-tenant simple app** | Library works fine, but you could survive with raw MQTTnet |

---

## 3. Why use it — real-world benefits

### Benefit 1: Network outage survival

**Without this library:**
```csharp
try { await mqttClient.PublishAsync(msg); }
catch (MqttCommunicationException ex)
{
    // Now what? Lose the message? Buffer somewhere? Retry?
    // You'll spend a week building this and miss edge cases.
}
```

**With this library:**
```csharp
await mqtt.PublishAsync("status", payload);
// If broker is down → buffered automatically.
// Network restored → drained automatically. You wrote zero retry code.
```

### Benefit 2: True process-crash detection

**The MQTT "online status" problem:** if your process dies, the broker has no idea — subscribers see you as ONLINE forever.

ResilientMqtt arms a **broker-side Last Will & Testament** at connect time. The broker publishes `OFFLINE` automatically when your TCP connection times out. Other systems see the truth.

```
Process crashes → broker keep-alive expires → broker publishes OFFLINE
Network cable yanked → same thing
Graceful shutdown → we publish OFFLINE explicitly, then disconnect
```

### Benefit 3: No thundering-herd reconnects

When 347 branch agents all reconnect at the same moment after a broker recovery:
- **Without jitter:** synchronized stampede → broker crashes again.
- **With jitter:** random spread of reconnects → broker handles them gracefully.

Already built in. Set `Reconnect.UseJitter = true` (default).

### Benefit 4: Zombie connection detection

TCP sockets can sit in "ESTABLISHED" state for hours after the underlying network died (NAT timeout, firewall state loss). `mqttClient.IsConnected` returns `true` because no RST was received. You'd publish into the void.

ResilientMqtt's **idle-aware health check** pings the broker when nothing has been *received* for a while. If the ping gets no answer while the socket still looks connected, the connection is reset and the normal reconnect runs — you never silently publish into a dead socket. *(Since 1.1.0 only inbound traffic counts as "alive": a QoS0 publish succeeds even on a dead socket, so a client that only publishes is still checked.)*

### Benefit 5: Lifecycle events — no polling loops

**Without `OnConnected` events:**
```csharp
// Anti-pattern: 500ms polling burns 172,800 CPU wakeups/day per service
while (!mqtt.IsConnected && !ct.IsCancellationRequested)
    await Task.Delay(500, ct);
await mqtt.SubscribeAsync(...);
```

**With events:**
```csharp
mqtt.OnConnected += async () =>
{
    await mqtt.SubscribeAsync("commands/+", handler);
};
// Fires on first connect AND every reconnect. Zero polling.
```

### Benefit 6: Pluggable persistence

In-memory buffer is fine for most cases. For zero-loss banking workloads, swap in your own SQLite-backed implementation in one DI line. Library code doesn't change.

```csharp
services.AddResilientMqtt(...)
        .UseOfflineBuffer<SqliteOfflineBuffer>();
```

### Benefit 7: Same library across your fleet

The exact same library binary ships to:
- ASP.NET Core API hub (server)
- 347 Windows Service branch agents (edge)
- Blazor dashboard (UI)
- Internal admin tools (console)

**Only `appsettings.json` differs.** No bespoke code per deployment.

---

## 4. ResilientMqtt vs raw MQTTnet

### Lines of code for "reliable publisher with auto-reconnect, buffering, and LWT"

| Approach | Lines of code | Time to write | Time to debug edge cases |
|---|---|---|---|
| **Raw MQTTnet** | ~400 lines | 2-3 days | Weeks of production issues |
| **ResilientMqtt** | ~10 lines | 5 minutes | Library handles it |

### Feature matrix

| Feature | Raw MQTTnet | ResilientMqtt |
|---|---|---|
| Connect / Publish / Subscribe | ✅ | ✅ (wraps MQTTnet) |
| Auto-reconnect | ⚠️ You write the loop | ✅ Built-in |
| Exponential backoff with jitter | ❌ You implement | ✅ Built-in |
| Circuit breaker | ❌ You implement | ✅ Built-in |
| Offline message buffer | ❌ You implement | ✅ Built-in, pluggable |
| Broker-side LWT (configured correctly) | ⚠️ Easy to misuse | ✅ Just toggle on |
| Idle-aware health checks | ❌ You implement | ✅ Built-in |
| Per-handler exception isolation | ❌ You implement | ✅ Built-in |
| Connection lifecycle events | ⚠️ Low-level only | ✅ `OnConnected` / `OnDisconnected` |
| Multi-tenant topic prefix | ❌ You implement | ✅ Built-in |
| Trim-safe configuration | ❌ Manual XML descriptors | ✅ Built-in |
| DI integration | ⚠️ Manual wiring | ✅ One-line registration |
| Auto-resubscribe on reconnect | ❌ You implement | ✅ Built-in |
| Buffer drain on reconnect | ❌ You implement | ✅ Built-in |
| Configuration validation | ❌ You implement | ✅ Built-in (one clear message per problem) |
| Classified failure reasons | ❌ Raw exceptions / codes | ✅ `LastFailure` + `ConnectionInfo` |
| Refused subscribe / publish detection | ⚠️ Inspect every result | ✅ Logged, counted, `Denied` outcome |
| Mutual TLS / private CA | ⚠️ Manual handlers | ✅ Config only |

### When raw MQTTnet still wins

- You need **MQTT 5 extras** (user properties, request/response, topic aliases) — MQTT 5 is the default protocol and its reason codes are classified, but these are not exposed
- You need **WebSocket transport** (could be added to ResilientMqtt — open an issue)
- You're writing a **broker / server side** application (this library is client-side)

---

## 5. Installation

### Option A — Project reference (recommended for internal use)

Add `ResilientMqtt.csproj` to your solution. In your consuming project:

```xml
<ItemGroup>
  <ProjectReference Include="..\ResilientMqtt\ResilientMqtt.csproj" />
</ItemGroup>
```

### Option B — NuGet (if published to your feed)

```bash
dotnet add package ResilientMqtt
```

**Target framework:** .NET 10.0
**Dependency:** MQTTnet 5.1+

### Running the library's tests

```bash
dotnet test tests/ResilientMqtt.Tests
```

Unit tests plus integration tests against an in-process MQTTnet broker (real sockets: refused sign-in, refused
subscription/publish, sessions, retained messages, zombie connections, takeovers). The test project is excluded from
the library build and the package.

**Full test campaign** (real brokers in Docker, network chaos, 500-client reconnect storm, multi-hour soak):
see [`tests/README-TESTING.md`](tests/README-TESTING.md) — `tests\run-all.ps1 -Smoke` for a quick check.

**Library documents:** [Testing guide](docs/TESTING.md) · [Usage](docs/USAGE.md) · [Future features](docs/FUTURE-FEATURES.md) · [Roadmap](docs/ROADMAP.md) · history of the audits: [1.0.3 research](docs/ResilientMqtt-Research.md), [1.0.3 plan](docs/ResilientMqtt-1.0.3-Plan.md), [1.1.0 investigation & findings](docs/ResilientMqtt-1.0.4-Plan.md).

---

## 6. Quick start (5 minutes)

### Step 1 — Add to `appsettings.json`

```json
{
  "Mqtt": {
    "Host": "broker.bank.lk",
    "Port": 8883,
    "Username": "branch-001",
    "Password": "secret",
    "ClientId": "branch-001-terminal-A",
    "TopicPrefix": "branches/001/A",
    "Tls": {
      "Enabled": true,
      "Mode": "Normal"
    }
  }
}
```

### Step 2 — Register in `Program.cs`

```csharp
using ResilientMqtt;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddResilientMqtt()
    .Configure(builder.Configuration.GetSection("Mqtt"))
    .AddAutoStart()           // auto connect on app start, disconnect on stop
    .ValidateOnStart();       // fail fast if config is invalid

builder.Services.AddHostedService<Worker>();
builder.Build().Run();
```

### Step 3 — Use it

```csharp
public class Worker(IResilientMqttClient mqtt, ILogger<Worker> log) : BackgroundService
{
    public Worker(IResilientMqttClient mqtt, ILogger<Worker> log) : base()
    {
        // Subscriptions register on connect and re-register on every reconnect
        mqtt.OnConnected += async () =>
        {
            await mqtt.SubscribeAsync("server/commands/+", async (topic, payload, ct) =>
            {
                log.LogInformation("Command received: {Topic}", topic);
            });
        };
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await mqtt.PublishAsync("heartbeat", new { time = DateTimeOffset.UtcNow });
            await Task.Delay(10_000, ct);
        }
    }
}
```

**That's it.** The library handles connect, reconnect, buffering, LWT, circuit breaking — invisibly.

---

## 7. Configuration — 3 ways

### Way 1: From `appsettings.json`

```csharp
builder.Services.AddResilientMqtt(builder.Configuration.GetSection("Mqtt"));
```

### Way 2: Code-only (no config file)

```csharp
builder.Services.AddResilientMqtt(opts =>
{
    opts.Host = "broker.bank.lk";
    opts.Port = 8883;
    opts.Username = "branch-001";
    opts.Password = Environment.GetEnvironmentVariable("MQTT_PASS")!;
    opts.ClientId = $"branch-{Environment.MachineName}";
    opts.TopicPrefix = "branches/001";

    opts.Tls.Enabled = true;
    opts.Tls.Mode = "Hard";
    opts.Tls.TrustedThumbprint = "AB12CD34...";
});
```

### Way 3: Combined + chained (most flexible)

```csharp
builder.Services
    .AddResilientMqtt()
    .Configure(builder.Configuration.GetSection("Mqtt"))          // 1. base from JSON
    .Configure(opts => opts.ClientId = $"branch-{Environment.MachineName}")  // 2. override
    .ConfigureFromDependency<AgentConfig>((mqtt, agent) =>         // 3. cross-bind
    {
        mqtt.ClientId    = $"branch-{agent.BranchId}-{agent.TerminalId}";
        mqtt.TopicPrefix = $"branches/{agent.BranchId}/{agent.TerminalId}";
    })
    .UseOfflineBuffer<MySqliteBuffer>()                            // 4. swap impl
    .AddAutoStart()
    .ValidateOnStart();
```

> 💡 **Tip:** `ConfigureFromDependency` is gold for multi-tenant scenarios. Bind `ClientId` and `TopicPrefix` from your branch/terminal identity so the same MQTT config template ships everywhere.

---

## 8. Publishing messages

### Typed (auto JSON serialization)

```csharp
await mqtt.PublishAsync("orders", new
{
    id = 123,
    amount = 50_000m,
    currency = "LKR"
});
// → publishes JSON to "{prefix}/orders"
```

### Raw string

```csharp
await mqtt.PublishRawAsync("logs", "user logged in");
```

### Binary

```csharp
byte[] snapshot = File.ReadAllBytes("snapshot.bin");
await mqtt.PublishBytesAsync("camera/snapshot", snapshot);
```

### With QoS and retain

```csharp
await mqtt.PublishAsync(
    "config/current",
    configObject,
    retain: true,
    qos: ResilientMqttQos.AtLeastOnce);
```

**QoS options:**
- `AtMostOnce` (0) — fire-and-forget, fastest, may lose
- `AtLeastOnce` (1) — guaranteed delivery, may duplicate ← **default, banking-safe**
- `ExactlyOnce` (2) — guaranteed once, slowest

### What happens when the broker is down?

```
PublishAsync()
  ├─ Circuit breaker OPEN?  → buffer, return
  ├─ Not connected?         → buffer, trigger reconnect, return
  ├─ Publish fails?         → buffer, return
  └─ All good → sent on wire
```

**Buffered messages auto-drain when reconnected.** No code needed from you.

---

## 9. Subscribing to messages

Subscriptions are registered **once** (typically inside `OnConnected`). The library automatically resubscribes after every reconnect — your handlers stay live across network drops.

### Style 1 — Inline delegate (quick)

```csharp
mqtt.OnConnected += async () =>
{
    await mqtt.SubscribeAsync("server/commands/restart", async (topic, payload, ct) =>
    {
        logger.LogInformation("Restart received: {Payload}", payload);
    });
};
```

### Style 2 — Class handler (testable)

```csharp
public class CommandHandler : IResilientMqttMessageHandler
{
    private readonly IMyService _service;
    public CommandHandler(IMyService service) => _service = service;

    public async Task HandleAsync(string topic, string payload, CancellationToken ct)
    {
        var cmd = JsonSerializer.Deserialize<Command>(payload);
        await _service.ExecuteAsync(cmd!, ct);
    }
}

// Register
mqtt.OnConnected += async () =>
{
    await mqtt.SubscribeAsync("server/commands/+", commandHandler);
};
```

### MQTT wildcards

| Wildcard | Meaning | Example | Matches |
|---|---|---|---|
| `+` | Exactly one level | `server/commands/+` | `server/commands/ping`, `server/commands/restart` |
| `#` | Zero or more levels (must be last) | `server/#` | `server/anything`, `server/a/b/c` |

### Unsubscribing

```csharp
await mqtt.UnsubscribeAsync("server/commands/+");
```

---

## 10. Lifecycle events (`OnConnected` / `OnDisconnected`)

### The events

```csharp
event Func<Task>? OnConnected;     // fires on first connect AND every reconnect
event Func<Task>? OnDisconnected;  // fires on every disconnect (broker drop OR graceful shutdown)
```

### Why events instead of polling

```csharp
// ❌ ANTI-PATTERN: 172,800 wasted CPU wakeups per day per service
while (!mqtt.IsConnected && !ct.IsCancellationRequested)
    await Task.Delay(500, ct);

// ✅ CORRECT: event-driven, zero polling
mqtt.OnConnected += async () => { ... };
```

### When does `OnConnected` fire?

**Every time** the underlying MQTT connection succeeds — including the initial connect and every reconnect after a drop.

```
Service starts
   ↓
ConnectAsync succeeds  ← OnConnected fires (first time)
   ↓
Network drops
   ↓
Reconnect loop runs
   ↓
ConnectAsync succeeds again  ← OnConnected fires again
```

### What runs inside `OnConnected`

Best practices for `OnConnected` handlers:

```csharp
mqtt.OnConnected += async () =>
{
    // 1. Register subscriptions (idempotent — safe to call on every reconnect)
    await mqtt.SubscribeAsync("server/commands/+", commandHandler);
    await mqtt.SubscribeAsync("server/patches/#", patchHandler);

    // 2. Publish a custom "ready" / "online" event for downstream systems
    await mqtt.PublishAsync("events/agent-ready", new
    {
        agent = "branch-B001",
        at = DateTimeOffset.UtcNow
    });

    // 3. Refresh connection-scoped state if needed
    await tokenService.RefreshIfExpiringAsync();

    // 4. Update UI / metrics
    metrics.Counter("mqtt.connects").Increment();
};
```

### What `OnDisconnected` is for

```csharp
mqtt.OnDisconnected += async () =>
{
    // Update UI status indicator
    statusLight.SetRed();

    // Notify ops if the branch goes down at a critical time
    if (IsBusinessHours())
        await alerter.WarnAsync("Branch B001 offline");

    // Bump metrics
    metrics.Counter("mqtt.disconnects").Increment();
};
```

### Multi-subscriber support

Multiple components can hook the same event independently:

```csharp
mqtt.OnConnected += async () => await componentA.OnReadyAsync();
mqtt.OnConnected += async () => await componentB.RefreshTokenAsync();
mqtt.OnConnected += async () => metrics.IncrementConnects();
```

All run sequentially. **One handler throwing does not break the others** — the library catches per-handler.

### Important guarantees

| Guarantee | Behaviour |
|---|---|
| **Sequential await** | Handlers run one after another, in registration order |
| **Exception isolation** | One throwing handler doesn't affect siblings |
| **Multicast-safe** | Adding/removing handlers during invocation is safe |
| **Idempotent re-subscribe** | Library auto-resubscribes its registry; adding the same topic in `OnConnected` is a no-op upgrade |

### When to use `StateChanged` instead

`OnConnected` / `OnDisconnected` cover the **two transitions that matter**. For fine-grained state tracking (`Connecting`, `Reconnecting`, `ShuttingDown`) the `StateChanged` event remains available:

```csharp
mqtt.StateChanged += (sender, state) =>
{
    logger.LogDebug("MQTT state: {State}", state);
};
```

---

## 10a. Connection health — *why* did it fail? *(1.1.0)*

A resilient client must not only reconnect — it must tell you **why** it couldn't, so your app (or a person) can
react correctly. Wrong credentials, a bad certificate or a stopped broker will **never** fix themselves by retrying;
a network blip will.

### `LastFailure` — the classified reason

```csharp
if (!mqtt.IsConnected && mqtt.LastFailure is { } f)
{
    // f.Kind: AuthRejected | TlsFailure | BrokerUnavailable | Network | Timeout | SessionTakenOver | ProtocolError | Unknown
    logger.LogWarning("MQTT down: {Kind} — {Detail} (code {Code})", f.Kind, f.Detail, f.Code);
}
```

| Kind | Typical cause | Will retrying fix it? |
|------|---------------|-----------------------|
| `AuthRejected` | wrong / rotated / revoked credentials, banned client | ❌ needs new credentials |
| `TlsFailure` | certificate rejected, TLS against a non-TLS port | ❌ needs a config / cert fix |
| `BrokerUnavailable` | port closed, broker stopped / shutting down / busy / rate-limited | ⏳ when the broker is back |
| `Network` | DNS failure, host unreachable, connection reset | ✅ usually |
| `Timeout` | no answer in time (connect, keep-alive, health ping) | ✅ usually |
| `SessionTakenOver` | another client connected with **the same ClientId** (cloned machine?) | ❌ fix the duplicate id |
| `ProtocolError` | invalid client id, unsupported protocol, endpoint isn't an MQTT broker | ❌ config fix |

The mapping is built from **measured** MQTTnet 5 behaviour (e.g. a refused sign-in *returns* a CONNACK code, a closed
port throws `SocketException(ConnectionRefused)`, a rejected certificate throws `AuthenticationException`).

### `ConnectionInfo` — one snapshot for dashboards and health endpoints

```csharp
var i = mqtt.ConnectionInfo;
// i.State, i.IsConnected, i.ConnectedSinceUtc, i.DownSinceUtc, i.ReconnectAttempt,
// i.TotalConnects (rising fast = flapping), i.LastFailure, i.BufferedCount,
// i.RejectedSubscriptions (filter → code), i.DeniedPublishes, i.ProtocolVersion, i.SessionPresent
```

### Events with details

```csharp
mqtt.ConnectionLost += a =>        // once per real drop (not per failed retry)
{
    logger.LogWarning("Lost after {Up}: {Kind}", a.WasConnectedFor, a.Failure.Kind);
    return Task.CompletedTask;
};
mqtt.ConnectionRestored += a =>    // after an outage: downtime, attempts, session resumed?
{
    logger.LogInformation("Back after {Down} ({Attempts} attempts)", a.Downtime, a.Attempts);
    return Task.CompletedTask;
};
```

`OnConnected` / `OnDisconnected` / `StillDown` / `StateChanged` are unchanged. Since 1.1.0 `OnDisconnected` fires
**once per real drop** (it used to fire for every failed reconnect attempt too).

### Did my message actually go out? — `PublishWithResultAsync`

```csharp
var outcome = await mqtt.PublishWithResultAsync("commands/reboot", cmd);
// Sent | Buffered (offline buffer — sent after reconnect) | Dropped (lost) | Denied (broker refused, QoS1/2)
```

### Refusals are no longer silent

- A **subscription the broker refuses** (ACL) is logged as an error (event 1102) and listed in
  `ConnectionInfo.RejectedSubscriptions` — before 1.1.0 it was logged as "Subscribed". *Only possible when the broker
  says so in the SUBACK:* Mosquitto's dynamic-security plugin, EMQX and HiveMQ do; Mosquitto's classic `acl_file`
  **grants** the SUBACK and silently filters delivery — no client can detect that (measured in the test campaign).
- A **QoS1/2 publish the broker refuses** returns `Denied`, is logged (event 1202, rate-limited) and counted —
  before 1.1.0 it counted as a success. (QoS 0 has no acknowledgement, so a refusal can't be seen.)

### Waiting for the connection

Returns `true` once the client is connected **and ready** — subscriptions restored and `OnConnected` handlers finished.

```csharp
if (!await mqtt.WaitForConnectionAsync(TimeSpan.FromSeconds(30)))
    logger.LogWarning("Still offline: {Why}", mqtt.LastFailure?.Kind);
```

### Slow handlers: `Dispatch.Mode`

By default (`Inline`) handlers run one at a time on the receive loop — a slow handler delays all inbound traffic
**and the keep-alive**. Set `"Dispatch": { "Mode": "Concurrent", "MaxConcurrency": 8 }` to run handlers off the
receive loop (messages may then be processed out of order; QoS1 is acknowledged before the handler finishes).

### Log event ids

Every log line carries a stable `EventId` (class `ResilientMqttLogEvents`) so you can filter or alert on it:

| Range | Area | Key ids |
|-------|------|---------|
| 1000 | connection | 1001 Connected · 1002 ConnectFailed · 1003 ConnectTimedOut · 1004 ConnectionLost · 1005 ReconnectScheduled · 1006 StillDown · 1007 DisconnectedGracefully · 1009 ConnectionRestored |
| 1100 | subscriptions | 1101 Subscribed · **1102 SubscriptionRefused** · 1103 Resubscribing · 1105 SubscribeFailed |
| 1200 | publish | 1201 PublishFailed · **1202 PublishDenied** · 1204 PublishBlockedCircuitOpen · 1205 PublishFailuresNotLogged |
| 1300 | offline buffer | 1301 BufferDraining · 1302 BufferDrainFailed · 1303 BufferOverflow · 1304 BufferDropsNotLogged |
| 1400 | health check | 1401 HealthClientDisconnected · 1402 HealthPingFailed · 1403 HealthForcedReconnect |
| 1500 | handlers | 1501 HandlerThrew · 1502 EventHandlerThrew · 1503 MessageUnmatched · 1504 HeldMessagesReplayed · 1505 DispatchBackPressure |
| 1600 | configuration | 1601 TlsInsecure · 1602 SessionMode · 1603 SessionWithRandomClientId · 1604 SessionSettingIgnored |
| 1700 | circuit breaker | 1701 CircuitOpened · 1702 CircuitHalfOpen · 1703 CircuitClosed |

### Test coverage — what is proven and what is not *(honest list)*

**Proven by the test suite (146 tests, `dotnet test tests/ResilientMqtt.Tests`)** — real sockets against an in-process
MQTTnet broker: refused sign-in (+ no `OnDisconnected` per attempt), closed port, silent broker (connect timeout),
persistent vs clean sessions on MQTT 5 and 3.1.1, `SessionTakenOver`, ACL-refused subscribe / QoS1 publish, publish
outcomes, idempotent subscribe + no retained replay on a resumed session, zombie detection on MQTT 5 and 3.1.1 (incl.
the 3.1.1 self-published Will), lost wake-up, resumed-session queue delivered to handlers registered in `OnConnected`
(order kept, acknowledged only after delivery, redelivered after a stop / a reconnect mid-replay), offline unsubscribe,
`Inline`/`Concurrent` dispatch, back-pressure, bounded dispose, breaker probe/drain, graceful status (breaker open,
dispose without disconnect, disconnect during set-up), `WaitForConnectionAsync` readiness, and **TLS end to end**:
`Hard`, `Ca` (+ name check), `None` vs a private CA, `Normal`, **mutual TLS** with a PFX, TLS against a plain port,
unknown mode fallback. Several fixes were also proven by running their test against the **pre-fix code** (it fails).
Re-checked against a real **Mosquitto** broker: refused sign-in, ACL refusals, persistent session, takeover, TLS
`Normal`/`Hard`, TLS against a plain port, closed port.

Since 1.1.0-rc.1 the suite also drives, through a fault-injection decorator: a CONNACK at the timeout boundary (#47), a
shutdown cancel wrapped by MQTTnet (#50/#67), a caller token cancelled mid-set-up (#54), the hold-queue back-pressure
limit, and mutual TLS with a certificate from the Windows certificate store.

**Covered by the test campaign kit, not by `dotnet test`** (needs Docker — `tests/README-TESTING.md`): `Ca` / mutual TLS
/ auth / ACL / restart against **Mosquitto**, the same behaviour on **EMQX and HiveMQ**, real network faults through
Toxiproxy, a **500-client reconnect storm** and a **multi-hour soak** (QoS1 gaps, memory / thread growth).
Last run on `1.1.0-rc.3` (2026-11-05): every stage passed, with a **20-minute** soak. The **multi-hour soak is still open**,
so long-run leaks are not ruled out; storms ran against Mosquitto only.

**Not covered anywhere yet:** MQTT over WebSockets (not supported).

### Sessions — what `CleanSession` really does *(fixed in 1.1.0)*

MQTTnet 5 speaks **MQTT 5.0** by default, where a session lives only as long as `SessionExpiryInterval` says. Before
1.1.0 the library never set it (MQTTnet's default is **0**), so `CleanSession = false` silently meant *"no session"* —
nothing was queued while a client was offline, contrary to these docs. Since 1.1.0:

| Setting | Effect |
|---------|--------|
| `CleanSession: true` | clean start; nothing is queued while offline. Choose this when a stale command must never run late. |
| `CleanSession: false` (no `SessionExpirySeconds`) | persistent session for **1 day** (`DefaultPersistentSessionSeconds`); QoS1/2 messages are queued while offline and delivered on reconnect. **Use a stable `ClientId`.** |
| `SessionExpirySeconds: N` | explicit session length on MQTT 5 (`0` = ends with the connection). Ignored on 3.1.1. |
| `ProtocolVersion: "V311"` | MQTT 3.1.1 (the broker decides session length; no reason codes). |

The startup log (event 1602) states the effective mode, and warns when a persistent session is combined with the
auto-generated `ClientId` (event 1603). On MQTT 5, restored subscriptions use *retain-handling = new subscriptions
only*, so resuming a session doesn't replay every retained message.

**Queued messages are never lost to a late subscription:** a resumed session delivers its queue right after the
CONNACK — often before your `OnConnected` handler has subscribed. Messages that match no subscription during
connection set-up are held and delivered once `OnConnected` finishes, in arrival order (event 1504); held QoS1/2
messages are acknowledged only after they reached a handler, so if the app stops first the broker redelivers them on
the next session. Simplest of all: subscribe **before** connecting — subscriptions are remembered and restored before
any message is delivered. Unsubscribing while offline is applied to the resumed session on the next connect.

**Delivery semantics (at-least-once):** QoS1 messages can be delivered twice around a reconnect (that is MQTT); make
handlers idempotent. In `Inline` mode a QoS1 message is acknowledged after its handler returns; in `Concurrent` mode it
is acknowledged when it is handed to the thread pool (MQTT requires acknowledgements in arrival order, which concurrent
handlers can't guarantee) — a crash while the handler runs loses that message.

**MQTT 3.1.1 note:** when the health check resets a dead ("zombie") connection, a 3.1.1 DISCONNECT always discards the
Will, so the client publishes the Will payload itself first (MQTT 5 sends a reason code that makes the broker publish it).

---

## 11. Complete Worker Service example

This is a **complete, production-ready Worker** for a banking branch agent.

### `Program.cs`

```csharp
using ResilientMqtt;
using BranchControlAgent;
using BranchControlAgent.Core.Models;
using Serilog;

string baseDir = AppContext.BaseDirectory;
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .WriteTo.File(
        Path.Combine(baseDir, "Logs", "daily-log-.txt"),
        rollingInterval: RollingInterval.Day,
        retainedFileCountLimit: 14)
    .CreateLogger();

try
{
    var builder = Host.CreateApplicationBuilder(args);

    builder.Services.AddWindowsService(o => o.ServiceName = "BranchControlAgent");
    builder.Logging.ClearProviders();
    builder.Logging.AddSerilog();

    // App config
    builder.Services
        .AddOptions<AgentConfig>()
        .Bind(builder.Configuration)
        .ValidateOnStart();

    // MQTT — derives ClientId/TopicPrefix from BranchId+TerminalId
    builder.Services
        .AddResilientMqtt()
        .Configure(builder.Configuration.GetSection("Mqtt"))
        .ConfigureFromDependency<AgentConfig>((mqtt, agent) =>
        {
            mqtt.ClientId    = $"branch-{agent.BranchId}-{agent.TerminalId}";
            mqtt.TopicPrefix = $"branches/{agent.BranchId}/{agent.TerminalId}";
        })
        .AddAutoStart()
        .ValidateOnStart();

    builder.Services.AddSingleton<HeartbeatHandler>();
    builder.Services.AddSingleton<CommandHandler>();
    builder.Services.AddHostedService<Worker>();

    await builder.Build().RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Service terminated unexpectedly!");
}
finally
{
    Log.CloseAndFlush();
}
```

### `Worker.cs`

```csharp
using ResilientMqtt;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BranchControlAgent;

public class Worker : BackgroundService
{
    private readonly IResilientMqttClient _mqtt;
    private readonly HeartbeatHandler _heartbeatHandler;
    private readonly CommandHandler _commandHandler;
    private readonly ILogger<Worker> _logger;

    public Worker(
        IResilientMqttClient mqtt,
        HeartbeatHandler heartbeatHandler,
        CommandHandler commandHandler,
        ILogger<Worker> logger)
    {
        _mqtt = mqtt;
        _heartbeatHandler = heartbeatHandler;
        _commandHandler = commandHandler;
        _logger = logger;

        // ════════════════════════════════════════════════════════════
        // Register lifecycle handlers ONCE in the constructor.
        // OnConnected fires on first connect AND every reconnect, so
        // subscriptions and "ready" publishes happen automatically.
        // ════════════════════════════════════════════════════════════
        _mqtt.OnConnected    += OnMqttConnectedAsync;
        _mqtt.OnDisconnected += OnMqttDisconnectedAsync;
    }

    private async Task OnMqttConnectedAsync()
    {
        _logger.LogInformation("MQTT connected — registering subscriptions");

        await _mqtt.SubscribeAsync("server/commands/+", _commandHandler);
        await _mqtt.SubscribeAsync("branches/+/heartbeat", _heartbeatHandler);

        // Announce we're ready (custom event, separate from LWT status)
        await _mqtt.PublishAsync("events/agent-ready", new
        {
            at = DateTimeOffset.UtcNow,
            version = "1.0.0"
        });

        _logger.LogInformation("Subscriptions registered, ready event published");
    }

    private Task OnMqttDisconnectedAsync()
    {
        _logger.LogWarning("MQTT disconnected — reconnect loop will retry");
        return Task.CompletedTask;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker started");

        // Periodic heartbeat publish — runs whether or not broker is currently up.
        // If broker is down, messages are buffered and drained on reconnect.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _mqtt.PublishAsync("heartbeat", new
                {
                    time = DateTimeOffset.UtcNow,
                    uptime = Environment.TickCount64,
                    mqttConnected = _mqtt.IsConnected
                }, ct: stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Heartbeat publish failed");
            }

            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _mqtt.OnConnected    -= OnMqttConnectedAsync;
        _mqtt.OnDisconnected -= OnMqttDisconnectedAsync;
        await base.StopAsync(cancellationToken);
    }
}
```

### What this does at runtime

**Startup:**
```
Service starts
   ↓
McsMqttHostedService (from AddAutoStart) → ConnectAsync (fire-and-forget)
   ↓
Worker.ExecuteAsync starts publishing heartbeats (buffered if broker down)
   ↓
ConnectAsync succeeds → OnConnected fires → subscriptions registered
   ↓
Buffered heartbeats auto-drain to broker
```

**Network drop:**
```
Connection drops
   ↓
OnDisconnected fires → log warning, alerter notified
   ↓
Heartbeat publishes → BUFFERED (in-memory, max 1000 messages)
   ↓
Reconnect loop: 1s, 2s, 4s... with jitter
   ↓
Broker publishes our LWT (OFFLINE) — subscribers alerted
```

**Recovery:**
```
ConnectAsync succeeds again
   ↓
Library auto-resubscribes all registered topics
   ↓
OnConnected fires → subscriptions re-added (idempotent), "ready" published
   ↓
ONLINE status published (overrides retained OFFLINE)
   ↓
Buffered messages drain one-by-one (50ms apart)
   ↓
Normal operation resumes — NO DATA LOST
```

**Graceful shutdown:**
```
Windows Service stop
   ↓
Worker.ExecuteAsync exits (stoppingToken)
   ↓
McsMqttHostedService.StopAsync → publishes OFFLINE → DISCONNECT
   ↓
LWT suppressed (we left politely)
```

---

## 12. Topic prefix behaviour

When `TopicPrefix` is set (e.g. `"branches/B001/T006"`), the library applies it differently to publish vs subscribe:

| Operation | Default | Reason |
|---|---|---|
| `PublishAsync` | **prefix ADDED** | Your publishes belong to this client; namespacing them is the usual intent |
| `SubscribeAsync` | **prefix NOT added** | Clients usually listen to global/server topics (`server/commands/+`) — the prefix would silently break those subscriptions |
| `UnsubscribeAsync` | **prefix NOT added** | Must match how it was subscribed |

Every method accepts a `useTopicPrefix` parameter to override:

```csharp
// Publish examples
await mqtt.PublishAsync("status", "ONLINE");
//  → "branches/B001/T006/status"          (default: useTopicPrefix=true)

await mqtt.PublishAsync("global/events", evt, useTopicPrefix: false);
//  → "global/events"                       (absolute topic)

// Subscribe examples
await mqtt.SubscribeAsync("server/commands/+", handler);
//  → "server/commands/+"                   (default: useTopicPrefix=false)

await mqtt.SubscribeAsync("responses/+", handler, useTopicPrefix: true);
//  → "branches/B001/T006/responses/+"      (listen only to my own responses)
```

**Rule of thumb:** sending **about yourself** → default. Sending or listening to **shared/global** topic → flip the flag.

---

## 13. Where do log messages go?

The library uses standard `Microsoft.Extensions.Logging` — it logs to **whatever your host configures**.

### In a Serilog-based app

```csharp
.WriteTo.Console()
.WriteTo.File("Logs/daily-log-.txt", rollingInterval: RollingInterval.Day)
```

Result — every `[ResilientMqttClient]` log line automatically goes to:
- ✅ Console (`dotnet run` window)
- ✅ `bin/Debug/net10.0/Logs/daily-log-2026-05-13.txt`
- ✅ Windows Event Viewer (when running as Windows Service)

### Log levels emitted by the library

| Level | Examples |
|---|---|
| `Information` | Connected, Subscribed, Drain start, Disconnected gracefully |
| `Warning` | Health check disconnected, Circuit breaker OPEN, Buffer overflow, Connect rejected |
| `Error` | Publish failed, Handler threw, Drain failed, OnConnected handler threw |

Filter prefix:

```bash
# Windows
type Logs\daily-log-2026-05-13.txt | findstr "ResilientMqtt"

# Linux / Mac
grep "ResilientMqtt" Logs/daily-log-2026-05-13.txt
```

### Tuning verbosity

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "ResilientMqtt": "Warning"
    }
  }
}
```

Or with Serilog overrides:

```csharp
.MinimumLevel.Override("ResilientMqtt", LogEventLevel.Warning)
```

---

## 14. Feature toggles

Every resilience feature has an `Enabled` flag.

```csharp
services.AddResilientMqtt(opts =>
{
    opts.Host = "broker.example.com";

    opts.OfflineBuffer.Enabled    = false;
    opts.CircuitBreaker.Enabled   = false;
    opts.HealthCheck.Enabled      = false;
    opts.Reconnect.Enabled        = false;
    opts.LastWill.Enabled         = false;
    opts.Tls.Enabled              = false;
});
```

Same toggles in `appsettings.json`:

```json
{
  "Mqtt": {
    "Host": "broker.example.com",
    "OfflineBuffer":  { "Enabled": false },
    "CircuitBreaker": { "Enabled": false },
    "HealthCheck":    { "Enabled": false },
    "Reconnect":      { "Enabled": false },
    "LastWill":       { "Enabled": false },
    "Tls":            { "Enabled": false }
  }
}
```

**Internally:** disabled features get a no-op (`Null*`) implementation — zero CPU/memory overhead.

---

## 15. Custom offline buffer (banking persistence)

Default buffer is in-memory (lost on process crash). For zero-loss banking workloads, implement `IOfflineBuffer`:

```csharp
public class SqliteOfflineBuffer : IOfflineBuffer
{
    private readonly SqliteConnection _db;
    public SqliteOfflineBuffer(string dbPath) { /* init schema */ }

    public int Count => /* SELECT COUNT(*) FROM offline_messages */;

    public bool TryEnqueue(MqttApplicationMessage message)
    {
        // INSERT INTO offline_messages (topic, payload, qos, retain) VALUES (...)
        return true;
    }

    public bool TryPeek(out MqttApplicationMessage? message)
    {
        // SELECT * FROM offline_messages ORDER BY id LIMIT 1
        message = /* reconstruct from row */;
        return message is not null;
    }

    public bool TryDequeue(out MqttApplicationMessage? message)
    {
        // DELETE oldest row, return it
        message = null;
        return false;
    }

    public Task ClearAsync(CancellationToken ct = default)
    {
        // DELETE FROM offline_messages
        return Task.CompletedTask;
    }
}
```

Register it:

```csharp
services.AddResilientMqtt(...)
        .UseOfflineBuffer<SqliteOfflineBuffer>();
```

The rest of the library uses your buffer transparently. No code changes elsewhere.

---

## 16. Configuration reference

### Root settings

| Key | Type | Default | Description |
|---|---|---|---|
| `Mqtt:Host` | string | `localhost` | Broker hostname or IP |
| `Mqtt:Port` | int | `1883` | Broker TCP port (use 8883 for TLS) |
| `Mqtt:Username` | string | `""` | Auth username (empty = anonymous) |
| `Mqtt:Password` | string | `""` | Auth password |
| `Mqtt:ClientId` | string | random GUID | **MUST be unique per running instance** (stable when using a persistent session) |
| `Mqtt:TopicPrefix` | string | `""` | Auto-prefix for publishes |
| `Mqtt:ProtocolVersion` | string | `V500` | `V500` (MQTT 5.0) or `V311` (MQTT 3.1.1) — *1.1.0* |
| `Mqtt:CleanSession` | bool | `false` | `false` = persistent session (QoS1/2 queued while offline — see §10a); `true` = clean start |
| `Mqtt:SessionExpirySeconds` | int? | *(auto)* | MQTT 5 session length; auto = 86400 when `CleanSession=false`, 0 when `true` — *1.1.0* |
| `Mqtt:KeepAliveSeconds` | int | `30` | MQTT keep-alive interval |
| `Mqtt:PublishTimeoutSeconds` | int | `15` | Max wait for one publish (QoS1/2 ack) before it counts as failed |

### TLS

| Key | Type | Default | Description |
|---|---|---|---|
| `Mqtt:Tls:Enabled` | bool | `false` | Enable TLS |
| `Mqtt:Tls:Mode` | string | `None` | `None` (OS validation) / `Normal` (accept any — dev only) / `Hard` (pin thumbprint) / `Ca` (trust a private CA). Empty = `None`; an unknown value falls back to `None` with a warning (as in 1.0.3) and fails `ValidateOnStart()` |
| `Mqtt:Tls:TrustedThumbprint` | string | `""` | Certificate thumbprint when Mode=Hard (spaces/colons ignored) |
| `Mqtt:Tls:CaCertificatePath` | string | `""` | Mode=Ca: CA certificate (PEM/DER) that issued the broker cert — *1.1.0* |
| `Mqtt:Tls:CheckCertificateName` | bool | `true` | Mode=Ca: also require the cert name to match the host — *1.1.0* |
| `Mqtt:Tls:ClientCertificatePath` | string | `""` | Mutual TLS: client cert + key (PFX) — *1.1.0* |
| `Mqtt:Tls:ClientCertificatePassword` | string | `""` | PFX password (use secrets / env vars) — *1.1.0* |
| `Mqtt:Tls:ClientCertificateThumbprint` | string | `""` | Mutual TLS: client cert from the OS store instead of a file — *1.1.0* |
| `Mqtt:Tls:ClientCertificateStore` | string | `LocalMachine` | `LocalMachine` or `CurrentUser` — *1.1.0* |

### Last Will & Testament

| Key | Type | Default | Description |
|---|---|---|---|
| `Mqtt:LastWill:Enabled` | bool | `true` | Arm broker-side LWT |
| `Mqtt:LastWill:Topic` | string | `status` | Sub-topic (prefixed) for status feed |
| `Mqtt:LastWill:OnlinePayload` | string | `ONLINE` | Published at successful connect |
| `Mqtt:LastWill:OfflinePayload` | string | `OFFLINE` | LWT + graceful shutdown payload |
| `Mqtt:LastWill:Retain` | bool | `true` | Broker retains last status |

### Offline Buffer

| Key | Type | Default | Description |
|---|---|---|---|
| `Mqtt:OfflineBuffer:Enabled` | bool | `true` | Buffer publishes when disconnected |
| `Mqtt:OfflineBuffer:MaxSize` | int | `1000` | Drop-oldest threshold |
| `Mqtt:OfflineBuffer:DrainPauseMs` | int | `50` | Delay between drained messages |

### Circuit Breaker

| Key | Type | Default | Description |
|---|---|---|---|
| `Mqtt:CircuitBreaker:Enabled` | bool | `true` | Short-circuit on repeated failures |
| `Mqtt:CircuitBreaker:FailureThreshold` | int | `5` | Failures before opening |
| `Mqtt:CircuitBreaker:ResetMinutes` | int | `2` | Open → HalfOpen wait |

### Health Check

| Key | Type | Default | Description |
|---|---|---|---|
| `Mqtt:HealthCheck:Enabled` | bool | `true` | Enable idle-aware ping monitor |
| `Mqtt:HealthCheck:IntervalMinutes` | int | `1` | Timer tick cadence |
| `Mqtt:HealthCheck:IdleThresholdMinutes` | int | `2` | Time without *inbound* traffic before pinging |
| `Mqtt:HealthCheck:PingTimeoutSeconds` | int | `10` | No ping reply in time → the connection is reset |

### Reconnect

| Key | Type | Default | Description |
|---|---|---|---|
| `Mqtt:Reconnect:Enabled` | bool | `true` | Auto-reconnect on disconnect |
| `Mqtt:Reconnect:BaseDelaySeconds` | int | `1` | Initial backoff delay |
| `Mqtt:Reconnect:MaxDelaySeconds` | int | `60` | Backoff cap |
| `Mqtt:Reconnect:UseJitter` | bool | `true` | Randomize delays (prevents thundering herd) |
| `Mqtt:Reconnect:ConnectTimeoutSeconds` | int | `15` | Max wait for one connect (TCP + handshake); also bounds subscribe/unsubscribe |
| `Mqtt:Reconnect:StillDownAfterSeconds` | int | `120` | Raise `StillDown` once per outage after this long (0 = off) |
| `Mqtt:Reconnect:AuthRejectedDelaySeconds` | int | `0` | After a refused sign-in wait at least this long before retrying (0 = normal backoff) — *1.1.0* |

### Dispatch *(1.1.0)*

| Key | Type | Default | Description |
|---|---|---|---|
| `Mqtt:Dispatch:Mode` | string | `Inline` | `Inline` (in order, on the receive loop) / `Concurrent` (off the loop) |
| `Mqtt:Dispatch:MaxConcurrency` | int | `8` | Concurrent: handlers running at once |
| `Mqtt:Dispatch:ShutdownWaitSeconds` | int | `5` | Concurrent: wait for running handlers on dispose |
| `Mqtt:Dispatch:MaxPending` | int | `1000` | Concurrent: handler calls allowed to wait before intake is slowed (back-pressure, no unbounded memory) |

---

## 17. Real-world scenarios & patterns

### Scenario 1: Branch agent reporting transactions

```csharp
public class TransactionReporter
{
    private readonly IResilientMqttClient _mqtt;
    public TransactionReporter(IResilientMqttClient mqtt) => _mqtt = mqtt;

    public Task ReportAsync(Transaction tx, CancellationToken ct) =>
        _mqtt.PublishAsync(
            $"transactions/{tx.Type}",
            tx,
            qos: ResilientMqttQos.AtLeastOnce,
            ct: ct);
}
```

If the broker is down, the message is buffered. The reporter never sees the outage.

### Scenario 2: Server hub aggregating many branches

```csharp
public class BranchEventCollector
{
    public BranchEventCollector(IResilientMqttClient mqtt, ILogger log)
    {
        mqtt.OnConnected += async () =>
        {
            // Listen to events from ALL branches
            await mqtt.SubscribeAsync("branches/+/events/#", async (topic, payload, ct) =>
            {
                var parts = topic.Split('/');
                var branchId = parts[1];
                log.LogInformation("Event from {Branch}: {Topic}", branchId, topic);
            });
        };
    }
}
```

### Scenario 3: Dashboard with real-time status

```csharp
public class DashboardPresenter
{
    public DashboardPresenter(IResilientMqttClient mqtt, IUiThread ui)
    {
        mqtt.OnConnected    += async () => await ui.RunAsync(() => StatusLight.Color = Green);
        mqtt.OnDisconnected += async () => await ui.RunAsync(() => StatusLight.Color = Red);
    }
}
```

### Scenario 4: Patch deployment with broker queue

```csharp
mqtt.OnConnected += async () =>
{
    // Subscribe with high QoS — broker will queue messages while we're offline
    await mqtt.SubscribeAsync(
        "patches/critical/#",
        new PatchHandler(),
        qos: ResilientMqttQos.AtLeastOnce);
};

// Combined with CleanSession: false + a STABLE ClientId → the broker queues them while offline
// for SessionExpirySeconds (default 1 day). (Before 1.1.0 this did NOT work on MQTT 5 — see §10a.)
```

### Scenario 5: Multi-tenant API server

```csharp
public class TenantBroadcastService
{
    private readonly IResilientMqttClient _mqtt;
    public TenantBroadcastService(IResilientMqttClient mqtt) => _mqtt = mqtt;

    public Task NotifyTenantAsync(string tenantId, object payload, CancellationToken ct) =>
        _mqtt.PublishAsync(
            $"tenants/{tenantId}/notifications",
            payload,
            useTopicPrefix: false,   // absolute topic — server doesn't use a prefix
            ct: ct);
}
```

---

## 18. Decision matrix — should I use this?

### ✅ Strong "yes" signals

- You have an MQTT-using app that **must keep working** when the network blinks
- You're running on **edge devices** (branches, IoT, kiosks) with unreliable connections
- You need **broker-side OFFLINE detection** (LWT) for accurate status reporting
- You manage **multiple clients** that should not stampede on broker recovery
- You're building **multiple .NET apps** that all need MQTT — same library deployed everywhere
- You want **standard DI / config / logging** integration
- Your project is .NET 8+ (built for .NET 10)

### ⚠️ "Probably yes, but check trade-offs"

- You need very high throughput (1000+ msg/sec) — profile circuit breaker overhead
- You target trimmed / AOT builds — library is trim-safe but **test it**
- You need MQTT 5 extras (user properties, request/response) — MQTT 5 is the default protocol, but those aren't exposed yet

### ❌ "Probably no"

- One-shot CLI tools / scripts
- Localhost dev experiments
- Throwaway prototypes
- You need broker / server side functionality
- You need WebSocket transport (raw MQTTnet works, library doesn't expose it yet)

### Cost-benefit summary

| Cost | Magnitude |
|---|---|
| Learning the API | ~30 minutes |
| Adding to a new project | 1 line of code |
| Binary size | ~200 KB on top of MQTTnet |
| Runtime overhead | Negligible (microseconds per publish) |

| Benefit | Magnitude |
|---|---|
| Code you don't write | ~400 lines per app |
| Production incidents avoided | Pay for itself in 1 saved outage |
| Consistency across services | Massive (same behaviour everywhere) |

---

## 19. ⚠️ Critical: IL Trimming gotcha

### The problem

If you enable **"Trim unused code"** in your publish profile (or `<PublishTrimmed>true</PublishTrimmed>` in your `.csproj`), configuration binding **silently breaks**:

- Symptom: `appsettings.json` values are ignored, defaults remain
- Cause: trimmer can't see reflection-based property binding, strips "unused" properties

### Symptom example

```
[INF] === ACTUAL LOADED MQTT VALUES ===
[INF] Host:     'localhost'        ← should be 'broker.bank.lk'
[INF] Username: '' (len=0)         ← should be 'branch-001'
[INF] Tls.Enabled: false           ← should be true
```

### The fix (already built in)

ResilientMqtt ships with an `ILLink.Descriptors.xml` embedded resource that tells the trimmer **NOT** to strip the options classes. As long as you reference the library normally, you're fine.

### If you still see issues

Make sure your `.csproj` references the library properly (`<ProjectReference>` or `<PackageReference>`). If you're seeing trimmed-away properties anyway, add to your own `.csproj`:

```xml
<ItemGroup>
  <TrimmerRootAssembly Include="ResilientMqtt" />
</ItemGroup>
```

### When in doubt: disable trimming

For banking branch deployments, **70 MB single-file exe** with trimming OFF is vastly better than 25 MB with silent runtime breakage. Disable in the publish profile:

```
☑ Produce single file
☐ Trim unused code    ← LEAVE UNCHECKED
☑ Self-contained
```

---

## 20. How it works internally

### Publish path

```
PublishAsync()
  ├─ Build full topic (prepend prefix if enabled)
  ├─ Serialize payload to bytes
  ├─ Build MqttApplicationMessage (topic, payload, QoS, retain)
  │
  ├─ CircuitBreaker.TryAcquire()?
  │     └─ OPEN → buffer + return
  │
  ├─ IsConnected?
  │     └─ No → buffer + trigger reconnect + return
  │
  ├─ Acquire _publishLock (serializes concurrent publishes)
  │
  ├─ _client.PublishAsync(message)
  │     ├─ Success → breaker.RecordSuccess(), healthCheck.NotifyActivity()
  │     └─ Throws  → log, breaker.RecordFailure(), buffer
  │
  └─ Release _publishLock
```

### Reconnect state machine

```
Connected ──[disconnect event]──► Reconnecting
                                       │
                                       ▼
                              ┌─ delay = backoff(attempt) ─┐
                              │                            │
                              ▼                            │
                         ConnectAsync()                    │
                              │                            │
                  ┌───────────┴───────────┐                │
                  │                       │                │
              Success                  Failure             │
                  │                       │                │
                  ▼                       └────────────────┘
            Connected                  (retry forever until shutdown)
            ↓
            (resubscribe → publish ONLINE → fire OnConnected → drain buffer → start health check)
```

### LWT lifecycle

```
Branch agent                          Broker
     │── CONNECT(will={OFFLINE}) ───────►│  (broker stores the will)
     │                                   │
     │── PUBLISH(ONLINE) ────────────────►│  (overrides retained OFFLINE)
     │                                   │
     ╳── network cable yanked            │
     │                                   │
     │   (keep-alive timeout: 30s)       │
     │                                   ▼
     │                        ┌──────────────────────┐
     │                        │ Broker publishes OUR │
     │                        │ Will: OFFLINE        │
     │                        └──────────────────────┘
                                          │
                                          ▼
                              All subscribers see OFFLINE
```

### Why exponential backoff with jitter matters

Without jitter, 347 branches reconnecting at `t = 64s` simultaneously → broker handles 347 simultaneous CONNECTs → struggles.

With jitter, each branch picks a random delay in `[baseDelay, exponential]` → load spreads → broker handles them gracefully.

```csharp
exponential = baseDelay * 2^(attempt - 1)
jittered    = Random * exponential
delay       = max(jittered, baseDelay)
```

---

## 21. Troubleshooting

### First step for any connection problem: read `LastFailure`

The log line `Connect failed — <Kind>: <detail>` (event 1002/1003) or `mqtt.LastFailure` / `mqtt.ConnectionInfo`
tells you which of the cases below you're in (see §10a).

### "Connect failed — broker not responding"

- ✅ Check `Mqtt:Host` and `Mqtt:Port`
- ✅ Verify broker is running: `telnet <host> <port>`
- ✅ Check firewall: port 1883 (plain) or 8883 (TLS) open?
- ✅ DNS: `nslookup <host>` from the deployment machine

### "NotAuthorized" repeatedly

- ✅ Verify username / password on the broker side (try MQTT Explorer with same creds)
- ✅ Check for **IL trimming** stripping properties (see section 19)
- ✅ Confirm `appsettings.json` is in publish folder
- ✅ Use the debug logger snippet (section 22 FAQ) to inspect actually-loaded values
- ✅ `LastFailure.Kind == AuthRejected` — retrying won't help; consider `Reconnect:AuthRejectedDelaySeconds` so a fleet with a rotated password doesn't hammer the broker

### Messages aren't reaching subscribers

- ✅ MQTT topics are **case-sensitive**
- ✅ Remember the topic prefix — `PublishAsync("status")` with prefix `"branches/001"` actually publishes to `branches/001/status`
- ✅ Wildcards: `+` matches **one** level, `#` matches **many** (must be last)

### "ClientId conflict — disconnected" loop

- ✅ Two services using the same `ClientId` kick each other off forever
- ✅ Derive `ClientId` per-instance (machine name, branch ID)
- ✅ Don't rely on the random default in production
- ✅ On MQTT 5 the victim's `LastFailure.Kind` is `SessionTakenOver` — a duplicated/cloned machine

### Buffer keeps overflowing

- ✅ Increase `OfflineBuffer:MaxSize`
- ✅ Or implement persistent buffer (section 15)
- ✅ Or reduce publish frequency / batch messages

### TLS handshake fails

- ✅ Try `Mqtt:Tls:Mode = "Normal"` first to confirm TLS works at all
- ✅ Then switch to `"Hard"` with correct `TrustedThumbprint`
- ✅ Get thumbprint: `openssl s_client -connect broker:8883 | openssl x509 -fingerprint -noout`
- ✅ Private PKI: `Mode = "Ca"` + `CaCertificatePath` validates the full chain (survives broker cert renewal, unlike a pinned thumbprint)
- ✅ Broker requires client certificates: set `ClientCertificatePath` (+ password) or `ClientCertificateThumbprint`
- ✅ `LastFailure.Detail` says "TLS on a non-TLS port?" when TLS was attempted against a plain listener

### `OnConnected` never fires

- ✅ Confirm broker connect succeeds (log shows "Connected — ...")
- ✅ Make sure handler subscription happens **before** `AddAutoStart()` kicks off — register in constructor, not in `ExecuteAsync`
- ✅ Check for exceptions inside the handler (per-handler isolation logs them)

---

## 22. FAQ

**Q: Does this support MQTT v5?**
A: Yes — **MQTT 5.0 is the default** (`ProtocolVersion: "V500"`); set `"V311"` for 3.1.1-only brokers. Broker reason codes (refused sign-in, session taken over, server shutting down…) are classified into `LastFailure`. User properties / request-response are not exposed yet.

**Q: Can I use this in a console app / WPF / Blazor?**
A: Yes — anywhere `IServiceCollection` and `Microsoft.Extensions.Logging` work. For non-host apps, build the provider manually:
```csharp
var services = new ServiceCollection();
services.AddLogging(b => b.AddConsole());
services.AddResilientMqtt(opts => opts.Host = "...");
var sp = services.BuildServiceProvider();
var mqtt = sp.GetRequiredService<IResilientMqttClient>();
await mqtt.ConnectAsync();
```

**Q: Is `IResilientMqttClient` thread-safe?**
A: Yes. `PublishAsync` from multiple threads is fine — internally serialized via `SemaphoreSlim`. `SubscribeAsync` uses a `ConcurrentDictionary`. Reconnect, drain, breaker and dispose paths are guarded (1.0.3/1.1.0 hardening, covered by the test suite).

**Q: What happens to QoS 1/2 messages if my app crashes?**
A: Messages **sent to you**: with `CleanSession = false` (and a stable `ClientId`) the broker queues them for `SessionExpirySeconds` (default 1 day) and delivers them when you reconnect. Messages **you publish** while offline sit in the offline buffer, which is in-memory and lost on a crash — use a persistent `IOfflineBuffer` for zero-loss (section 15).

**Q: Can I have multiple MQTT connections in one app?**
A: Not directly — `AddResilientMqtt` registers `IResilientMqttClient` as a singleton. For multiple brokers, run multiple processes or open an issue.

**Q: Does it work with HiveMQ / Mosquitto / EMQX / AWS IoT Core?**
A: Yes — any MQTT 5.0 broker, or any 3.1.1 broker with `ProtocolVersion: "V311"`. Mutual TLS (AWS IoT, Azure IoT, private PKI) is supported via `Tls:ClientCertificate*`.

**Q: Why is `IHostedService` used for auto-start instead of `BackgroundService`?**
A: Auto-start has two run-once events (connect on boot, disconnect on shutdown) — no continuous loop. `IHostedService` is the right contract.

**Q: How do I see what configuration values actually loaded?**
A: Drop this debug snippet after `host = builder.Build()`:
```csharp
using (var scope = host.Services.CreateScope())
{
    var opts = scope.ServiceProvider
        .GetRequiredService<IOptions<ResilientMqttOptions>>().Value;
    Log.Information("MQTT loaded — Host={Host}, User='{User}', Tls={Tls}",
        opts.Host, opts.Username, opts.Tls.Enabled);
}
```

**Q: Why do my appsettings.json values get ignored?**
A: Most likely IL trimming. See section 19.

**Q: How do I run "one-time initialization" after the first connect (not every reconnect)?**
A: Use your own `bool` flag inside the `OnConnected` handler:
```csharp
bool firstTime = true;
mqtt.OnConnected += async () =>
{
    if (firstTime)
    {
        firstTime = false;
        await DoOneTimeSetupAsync();
    }
    await RegisterSubscriptionsAsync();
};
```

---

## 23. Migration from earlier versions

If you were using a polling-based pattern before lifecycle events were added:

### Before (polling loop)

```csharp
protected override async Task ExecuteAsync(CancellationToken ct)
{
    while (!_mqtt.IsConnected && !ct.IsCancellationRequested)
        await Task.Delay(500, ct);

    if (_mqtt.IsConnected && Interlocked.Exchange(ref _subscribed, 1) == 0)
    {
        await _mqtt.SubscribeAsync("topic", handler);
    }

    await Task.Delay(-1, ct);
}
```

### After (OnConnected event)

```csharp
public Worker(IResilientMqttClient mqtt, IMyHandler handler)
{
    _mqtt = mqtt;
    _mqtt.OnConnected += async () =>
    {
        await _mqtt.SubscribeAsync("topic", handler);
    };
}

protected override Task ExecuteAsync(CancellationToken ct)
    => Task.Delay(Timeout.Infinite, ct);
```

**Benefits gained:**
- Eliminates 172,800 CPU wakeups per day per service
- No `Interlocked` flag needed — library handles re-subscribe
- Subscription registration also fires on reconnect (was missing before)
- Cleaner, smaller, testable

**Breaking changes:** none. The old pattern still works; the new one is just better.

---

## License

MIT.

## Authors

MCS Computer Systems (Pvt) Limited — H.A.T.S Thisera.

---

*Built for Sri Lankan banking infrastructure. Field-tested across 347+ CDK devices.*
# ResilientMqtt — Developer Usage Guide

> 📘 **This document is a cookbook.** Copy, paste, configure, ship.
> For "what is this library / why does it exist / internals" — see [README.md](README.md).
> Target framework: **.NET 10.0**. Dependency: **MQTTnet 5.1+**.

---

## Table of Contents

1. [Install](#1-install)
2. [Minimum viable setup (just publish/subscribe)](#2-minimum-viable-setup-just-publishsubscribe)
3. [Full configuration template](#3-full-configuration-template)
4. [Every option explained](#4-every-option-explained)
5. [Configure 3 ways](#5-configure-3-ways)
6. [Auto-derive ClientId / TopicPrefix from your own config](#6-auto-derive-clientid--topicprefix-from-your-own-config)
7. [Project-type recipes](#7-project-type-recipes)
   - [Worker Service (Windows Service)](#71-worker-service-windows-service)
   - [ASP.NET Core API](#72-aspnet-core-api)
   - [Blazor Server / WASM](#73-blazor-server--wasm)
   - [WPF / WinForms desktop](#74-wpf--winforms-desktop)
   - [Console app](#75-console-app)
8. [Publishing](#8-publishing)
9. [Subscribing](#9-subscribing)
10. [Connection lifecycle events](#10-connection-lifecycle-events)
11. [Toggle features on / off](#11-toggle-features-on--off)
12. [Custom offline buffer (persistent storage)](#12-custom-offline-buffer-persistent-storage)
13. [Logging — view library logs](#13-logging--view-library-logs)
14. [Common patterns (copy-paste ready)](#14-common-patterns-copy-paste-ready)
15. [What to do when things break](#15-what-to-do-when-things-break)
16. [⚠️ Critical: IL trimming gotcha](#16-️-critical-il-trimming-gotcha)
17. [Migration from older versions](#17-migration-from-older-versions)
18. [Quick reference cheat sheet](#18-quick-reference-cheat-sheet)

---

## 1. Install

```bash
dotnet add package ResilientMqtt
```

That's it. No other dependencies — MQTTnet comes transitively.

**Verify install:**
```bash
dotnet list package | grep ResilientMqtt
```

---

## 2. Minimum viable setup (just publish/subscribe)

The absolute smallest amount of code to get going.

### `appsettings.json`
```json
{
  "Mqtt": {
    "Host": "localhost",
    "Port": 1883,
    "ClientId": "my-app"
  }
}
```

### `Program.cs`
```csharp
using ResilientMqtt;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddResilientMqtt()
    .Configure(builder.Configuration.GetSection("Mqtt"))
    .AddAutoStart();

builder.Services.AddHostedService<MyWorker>();
builder.Build().Run();
```

### Your code
```csharp
public class MyWorker(IResilientMqttClient mqtt) : BackgroundService
{
    public MyWorker(IResilientMqttClient mqtt) : base()
    {
        mqtt.OnConnected += async () =>
        {
            await mqtt.SubscribeAsync("incoming/topic", async (topic, payload, ct) =>
            {
                Console.WriteLine($"Got: {payload}");
            });
        };
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await mqtt.PublishAsync("outgoing/topic", new { hello = "world" });
            await Task.Delay(5000, ct);
        }
    }
}
```

**That's a complete, production-quality MQTT client.** Reconnect, buffering, LWT — all running behind the scenes.

---

## 3. Full configuration template

Copy this into your `appsettings.json` and edit the values. Every option is shown.

```json
{
  "Mqtt": {
    "Host": "broker.example.com",
    "Port": 8883,
    "Username": "your-username",
    "Password": "your-password",
    "ClientId": "unique-per-instance",
    "TopicPrefix": "tenants/abc",
    "CleanSession": false,
    "KeepAliveSeconds": 30,

    "Tls": {
      "Enabled": true,
      "Mode": "Normal",
      "TrustedThumbprint": ""
    },

    "LastWill": {
      "Enabled": true,
      "Topic": "status",
      "OnlinePayload": "ONLINE",
      "OfflinePayload": "OFFLINE",
      "Retain": true
    },

    "OfflineBuffer": {
      "Enabled": true,
      "MaxSize": 1000,
      "DrainPauseMs": 50
    },

    "CircuitBreaker": {
      "Enabled": true,
      "FailureThreshold": 5,
      "ResetMinutes": 2
    },

    "HealthCheck": {
      "Enabled": true,
      "IntervalMinutes": 1,
      "IdleThresholdMinutes": 2
    },

    "Reconnect": {
      "Enabled": true,
      "BaseDelaySeconds": 1,
      "MaxDelaySeconds": 60,
      "UseJitter": true
    }
  }
}
```

---

## 4. Every option explained

### Connection (required basics)

| Option | Default | What it does | When to change |
|---|---|---|---|
| `Host` | `localhost` | Broker hostname or IP | Always |
| `Port` | `1883` | Broker TCP port | Set to `8883` for TLS |
| `Username` | `""` | Auth username | If broker requires auth |
| `Password` | `""` | Auth password | If broker requires auth |
| `ClientId` | random GUID | Unique connection ID | **Always set explicitly** — duplicates kick each other off |
| `TopicPrefix` | `""` | Auto-prefix for publishes | Multi-tenant setups (e.g. `"branches/B001"`) |
| `CleanSession` | `false` | `false` = broker keeps QoS1+ messages across reconnects | Set `true` only for stateless workloads |
| `KeepAliveSeconds` | `30` | Broker ping interval | Increase for poor networks (max 65535) |

### TLS

| Option | Default | What it does | When to use which |
|---|---|---|---|
| `Tls.Enabled` | `false` | Turn on TLS | If broker is on port 8883 |
| `Tls.Mode` | `None` | Cert validation strategy | See below |
| `Tls.TrustedThumbprint` | `""` | Certificate SHA1/SHA256 | Only when `Mode = "Hard"` |

**TLS Mode options:**
- `"None"` — system default cert validation. Most secure, requires valid CA chain.
- `"Normal"` — accept any cert. Fine for dev / internal LAN where you trust the network.
- `"Hard"` — pin a specific cert thumbprint. Most secure for IoT / production with self-signed certs.

### Last Will & Testament (LWT)

| Option | Default | What it does |
|---|---|---|
| `LastWill.Enabled` | `true` | Arm broker-side automatic OFFLINE publish on disconnect |
| `LastWill.Topic` | `status` | Topic to publish on (gets prefixed if `TopicPrefix` set) |
| `LastWill.OnlinePayload` | `ONLINE` | Payload published when we connect |
| `LastWill.OfflinePayload` | `OFFLINE` | Payload broker publishes if our connection dies |
| `LastWill.Retain` | `true` | Broker remembers last status for new subscribers |

**Why this matters:** without LWT, subscribers think you're online even after your process crashed. With LWT, the broker publishes OFFLINE on your behalf when your TCP socket times out.

### Offline Buffer

| Option | Default | What it does |
|---|---|---|
| `OfflineBuffer.Enabled` | `true` | Queue messages when broker unreachable |
| `OfflineBuffer.MaxSize` | `1000` | Max queued messages (drops oldest on overflow) |
| `OfflineBuffer.DrainPauseMs` | `50` | Delay between drained messages on reconnect (prevents flood) |

**Recommendation:** Keep enabled unless you genuinely don't care about messages during outages.

### Circuit Breaker

| Option | Default | What it does |
|---|---|---|
| `CircuitBreaker.Enabled` | `true` | Stop hammering broker after repeated failures |
| `CircuitBreaker.FailureThreshold` | `5` | Consecutive failures before opening |
| `CircuitBreaker.ResetMinutes` | `2` | Wait time before probing again |

**When circuit is OPEN:** publishes go straight to the buffer (no network attempt) until the reset timer elapses.

### Health Check

| Option | Default | What it does |
|---|---|---|
| `HealthCheck.Enabled` | `true` | Periodic ping to detect zombie connections |
| `HealthCheck.IntervalMinutes` | `1` | How often to check |
| `HealthCheck.IdleThresholdMinutes` | `2` | Only ping if channel has been quiet this long |

**Why this exists:** TCP sockets can stay "open" for hours after the network died (NAT timeout, etc.). Health check pings detect this and trigger reconnect.

### Reconnect

| Option | Default | What it does |
|---|---|---|
| `Reconnect.Enabled` | `true` | Auto-reconnect on drops |
| `Reconnect.BaseDelaySeconds` | `1` | Initial retry delay |
| `Reconnect.MaxDelaySeconds` | `60` | Cap on exponential backoff |
| `Reconnect.UseJitter` | `true` | Randomize delays to prevent thundering-herd |

**Always leave `UseJitter = true`** if you have multiple clients reconnecting after a broker recovery.

---

## 5. Configure 3 ways

You can configure the library entirely from `appsettings.json`, entirely in code, or any mix. Pick what suits your situation.

### Way A: From `appsettings.json` only

```csharp
builder.Services.AddResilientMqtt(builder.Configuration.GetSection("Mqtt"));
```

### Way B: Code-only (no config file)

```csharp
builder.Services.AddResilientMqtt(opts =>
{
    opts.Host = "broker.example.com";
    opts.Port = 8883;
    opts.Username = "user";
    opts.Password = Environment.GetEnvironmentVariable("MQTT_PASS")!;
    opts.ClientId = $"client-{Environment.MachineName}";
    opts.Tls.Enabled = true;
});
```

### Way C: Combined + fluent chain (most powerful)

```csharp
builder.Services
    .AddResilientMqtt()
    .Configure(builder.Configuration.GetSection("Mqtt"))     // 1. base from JSON
    .Configure(opts =>                                        // 2. code overrides
    {
        opts.ClientId = $"client-{Environment.MachineName}";
    })
    .ConfigureFromDependency<AgentConfig>((mqtt, agent) =>    // 3. cross-bind
    {
        mqtt.TopicPrefix = $"branches/{agent.BranchId}";
    })
    .UseOfflineBuffer<MySqliteBuffer>()                       // 4. swap impl
    .AddAutoStart()                                           // 5. lifecycle
    .ValidateOnStart();                                       // 6. fail fast
```

**Order matters** — later `Configure` calls override earlier ones. `ConfigureFromDependency` runs after all `Configure` calls.

---

## 6. Auto-derive `ClientId` / `TopicPrefix` from your own config

A very common pattern: you have an `AgentConfig` (or similar) holding identity data, and want to derive the MQTT `ClientId` from it without copy-pasting.

```csharp
public class AgentConfig
{
    public string BranchId { get; set; } = "";
    public string TerminalId { get; set; } = "";
}
```

```csharp
// Register your own config first
builder.Services.AddOptions<AgentConfig>().Bind(builder.Configuration);

// Then MQTT — derive from AgentConfig
builder.Services
    .AddResilientMqtt()
    .Configure(builder.Configuration.GetSection("Mqtt"))
    .ConfigureFromDependency<AgentConfig>((mqtt, agent) =>
    {
        mqtt.ClientId    = $"branch-{agent.BranchId}-{agent.TerminalId}";
        mqtt.TopicPrefix = $"branches/{agent.BranchId}/{agent.TerminalId}";
    })
    .AddAutoStart();
```

Now the same `appsettings.json` template ships to every branch — only `BranchId` / `TerminalId` differ per deployment.

---

## 7. Project-type recipes

### 7.1 Worker Service (Windows Service)

```csharp
// Program.cs
using ResilientMqtt;
using Serilog;

string baseDir = AppContext.BaseDirectory;
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File(
        Path.Combine(baseDir, "Logs", "log-.txt"),
        rollingInterval: RollingInterval.Day)
    .CreateLogger();

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddWindowsService(o => o.ServiceName = "MyAgent");
builder.Logging.ClearProviders();
builder.Logging.AddSerilog();

builder.Services
    .AddResilientMqtt()
    .Configure(builder.Configuration.GetSection("Mqtt"))
    .AddAutoStart()
    .ValidateOnStart();

builder.Services.AddHostedService<Worker>();
builder.Build().Run();
```

**Important `.csproj` settings** for Windows Service:
```xml
<PropertyGroup>
  <TargetFramework>net10.0</TargetFramework>
  <PublishSingleFile>true</PublishSingleFile>
  <SelfContained>true</SelfContained>
  <RuntimeIdentifier>win-x64</RuntimeIdentifier>
</PropertyGroup>

<ItemGroup>
  <None Update="appsettings.json">
    <CopyToOutputDirectory>PreserveNewest</CopyToOutputDirectory>
    <CopyToPublishDirectory>PreserveNewest</CopyToPublishDirectory>
  </None>
</ItemGroup>
```

⚠️ **Do NOT enable "Trim unused code"** in the publish profile — see [section 16](#16-️-critical-il-trimming-gotcha).

### 7.2 ASP.NET Core API

```csharp
// Program.cs
using ResilientMqtt;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddResilientMqtt(builder.Configuration.GetSection("Mqtt"))
    .AddAutoStart()
    .ValidateOnStart();

var app = builder.Build();

app.MapGet("/publish/{topic}", async (string topic, IResilientMqttClient mqtt) =>
{
    await mqtt.PublishAsync(topic, new { at = DateTimeOffset.UtcNow });
    return Results.Ok();
});

app.Run();
```

### 7.3 Blazor Server / WASM

```csharp
// Program.cs
builder.Services
    .AddResilientMqtt(builder.Configuration.GetSection("Mqtt"))
    .AddAutoStart();
```

```razor
@* MyComponent.razor *@
@using ResilientMqtt
@inject IResilientMqttClient Mqtt
@implements IDisposable

<h3>Status: @_status</h3>

@code {
    private string _status = "Connecting...";

    protected override void OnInitialized()
    {
        Mqtt.OnConnected    += OnConnected;
        Mqtt.OnDisconnected += OnDisconnected;
    }

    private async Task OnConnected()
    {
        _status = "🟢 Connected";
        await InvokeAsync(StateHasChanged);
    }

    private async Task OnDisconnected()
    {
        _status = "🔴 Disconnected";
        await InvokeAsync(StateHasChanged);
    }

    public void Dispose()
    {
        Mqtt.OnConnected    -= OnConnected;
        Mqtt.OnDisconnected -= OnDisconnected;
    }
}
```

### 7.4 WPF / WinForms desktop

```csharp
// App.xaml.cs / Program.cs
protected override void OnStartup(StartupEventArgs e)
{
    var services = new ServiceCollection();

    var config = new ConfigurationBuilder()
        .AddJsonFile("appsettings.json")
        .Build();

    services.AddLogging(b => b.AddConsole());
    services.AddSingleton<IConfiguration>(config);
    services.AddResilientMqtt(config.GetSection("Mqtt"))
            .AddAutoStart();

    // The hosted service needs IHostedService runtime — for WPF use this trick:
    var sp = services.BuildServiceProvider();
    var mqtt = sp.GetRequiredService<IResilientMqttClient>();
    _ = mqtt.ConnectAsync();   // fire-and-forget connect

    base.OnStartup(e);
}
```

### 7.5 Console app

```csharp
using ResilientMqtt;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

var services = new ServiceCollection();
services.AddLogging(b => b.AddConsole());
services.AddResilientMqtt(opts =>
{
    opts.Host = "broker.example.com";
    opts.ClientId = "console-tool";
});

var sp = services.BuildServiceProvider();
var mqtt = sp.GetRequiredService<IResilientMqttClient>();

await mqtt.ConnectAsync();
await mqtt.PublishAsync("greetings", new { hello = "world" });

Console.WriteLine("Press any key to exit...");
Console.ReadKey();

await mqtt.DisconnectAsync();
```

---

## 8. Publishing

### Typed (JSON auto-serialize)

```csharp
await mqtt.PublishAsync("orders", new { id = 123, total = 50_000 });
```

### Raw string

```csharp
await mqtt.PublishRawAsync("logs", "user logged in");
```

### Binary

```csharp
byte[] photo = File.ReadAllBytes("photo.jpg");
await mqtt.PublishBytesAsync("camera/snapshot", photo);
```

### Retained message

```csharp
await mqtt.PublishAsync("config/current", config, retain: true);
```

### Different QoS

```csharp
await mqtt.PublishAsync("event", payload, qos: ResilientMqttQos.AtMostOnce);    // fire-and-forget
await mqtt.PublishAsync("event", payload, qos: ResilientMqttQos.AtLeastOnce);   // default
await mqtt.PublishAsync("event", payload, qos: ResilientMqttQos.ExactlyOnce);   // slowest, guaranteed-once
```

### Publishing to absolute topic (skip TopicPrefix)

```csharp
await mqtt.PublishAsync("global/announcements", msg, useTopicPrefix: false);
// → publishes to "global/announcements" (no prefix prepended)
```

---

## 9. Subscribing

### Inline delegate

```csharp
mqtt.OnConnected += async () =>
{
    await mqtt.SubscribeAsync("commands/+", async (topic, payload, ct) =>
    {
        Console.WriteLine($"Command on {topic}: {payload}");
    });
};
```

### Class handler (testable, DI-friendly)

```csharp
public class CommandHandler : IResilientMqttMessageHandler
{
    private readonly IMyService _service;
    public CommandHandler(IMyService service) => _service = service;

    public async Task HandleAsync(string topic, string payload, CancellationToken ct)
    {
        var cmd = System.Text.Json.JsonSerializer.Deserialize<Command>(payload);
        await _service.ExecuteAsync(cmd!, ct);
    }
}

// Register the handler
builder.Services.AddSingleton<CommandHandler>();

// Subscribe inside OnConnected (in a Worker constructor for example)
public Worker(IResilientMqttClient mqtt, CommandHandler handler)
{
    mqtt.OnConnected += async () =>
    {
        await mqtt.SubscribeAsync("commands/+", handler);
    };
}
```

### Wildcards

```csharp
// + = exactly one level
await mqtt.SubscribeAsync("server/commands/+", handler);
// matches: server/commands/ping, server/commands/restart
// does NOT match: server/commands/sub/nested

// # = zero or more levels (must be last)
await mqtt.SubscribeAsync("server/#", handler);
// matches: server/anything, server/a/b/c/d
```

### Subscribe to prefixed topic

By default `SubscribeAsync` uses absolute topics. To listen only to your own prefix:

```csharp
await mqtt.SubscribeAsync("responses/+", handler, useTopicPrefix: true);
// → actual subscription: "{TopicPrefix}/responses/+"
```

### Unsubscribe

```csharp
await mqtt.UnsubscribeAsync("commands/+");
```

---

## 10. Connection lifecycle events

The library exposes two simple events for connection state changes.

### `OnConnected` — fires after every successful connection

```csharp
mqtt.OnConnected += async () =>
{
    // Register subscriptions
    await mqtt.SubscribeAsync("topic1", handler1);
    await mqtt.SubscribeAsync("topic2", handler2);

    // Publish "I'm online" custom message
    await mqtt.PublishAsync("events/agent-ready", new { at = DateTimeOffset.UtcNow });

    // Refresh state
    metrics.IncrementConnects();
};
```

**Fires when:**
- Initial connection succeeds
- Reconnect succeeds after a drop
- Connection re-established after broker outage

### `OnDisconnected` — fires when the connection drops

```csharp
mqtt.OnDisconnected += async () =>
{
    await ui.RunAsync(() => statusLight.SetRed());
    metrics.IncrementDisconnects();
};
```

**Fires when:**
- Network drops unexpectedly
- Broker closes the connection
- Graceful shutdown (you called `DisconnectAsync`)

### Multiple subscribers

You can hook the same event from multiple components — all run sequentially, exceptions in one don't affect others:

```csharp
mqtt.OnConnected += async () => await componentA.OnReadyAsync();
mqtt.OnConnected += async () => await componentB.RefreshTokenAsync();
mqtt.OnConnected += async () => metrics.Counter("mqtt.connects").Increment();
```

### Always unhook in dispose (for non-singleton subscribers)

```csharp
public class MyComponent : IDisposable
{
    private readonly IResilientMqttClient _mqtt;
    public MyComponent(IResilientMqttClient mqtt)
    {
        _mqtt = mqtt;
        _mqtt.OnConnected += OnConnectedAsync;
    }

    private Task OnConnectedAsync() => Task.CompletedTask;

    public void Dispose()
    {
        _mqtt.OnConnected -= OnConnectedAsync;
    }
}
```

### Fine-grained state tracking

If you need `Connecting` / `Reconnecting` / `ShuttingDown` states, use the more detailed `StateChanged` event:

```csharp
mqtt.StateChanged += (_, state) => logger.LogDebug("State: {State}", state);
```

---

## 11. Toggle features on / off

Every resilience feature has an `Enabled` flag. Disable in `appsettings.json`:

```json
{
  "Mqtt": {
    "OfflineBuffer":  { "Enabled": false },
    "CircuitBreaker": { "Enabled": false },
    "HealthCheck":    { "Enabled": false },
    "Reconnect":      { "Enabled": false },
    "LastWill":       { "Enabled": false }
  }
}
```

Or in code:
```csharp
.Configure(opts =>
{
    opts.OfflineBuffer.Enabled = false;
    opts.CircuitBreaker.Enabled = false;
});
```

**When to disable what:**

| Disable | When |
|---|---|
| `OfflineBuffer` | Throughput-only workloads where message loss is acceptable |
| `CircuitBreaker` | Single-shot publishers / scripts |
| `HealthCheck` | Very high-frequency publishers (channel never idle) |
| `Reconnect` | One-shot publish-and-disconnect tools |
| `LastWill` | When status tracking isn't a concern |

**Default is "all on" — keep it that way unless you have a reason.**

---

## 12. Custom offline buffer (persistent storage)

Default buffer is in-memory (lost on process restart). For zero-loss workloads, implement `IOfflineBuffer`:

```csharp
using MQTTnet;
using ResilientMqtt.Buffering;

public class SqliteOfflineBuffer : IOfflineBuffer
{
    public int Count => /* SELECT COUNT(*) FROM offline_messages */;

    public bool TryEnqueue(MqttApplicationMessage message)
    {
        // INSERT topic, payload, qos, retain into SQLite
        return true;
    }

    public bool TryPeek(out MqttApplicationMessage? message)
    {
        // SELECT oldest row, reconstruct MqttApplicationMessage
        message = null;
        return false;
    }

    public bool TryDequeue(out MqttApplicationMessage? message)
    {
        // DELETE oldest row, return reconstructed message
        message = null;
        return false;
    }

    public Task ClearAsync(CancellationToken ct = default) => Task.CompletedTask;
}
```

Register it:

```csharp
builder.Services
    .AddResilientMqtt()
    .Configure(builder.Configuration.GetSection("Mqtt"))
    .UseOfflineBuffer<SqliteOfflineBuffer>();
```

The rest of the library uses your buffer transparently — no other code changes.

---

## 13. Logging — view library logs

The library uses standard `Microsoft.Extensions.Logging`. **It logs wherever you configure your host to log.**

### View library logs only

All library log entries are prefixed with `[ResilientMqttClient]`:

```bash
# Windows
type Logs\log-2026-05-13.txt | findstr "ResilientMqttClient"

# Linux / Mac
grep "ResilientMqttClient" Logs/log-2026-05-13.txt
```

### What gets logged

| Level | Examples |
|---|---|
| `Information` | Connected, Subscribed, Disconnected gracefully, Buffer drain start |
| `Warning` | Disconnected unexpectedly, Circuit breaker OPEN, Buffer overflow, Connect rejected |
| `Error` | Publish failed, Handler threw, Drain failed, OnConnected handler threw |

### Tune verbosity

`appsettings.json`:
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

Serilog:
```csharp
.MinimumLevel.Override("ResilientMqtt", LogEventLevel.Warning)
```

### Verify what config the library loaded (debug helper)

Drop this snippet after `host = builder.Build()`:

```csharp
using (var scope = host.Services.CreateScope())
{
    var mqttOpts = scope.ServiceProvider
        .GetRequiredService<IOptions<ResilientMqttOptions>>().Value;

    Log.Information("=== MQTT Config ===");
    Log.Information("Host: '{Host}', Port: {Port}", mqttOpts.Host, mqttOpts.Port);
    Log.Information("Username: '{User}', Password len: {Len}",
        mqttOpts.Username, mqttOpts.Password?.Length);
    Log.Information("ClientId: '{Id}'", mqttOpts.ClientId);
    Log.Information("TLS Enabled: {Tls}", mqttOpts.Tls.Enabled);
}
```

Run once, verify values, then remove for production.

---

## 14. Common patterns (copy-paste ready)

### Pattern 1: Worker with periodic publish + subscribe

```csharp
public class TelemetryWorker : BackgroundService
{
    private readonly IResilientMqttClient _mqtt;
    private readonly IMyHandler _handler;

    public TelemetryWorker(IResilientMqttClient mqtt, IMyHandler handler)
    {
        _mqtt = mqtt;
        _handler = handler;

        _mqtt.OnConnected += async () =>
        {
            await _mqtt.SubscribeAsync("commands/+", _handler);
        };
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await _mqtt.PublishAsync("telemetry", new
            {
                at = DateTimeOffset.UtcNow,
                cpu = Environment.ProcessorCount,
                memory = GC.GetTotalMemory(false)
            });
            await Task.Delay(TimeSpan.FromSeconds(30), ct);
        }
    }
}
```

### Pattern 2: Request/response style (publish + wait for matching topic)

```csharp
public class RequestService
{
    private readonly IResilientMqttClient _mqtt;
    private readonly Dictionary<string, TaskCompletionSource<string>> _pending = new();

    public RequestService(IResilientMqttClient mqtt)
    {
        _mqtt = mqtt;
        _mqtt.OnConnected += async () =>
        {
            await _mqtt.SubscribeAsync("responses/+", async (topic, payload, ct) =>
            {
                var requestId = topic.Split('/').Last();
                if (_pending.TryGetValue(requestId, out var tcs))
                    tcs.TrySetResult(payload);
            }, useTopicPrefix: true);
        };
    }

    public async Task<string> RequestAsync(string command, TimeSpan timeout)
    {
        var requestId = Guid.NewGuid().ToString("N");
        var tcs = new TaskCompletionSource<string>();
        _pending[requestId] = tcs;

        try
        {
            await _mqtt.PublishAsync($"requests/{command}", new { id = requestId });
            using var cts = new CancellationTokenSource(timeout);
            cts.Token.Register(() => tcs.TrySetCanceled());
            return await tcs.Task;
        }
        finally { _pending.Remove(requestId); }
    }
}
```

### Pattern 3: Send "ready" announcement on connect

```csharp
mqtt.OnConnected += async () =>
{
    await mqtt.PublishAsync("events/agent-ready", new
    {
        agent = Environment.MachineName,
        version = Assembly.GetEntryAssembly()?.GetName().Version?.ToString(),
        startedAt = DateTimeOffset.UtcNow
    });
};
```

### Pattern 4: Distinguish first-connect vs reconnect

```csharp
bool firstConnect = true;
mqtt.OnConnected += async () =>
{
    if (firstConnect)
    {
        firstConnect = false;
        await DoStartupOnlyWorkAsync();
    }
    else
    {
        logger.LogInformation("Back online after reconnect");
    }

    // Always-run logic
    await RegisterSubscriptionsAsync();
};
```

### Pattern 5: Multi-broker via separate processes

The library registers `IResilientMqttClient` as a singleton — one broker per process. For multiple brokers, run multiple processes (each with its own DI container).

### Pattern 6: Dashboard real-time status

```csharp
public class StatusViewModel : INotifyPropertyChanged
{
    private string _status = "Connecting...";
    public string Status
    {
        get => _status;
        set { _status = value; PropertyChanged?.Invoke(this, new(nameof(Status))); }
    }

    public StatusViewModel(IResilientMqttClient mqtt)
    {
        mqtt.OnConnected    += () => { Status = "🟢 Connected";    return Task.CompletedTask; };
        mqtt.OnDisconnected += () => { Status = "🔴 Disconnected"; return Task.CompletedTask; };
    }

    public event PropertyChangedEventHandler? PropertyChanged;
}
```

---

## 15. What to do when things break

### Symptom: "NotAuthorized" repeatedly

1. **Verify credentials** with an MQTT GUI tool (MQTT Explorer) — same username/password
2. **Check trimming wasn't enabled** — see [section 16](#16-️-critical-il-trimming-gotcha)
3. **Check `appsettings.json` is in the publish folder** (look in `bin/Release/.../publish/`)
4. **Drop in the config-verify snippet** (section 13) — see what values actually loaded
5. **Check `ClientId` isn't colliding** with another instance

### Symptom: Connects but messages don't arrive

1. **Topic case sensitivity** — `Commands` ≠ `commands`
2. **TopicPrefix mismatch** — see [section 12 of README.md](README.md#12-topic-prefix-behaviour)
3. **Wildcard mistake** — `+` = one level, `#` = many (must be last)
4. **QoS 0 on lossy network** — try `AtLeastOnce`

### Symptom: TLS handshake fails

1. Try `Tls.Mode = "Normal"` first to isolate TLS-vs-cert issue
2. For `"Hard"` mode, get the thumbprint:
   ```bash
   openssl s_client -connect broker:8883 | openssl x509 -fingerprint -noout
   ```
3. Some brokers need SNI — that's automatic in MQTTnet 5.x

### Symptom: Constant reconnect loop

1. Two clients with same `ClientId` kick each other off forever — make sure ClientId is unique
2. Broker session limits (max connections per user) — check broker logs
3. Network MTU issues on cellular — increase `KeepAliveSeconds`

### Symptom: `OnConnected` never fires

1. Connect log line shows up? If no → check credentials/network
2. Register `OnConnected` **in constructor**, not in `ExecuteAsync` — by the time `ExecuteAsync` runs, the connection may already have happened
3. Look for exceptions inside your handler — they're logged with `[ResilientMqttClient] OnConnected handler threw`

### Symptom: Buffer keeps overflowing (warning logs)

1. Bump `OfflineBuffer.MaxSize`
2. Implement persistent `IOfflineBuffer` (section 12)
3. Reduce publish rate / batch messages

---

## 16. ⚠️ Critical: IL trimming gotcha

If you publish with **"Trim unused code" enabled** (or `<PublishTrimmed>true</PublishTrimmed>` in `.csproj`), configuration binding **silently breaks**:

```
[INF] === MQTT Config ===
[INF] Host: 'localhost'      ← should be from appsettings.json!
[INF] Username: ''           ← should be from appsettings.json!
```

### Why

Trimming uses static analysis to strip "unused" code. Configuration binding uses **reflection** at runtime, which the trimmer can't see. Properties get silently stripped.

### Fix

The library ships with an `ILLink.Descriptors.xml` embedded resource that tells the trimmer NOT to strip the options classes. You shouldn't normally hit this — but if you do, in your **own** `.csproj`:

```xml
<ItemGroup>
  <TrimmerRootAssembly Include="ResilientMqtt" />
</ItemGroup>
```

### Easiest fix: disable trimming

For most projects, **trimming isn't worth the risk**. In your publish profile:

```
☑ Produce single file
☐ Trim unused code     ← UNCHECK
☑ Self-contained
```

You'll get a ~70 MB exe instead of ~25 MB. For Windows Service / branch / IoT deployments, that's completely fine.

---

## 17. Migration from older versions

### If you were using polling loops before lifecycle events

**Before:**
```csharp
protected override async Task ExecuteAsync(CancellationToken ct)
{
    while (!_mqtt.IsConnected && !ct.IsCancellationRequested)
        await Task.Delay(500, ct);

    if (Interlocked.Exchange(ref _subscribed, 1) == 0)
        await _mqtt.SubscribeAsync("topic", handler);

    await Task.Delay(-1, ct);
}
```

**After:**
```csharp
public Worker(IResilientMqttClient mqtt, IMyHandler handler)
{
    mqtt.OnConnected += async () =>
    {
        await mqtt.SubscribeAsync("topic", handler);
    };
}

protected override Task ExecuteAsync(CancellationToken ct)
    => Task.Delay(Timeout.Infinite, ct);
```

**Benefits:** no polling, subscriptions re-register on every reconnect, code is shorter.

**Breaking changes:** none. Both patterns work; the new one is just better.

---

## 18. Quick reference cheat sheet

```csharp
// ── Setup ────────────────────────────────────────────────────────────
builder.Services
    .AddResilientMqtt()
    .Configure(builder.Configuration.GetSection("Mqtt"))
    .AddAutoStart()
    .ValidateOnStart();

// ── Publish ──────────────────────────────────────────────────────────
await mqtt.PublishAsync("topic", new { data = "..." });               // typed JSON
await mqtt.PublishRawAsync("topic", "raw string");                    // raw string
await mqtt.PublishBytesAsync("topic", bytes);                         // binary
await mqtt.PublishAsync("topic", x, retain: true);                    // retained
await mqtt.PublishAsync("topic", x, qos: ResilientMqttQos.AtMostOnce); // QoS
await mqtt.PublishAsync("topic", x, useTopicPrefix: false);           // absolute

// ── Subscribe ────────────────────────────────────────────────────────
mqtt.OnConnected += async () =>
{
    await mqtt.SubscribeAsync("topic", async (t, p, ct) => { /* ... */ });
    await mqtt.SubscribeAsync("topic", classHandler);
    await mqtt.SubscribeAsync("topic", h, qos: ResilientMqttQos.AtLeastOnce);
    await mqtt.SubscribeAsync("topic", h, useTopicPrefix: true);
};

// ── Lifecycle ────────────────────────────────────────────────────────
mqtt.OnConnected    += async () => { /* fires every (re)connect */ };
mqtt.OnDisconnected += async () => { /* fires every disconnect  */ };
mqtt.StateChanged   += (_, s)   => { /* fine-grained states     */ };

// ── State checks ─────────────────────────────────────────────────────
if (mqtt.IsConnected) { /* ... */ }
var state = mqtt.State;   // Disconnected / Connecting / Connected / Reconnecting / ShuttingDown

// ── Manual control (rare — AddAutoStart handles this) ────────────────
await mqtt.ConnectAsync();
await mqtt.DisconnectAsync();
```

---

## License

MIT.

---

*Need to understand what this library is and how it works internally?
See [README.md](README.md).*

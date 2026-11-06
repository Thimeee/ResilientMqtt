# ResilientMqtt 1.1.0: what this version is

> **State:** in release candidates (`1.1.0-rc.N`), final not released. Current state and open items: [STATUS](STATUS.md).
> Every change with its finding number: [CHANGELOG](../CHANGELOG.md). Full option reference: [USAGE](USAGE.md).
> The investigation behind it (#19–#42 and later): [ResilientMqtt-1.0.4-Plan.md](ResilientMqtt-1.0.4-Plan.md). It was
> planned as "1.0.4" and released as 1.1.0 because it adds public API.

---

## Why this version exists

1.0.3 fixed an 18-issue audit done by reading the code. Running the library against a real broker then showed that
several promised behaviours did not happen: `CleanSession=false` kept no session, refused subscriptions were logged as
"Subscribed", refused publishes counted as success, and the caller could never learn **why** a connection failed.

1.1.0 makes the library tell the truth about what happened, fixes those behaviours, and adds what a general-purpose
client needs. **No public member was removed or renamed**, so existing code compiles unchanged.

---

## What is new

| Area | What | How to use it |
|------|------|---------------|
| **Why it failed** | 8 failure kinds: `AuthRejected`, `TlsFailure`, `BrokerUnavailable`, `Network`, `Timeout`, `SessionTakenOver`, `ProtocolError`, `Unknown` | `client.LastFailure` |
| **Connection snapshot** | State, connected since, down since, attempts, flap counter, buffered count, refused subscriptions, denied publishes, protocol, session present | `client.ConnectionInfo` |
| **Events with details** | Failure + how long it was up; downtime + attempts + session present | `ConnectionLost`, `ConnectionRestored` |
| **Publish outcome** | `Sent`, `Buffered`, `Dropped`, `Denied` | `PublishWithResultAsync`, `PublishBytesWithResultAsync` |
| **Wait until ready** | Returns when subscriptions are restored and `OnConnected` has finished | `WaitForConnectionAsync(timeout)` |
| **Real sessions** | MQTT version + session length set explicitly; 1 day by default when `CleanSession=false` | `ProtocolVersion`, `SessionExpirySeconds` |
| **Resumed-session safety** | Queued messages arriving before `OnConnected` subscribes are held, replayed in order, and acknowledged only after delivery | Automatic |
| **Safe dispatch** | A slow handler no longer stalls the receive loop and keep-alive; back-pressure instead of unbounded memory | `Dispatch.Mode = Concurrent`, `MaxConcurrency`, `MaxPending`, `ShutdownWaitSeconds` |
| **TLS** | Private CA with full chain validation; mutual TLS from a PFX or the Windows certificate store | `Tls.Mode = Ca`, `CaCertificatePath`, `CheckCertificateName`, `ClientCertificatePath` / `Password`, `ClientCertificateThumbprint` / `Store` |
| **Wrong credentials** | A longer wait after a refused sign-in, so the broker isn't hammered | `Reconnect.AuthRejectedDelaySeconds` |
| **Log events** | Stable `EventId` on every line, for filtering and alerting | `ResilientMqttLogEvents`, table in README §10a |
| **Bounded logs** | Lines that could repeat per message are limited to one per minute, with a count | Automatic (1201, 1204, 1303; counts in 1205, 1304) |
| **Options validation** | One clear message per problem at startup | `ValidateOnStart()` |

---

## What was wrong and is now fixed

| Before | Now | Finding |
|--------|-----|---------|
| `CleanSession=false` kept no session; nothing queued while offline | Real session | #19 |
| `OnDisconnected` fired on every failed retry | Once per real drop | #21 |
| A connect timeout was silent | Logged, truthful state | #22 |
| A refused subscription was logged as "Subscribed" | Error + `RejectedSubscriptions` | #23 |
| A refused QoS1/2 publish counted as success | `Denied`, logged, not buffered | #24 |
| Zombie connections went unseen on clients that publish regularly | Only inbound traffic counts as alive; an unanswered ping resets | #25 |
| Every reconnect sent each SUBSCRIBE twice, replaying retained messages twice | One SUBSCRIBE per filter | #27 |
| A stuck HalfOpen probe could block all publishes | Released on every path | #28 |
| A graceful stop could leave the retained status "online" | OFFLINE always goes out | #30, #42 |
| Subscribe calls had no timeout | Bounded | #31 |
| Queued messages could be lost right after reconnecting | Held, replayed, acknowledged only after delivery | #44, #53, #63–#66 |
| An MQTT 3.1.1 zombie reset discarded the Will | The client publishes it first | #57 |
| A failing link could flood the log | One line per minute + count | #68–#71 |

The rest (#26, #29, #43, #45–#67) are in the [CHANGELOG](../CHANGELOG.md).

---

## Upgrading from 1.0.x

No code changes are required, but behaviour changes. **Check your `CleanSession` value first:**

| Setting | Effect in 1.0.x | Effect in 1.1.0 |
|---------|----------------|-----------------|
| `CleanSession: true` | Clean start | Same |
| `CleanSession: false` | No session in practice | **Real session for 1 day:** the broker queues QoS1/2 messages while offline, and they arrive on reconnect |
| `CleanSession: false` + `SessionExpirySeconds: 0` | | Same as the old effect |

The full list of behaviour changes: [USAGE §17](USAGE.md#17-migration-from-older-versions).

---

## Quick example

```csharp
// Why is it down?
if (mqtt.LastFailure is { Kind: ResilientMqttFailureKind.AuthRejected } failure)
    logger.LogError("Sign-in refused: {Detail}", failure.Detail);

// Did it really go out?
var outcome = await mqtt.PublishWithResultAsync("telemetry/health", payload);
if (outcome == ResilientMqttPublishOutcome.Denied)
    logger.LogError("The broker refused this topic.");

// React to a drop, with the reason.
mqtt.ConnectionLost += args =>
{
    logger.LogWarning("Lost after {Up}: {Kind}", args.WasConnectedFor, args.Failure.Kind);
    return Task.CompletedTask;
};

// Start only when subscriptions are back.
if (!await mqtt.WaitForConnectionAsync(TimeSpan.FromSeconds(30)))
    logger.LogWarning("Not connected yet: {Kind}", mqtt.LastFailure?.Kind);
```

---

## How it was tested

Results and dates: [STATUS](STATUS.md#test-state). How each layer works: [TESTING](TESTING.md).

**Passed:** 146 unit + integration tests; 36 chaos, cross-broker and TLS tests on Mosquitto, EMQX and HiveMQ; 500-client
reconnect storms; a 20-minute soak with 6 faults and no message gaps.

**Not yet proven:** a multi-hour soak, and a real kiosk lab test. Final `1.1.0` waits for both.

---

## Version history inside 1.1.0

| Version | What | Published |
|---------|------|-----------|
| `rc.1` | The rebuild, fault-injection tests, Docker test kit (git `2b08ca5`) | No |
| `rc.2` | Publish-failure log limit (#68), test for #60 (git `4fd0dba`) | No |
| `rc.3` | Log limits for open circuit and full buffer, count at shutdown (#69–#71) (git `763ddb3`) | ✅ prerelease, 2026-11-06 |
| `1.1.0` | Final, after the open tests pass | Not yet |

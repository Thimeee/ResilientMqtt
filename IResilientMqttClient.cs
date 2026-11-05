using System;
using System.Collections.Generic;
using System.Text;

namespace ResilientMqtt
{

    /// <summary>Quality of Service levels per MQTT spec.</summary>
    public enum ResilientMqttQos
    {
        AtMostOnce = 0,
        AtLeastOnce = 1,
        ExactlyOnce = 2
    }

    /// <summary>Lifecycle status of the underlying connection.</summary>
    public enum ResilientMqttConnectionState
    {
        Disconnected,
        Connecting,
        Connected,
        Reconnecting,
        ShuttingDown
    }

    /// <summary>Inbound message handler contract.</summary>
    public interface IResilientMqttMessageHandler
    {
        Task HandleAsync(string topic, string payload, CancellationToken ct);
    }
    /// <summary>
    /// Public MQTT client contract. Inject this into your services via DI.
    /// </summary>
    /// <remarks>
    /// <para><b>Topic prefix semantics:</b></para>
    /// <para>
    /// When <see cref="ResilientMqttOptions.TopicPrefix"/> is set (e.g. "branches/B001/T006"),
    /// the prefix is applied differently to publish vs subscribe:
    /// <list type="bullet">
    /// <item><b>PublishAsync</b> — prefix is applied by default (set <c>useTopicPrefix: false</c>
    /// to publish to an absolute topic).</item>
    /// <item><b>SubscribeAsync</b> — prefix is NOT applied by default (set <c>useTopicPrefix: true</c>
    /// to listen for prefixed topics, e.g. responses to your own publishes). This default
    /// lets you subscribe to cross-tenant topics like <c>"server/broadcasts/#"</c> without surprises.</item>
    /// </list>
    /// </para>
    /// <para><b>Thread safety:</b> every member may be called from any thread.</para>
    /// <para><b>Members added in 1.1.0</b> have default implementations, so existing implementations of this
    /// interface (test fakes, decorators) keep compiling; the built-in client overrides all of them.</para>
    /// </remarks>
    public interface IResilientMqttClient : IAsyncDisposable
    {
        /// <summary>true when the underlying MQTT connection is up.</summary>
        bool IsConnected { get; }

        /// <summary>Current lifecycle state.</summary>
        ResilientMqttConnectionState State { get; }

        /// <summary>
        /// Raised on every state change. Invoked synchronously on the thread that changed the state (possibly the
        /// MQTT receive loop) — keep handlers quick. Handler exceptions are logged and ignored.
        /// </summary>
        event EventHandler<ResilientMqttConnectionState>? StateChanged;

        /// <summary>
        /// Fires after every successful connection (first connect AND every reconnect).
        /// Handlers run sequentially and are awaited — while the connect lock is held, so keep them quick and
        /// move heavy work to a background task. Exceptions in one handler do not affect others.
        /// <para>Use for: publishing "ready" messages, refreshing connection-scoped state. Existing subscriptions
        /// are restored automatically before this fires — subscribing again here is harmless (no duplicate
        /// SUBSCRIBE is sent) but unnecessary.</para>
        /// <example>
        /// <code>
        /// mqtt.OnConnected += async () =>
        /// {
        ///     await mqtt.PublishAsync("ready", new { at = DateTimeOffset.UtcNow });
        /// };
        /// </code>
        /// </example>
        /// </summary>
        event Func<Task>? OnConnected;

        /// <summary>
        /// Fires once when an established connection drops (before reconnect attempts begin), and after a
        /// graceful <see cref="DisconnectAsync"/>. It does NOT fire for each failed reconnect attempt.
        /// Run on a background task. Use <see cref="ConnectionLost"/> to also get the reason.
        /// </summary>
        event Func<Task>? OnDisconnected;

        /// <summary>
        /// Fires once per outage when the client has been unable to reconnect for
        /// <see cref="ReconnectOptions.StillDownAfterSeconds"/>. Args: (attempt, downDuration).
        /// Use it to alert ops that a client is still offline (read <see cref="LastFailure"/> for why).
        /// Disabled when <c>StillDownAfterSeconds = 0</c>. Invoked on a background task — keep handlers quick.
        /// </summary>
        event Action<int, TimeSpan>? StillDown;

        /// <summary>
        /// (1.1.0) Fires once when an established connection is lost unexpectedly, with the classified reason and
        /// how long the connection had been up. Not raised for a graceful <see cref="DisconnectAsync"/>.
        /// Runs on a background task.
        /// </summary>
        event Func<ResilientMqttConnectionLostArgs, Task>? ConnectionLost { add { } remove { } }

        /// <summary>
        /// (1.1.0) Fires when a connection is established after an outage (a lost connection or failed attempts),
        /// with the downtime, attempts and whether the broker resumed the session. Runs on a background task,
        /// after <see cref="OnConnected"/>.
        /// </summary>
        event Func<ResilientMqttConnectionRestoredArgs, Task>? ConnectionRestored { add { } remove { } }

        /// <summary>
        /// (1.1.0) Why the last connect attempt failed or the connection was lost. null after a successful connect.
        /// </summary>
        ResilientMqttFailure? LastFailure => null;

        /// <summary>(1.1.0) A snapshot of the connection's health — state, outage, failure, counters.</summary>
        ResilientMqttConnectionInfo ConnectionInfo => new(
            State, IsConnected, null, null, 0, 0, LastFailure, 0,
            new Dictionary<string, string>(), 0, string.Empty, false);

        /// <summary>
        /// Connects now. Normally not needed — <c>AddAutoStart()</c> connects at startup and the client reconnects
        /// on its own. Returns false when the attempt failed (the reconnect loop then keeps trying).
        /// </summary>
        Task<bool> ConnectAsync(CancellationToken ct = default);

        /// <summary>
        /// (1.1.0) Waits until the client is connected AND ready — subscriptions restored and
        /// <see cref="OnConnected"/> handlers finished. Returns true when ready, false when <paramref name="timeout"/>
        /// elapses first.
        /// </summary>
        /// <param name="timeout">Maximum time to wait.</param>
        /// <param name="ct">Cancels the wait.</param>
        async Task<bool> WaitForConnectionAsync(TimeSpan timeout, CancellationToken ct = default)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (!IsConnected)
            {
                if (DateTime.UtcNow >= deadline) return false;
                await Task.Delay(100, ct).ConfigureAwait(false);
            }
            return true;
        }

        /// <summary>
        /// Gracefully publishes the shutdown status and sends a clean DISCONNECT.
        /// <para><b>Single-use:</b> after this is called the client is permanently shutting down —
        /// a later <see cref="ConnectAsync"/> is a no-op that returns false. Use it for app shutdown,
        /// not for a transient disconnect (the built-in reconnect handles transient drops).</para>
        /// </summary>
        Task DisconnectAsync(CancellationToken ct = default);

        // ════════════════════════════════════════════════════════════════
        // PUBLISH — default useTopicPrefix: true
        // ════════════════════════════════════════════════════════════════

        /// <summary>
        /// Publish a typed payload (auto JSON serialization; a string payload is sent as-is). Never throws for
        /// connection problems — the message is sent, buffered or dropped. Use
        /// <see cref="PublishWithResultAsync{T}"/> to learn which.
        /// </summary>
        /// <param name="topic">Topic (relative to the prefix unless <paramref name="useTopicPrefix"/> is false).</param>
        /// <param name="payload">Payload — serialized to camelCase JSON unless it is a string.</param>
        /// <param name="retain">Ask the broker to retain the message for future subscribers.</param>
        /// <param name="qos">Quality of service.</param>
        /// <param name="useTopicPrefix">
        /// true (default) → prepend <see cref="ResilientMqttOptions.TopicPrefix"/>.
        /// false → use <paramref name="topic"/> verbatim (absolute topic).
        /// </param>
        /// <param name="ct">Cancels the publish (a cancelled message is dropped, not buffered).</param>
        Task PublishAsync<T>(
            string topic,
            T payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default);

        /// <summary>Publish a raw string payload. See <see cref="PublishAsync{T}"/>.</summary>
        /// <param name="topic">Topic.</param>
        /// <param name="payload">UTF-8 text payload.</param>
        /// <param name="retain">Retain flag.</param>
        /// <param name="qos">Quality of service.</param>
        /// <param name="useTopicPrefix">Prepend the topic prefix (default true).</param>
        /// <param name="ct">Cancellation.</param>
        Task PublishRawAsync(
            string topic,
            string payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default);

        /// <summary>Publish raw bytes (binary). See <see cref="PublishAsync{T}"/>.</summary>
        /// <param name="topic">Topic.</param>
        /// <param name="payload">Binary payload.</param>
        /// <param name="retain">Retain flag.</param>
        /// <param name="qos">Quality of service.</param>
        /// <param name="useTopicPrefix">Prepend the topic prefix (default true).</param>
        /// <param name="ct">Cancellation.</param>
        Task PublishBytesAsync(
            string topic,
            byte[] payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default);

        /// <summary>
        /// (1.1.0) Same as <see cref="PublishAsync{T}"/>, but tells you what happened to the message:
        /// <see cref="ResilientMqttPublishOutcome.Sent"/>, <c>Buffered</c>, <c>Dropped</c> or <c>Denied</c>.
        /// Use it for commands where "was it actually sent?" matters.
        /// </summary>
        /// <param name="topic">Topic.</param>
        /// <param name="payload">Payload (JSON unless a string).</param>
        /// <param name="retain">Retain flag.</param>
        /// <param name="qos">Quality of service (Denied can only be detected for QoS 1/2).</param>
        /// <param name="useTopicPrefix">Prepend the topic prefix (default true).</param>
        /// <param name="ct">Cancellation.</param>
        async Task<ResilientMqttPublishOutcome> PublishWithResultAsync<T>(
            string topic,
            T payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
        {
            await PublishAsync(topic, payload, retain, qos, useTopicPrefix, ct).ConfigureAwait(false);
            return ResilientMqttPublishOutcome.Unknown;
        }

        /// <summary>(1.1.0) Binary variant of <see cref="PublishWithResultAsync{T}"/>.</summary>
        /// <param name="topic">Topic.</param>
        /// <param name="payload">Binary payload.</param>
        /// <param name="retain">Retain flag.</param>
        /// <param name="qos">Quality of service.</param>
        /// <param name="useTopicPrefix">Prepend the topic prefix (default true).</param>
        /// <param name="ct">Cancellation.</param>
        async Task<ResilientMqttPublishOutcome> PublishBytesWithResultAsync(
            string topic,
            byte[] payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default)
        {
            await PublishBytesAsync(topic, payload, retain, qos, useTopicPrefix, ct).ConfigureAwait(false);
            return ResilientMqttPublishOutcome.Unknown;
        }

        // ════════════════════════════════════════════════════════════════
        // SUBSCRIBE — default useTopicPrefix: false
        // ════════════════════════════════════════════════════════════════

        /// <summary>
        /// Subscribe with a typed handler. Remembered and restored automatically after every reconnect.
        /// <para><b>Idempotent:</b> subscribing again to a filter that is already active with the same QoS only
        /// replaces the handler — no second SUBSCRIBE is sent (so retained messages are not replayed).</para>
        /// <para><b>Refusals:</b> if the broker refuses the filter (e.g. ACL), the call does not throw; it is
        /// logged (event 1102) and listed in <see cref="ResilientMqttConnectionInfo.RejectedSubscriptions"/>.</para>
        /// <para><b>Dispatch:</b> an inbound message is delivered to <b>every</b> subscription whose
        /// filter matches its topic — avoid registering overlapping filters (e.g. a wildcard plus an
        /// exact topic) unless you intend the handler to run for each match.</para>
        /// </summary>
        /// <param name="topicPattern">Topic filter (wildcards + and # allowed).</param>
        /// <param name="handler">Handler invoked for matching messages.</param>
        /// <param name="qos">Requested quality of service.</param>
        /// <param name="useTopicPrefix">
        /// false (default) → use <paramref name="topicPattern"/> verbatim.
        /// true → prepend <see cref="ResilientMqttOptions.TopicPrefix"/> (useful when
        /// subscribing to responses meant for this client only).
        /// </param>
        /// <param name="ct">Cancellation.</param>
        Task SubscribeAsync(
            string topicPattern,
            IResilientMqttMessageHandler handler,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = false,
            CancellationToken ct = default);

        /// <summary>Subscribe with an inline delegate (topic, payload, ct).</summary>
        /// <param name="topicPattern">Topic filter.</param>
        /// <param name="handler">Delegate invoked for matching messages.</param>
        /// <param name="qos">Requested quality of service.</param>
        /// <param name="useTopicPrefix">Prepend the topic prefix (default false).</param>
        /// <param name="ct">Cancellation.</param>
        Task SubscribeAsync(
            string topicPattern,
            Func<string, string, CancellationToken, Task> handler,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = false,
            CancellationToken ct = default);

        /// <summary>Unsubscribe a topic pattern. Must match how it was subscribed.</summary>
        /// <param name="topicPattern">Topic filter.</param>
        /// <param name="useTopicPrefix">Same value used when subscribing.</param>
        /// <param name="ct">Cancellation.</param>
        Task UnsubscribeAsync(
            string topicPattern,
            bool useTopicPrefix = false,
            CancellationToken ct = default);
    }

}

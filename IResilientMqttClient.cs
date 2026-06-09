using System;
using System.Collections.Generic;
using System.Text;

namespace ResilientMqtt
{

    // <summary>Quality of Service levels per MQTT spec.</summary>
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
    /// </remarks>
    public interface IResilientMqttClient : IAsyncDisposable
    {
        bool IsConnected { get; }
        ResilientMqttConnectionState State { get; }
        event EventHandler<ResilientMqttConnectionState>? StateChanged;

        /// <summary>
        /// Fires after every successful connection (first connect AND every reconnect).
        /// Handlers run sequentially and are awaited. Exceptions in one handler do not
        /// affect others.
        /// <para>Use for: registering subscriptions, publishing "ready" messages, refreshing
        /// connection-scoped state. Existing subscriptions auto-resubscribe internally —
        /// you only need to add NEW subscriptions here.</para>
        /// <example>
        /// <code>
        /// mqtt.OnConnected += async () =>
        /// {
        ///     await mqtt.SubscribeAsync("commands/+", handler);
        ///     await mqtt.PublishAsync("ready", new { at = DateTimeOffset.UtcNow });
        /// };
        /// </code>
        /// </example>
        /// </summary>
        event Func<Task>? OnConnected;

        /// <summary>
        /// Fires when the connection drops (before reconnect attempts begin).
        /// Handlers run sequentially and are awaited. Use for cleanup, alerting,
        /// or UI state updates.
        /// </summary>
        event Func<Task>? OnDisconnected;

        Task<bool> ConnectAsync(CancellationToken ct = default);
        Task DisconnectAsync(CancellationToken ct = default);

        // ════════════════════════════════════════════════════════════════
        // PUBLISH — default useTopicPrefix: true
        // ════════════════════════════════════════════════════════════════

        /// <summary>Publish a typed payload (auto JSON serialization).</summary>
        /// <param name="useTopicPrefix">
        /// true (default) → prepend <see cref="ResilientMqttOptions.TopicPrefix"/>.
        /// false → use <paramref name="topic"/> verbatim (absolute topic).
        /// </param>
        Task PublishAsync<T>(
            string topic,
            T payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default);

        /// <summary>Publish a raw string payload.</summary>
        Task PublishRawAsync(
            string topic,
            string payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default);

        /// <summary>Publish raw bytes (binary).</summary>
        Task PublishBytesAsync(
            string topic,
            byte[] payload,
            bool retain = false,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = true,
            CancellationToken ct = default);

        // ════════════════════════════════════════════════════════════════
        // SUBSCRIBE — default useTopicPrefix: false
        // ════════════════════════════════════════════════════════════════

        /// <summary>Subscribe with a typed handler. Auto-resubscribes on reconnect.</summary>
        /// <param name="useTopicPrefix">
        /// false (default) → use <paramref name="topicPattern"/> verbatim.
        /// true → prepend <see cref="ResilientMqttOptions.TopicPrefix"/> (useful when
        /// subscribing to responses meant for this client only).
        /// </param>
        Task SubscribeAsync(
            string topicPattern,
            IResilientMqttMessageHandler handler,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = false,
            CancellationToken ct = default);

        /// <summary>Subscribe with an inline delegate.</summary>
        Task SubscribeAsync(
            string topicPattern,
            Func<string, string, CancellationToken, Task> handler,
            ResilientMqttQos qos = ResilientMqttQos.AtLeastOnce,
            bool useTopicPrefix = false,
            CancellationToken ct = default);

        /// <summary>Unsubscribe a topic pattern. Must match how it was subscribed.</summary>
        Task UnsubscribeAsync(
            string topicPattern,
            bool useTopicPrefix = false,
            CancellationToken ct = default);
    }

}
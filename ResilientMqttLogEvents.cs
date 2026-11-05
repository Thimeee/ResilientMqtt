using Microsoft.Extensions.Logging;

namespace ResilientMqtt
{
    /// <summary>
    /// Stable log event ids used by ResilientMqtt, so hosts can filter or alert on them (Seq, ELK, Windows Event Log).
    /// Ids never change meaning between versions. Ranges: 1000 connection · 1100 subscriptions · 1200 publish ·
    /// 1300 offline buffer · 1400 health check · 1500 handlers · 1600 configuration · 1700 circuit breaker.
    /// </summary>
    public static class ResilientMqttLogEvents
    {
        public static readonly EventId Connected = new(1001, nameof(Connected));
        public static readonly EventId ConnectFailed = new(1002, nameof(ConnectFailed));
        public static readonly EventId ConnectTimedOut = new(1003, nameof(ConnectTimedOut));
        public static readonly EventId ConnectionLost = new(1004, nameof(ConnectionLost));
        public static readonly EventId ReconnectScheduled = new(1005, nameof(ReconnectScheduled));
        public static readonly EventId StillDown = new(1006, nameof(StillDown));
        public static readonly EventId DisconnectedGracefully = new(1007, nameof(DisconnectedGracefully));
        public static readonly EventId GracefulDisconnectFailed = new(1008, nameof(GracefulDisconnectFailed));
        public static readonly EventId ConnectionRestored = new(1009, nameof(ConnectionRestored));

        public static readonly EventId Subscribed = new(1101, nameof(Subscribed));
        public static readonly EventId SubscriptionRefused = new(1102, nameof(SubscriptionRefused));
        public static readonly EventId Resubscribing = new(1103, nameof(Resubscribing));
        public static readonly EventId Unsubscribed = new(1104, nameof(Unsubscribed));
        public static readonly EventId SubscribeFailed = new(1105, nameof(SubscribeFailed));

        public static readonly EventId PublishFailed = new(1201, nameof(PublishFailed));
        public static readonly EventId PublishDenied = new(1202, nameof(PublishDenied));
        public static readonly EventId PublishBlockedCircuitOpen = new(1204, nameof(PublishBlockedCircuitOpen));

        public static readonly EventId BufferDraining = new(1301, nameof(BufferDraining));
        public static readonly EventId BufferDrainFailed = new(1302, nameof(BufferDrainFailed));
        public static readonly EventId BufferOverflow = new(1303, nameof(BufferOverflow));

        public static readonly EventId HealthClientDisconnected = new(1401, nameof(HealthClientDisconnected));
        public static readonly EventId HealthPingFailed = new(1402, nameof(HealthPingFailed));
        public static readonly EventId HealthForcedReconnect = new(1403, nameof(HealthForcedReconnect));
        public static readonly EventId HealthTickFailed = new(1404, nameof(HealthTickFailed));

        public static readonly EventId HandlerThrew = new(1501, nameof(HandlerThrew));
        public static readonly EventId EventHandlerThrew = new(1502, nameof(EventHandlerThrew));
        public static readonly EventId MessageUnmatched = new(1503, nameof(MessageUnmatched));
        public static readonly EventId HeldMessagesReplayed = new(1504, nameof(HeldMessagesReplayed));
        public static readonly EventId DispatchBackPressure = new(1505, nameof(DispatchBackPressure));

        public static readonly EventId TlsInsecure = new(1601, nameof(TlsInsecure));
        public static readonly EventId SessionMode = new(1602, nameof(SessionMode));
        public static readonly EventId SessionWithRandomClientId = new(1603, nameof(SessionWithRandomClientId));
        public static readonly EventId SessionSettingIgnored = new(1604, nameof(SessionSettingIgnored));

        public static readonly EventId CircuitOpened = new(1701, nameof(CircuitOpened));
        public static readonly EventId CircuitHalfOpen = new(1702, nameof(CircuitHalfOpen));
        public static readonly EventId CircuitClosed = new(1703, nameof(CircuitClosed));
    }
}

using System;
using System.Collections.Generic;
using System.Text;
using MQTTnet;

namespace ResilientMqtt.Internal
{
    /// <summary>Pluggable retry delay strategy.</summary>
    internal interface IRetryStrategy
    {
        TimeSpan GetNextDelay(int attempt);
    }

    /// <summary>Pluggable connection health monitor.</summary>
    internal interface IHealthCheck : IDisposable
    {
        /// <param name="client">The MQTTnet client to watch.</param>
        /// <param name="onDisconnected">The client reports it is not connected → start reconnecting.</param>
        /// <param name="onPingFailed">The client says "connected" but the broker didn't answer a ping (a dead,
        /// "zombie" connection) → the owner must reset the connection (#25).</param>
        void Start(IMqttClient client, Func<Task> onDisconnected, Func<Task> onPingFailed);

        /// <summary>Proof the broker is alive: call ONLY for INBOUND traffic (a received message, a QoS1/2
        /// acknowledgement, a ping reply). A plain QoS0 send proves nothing — it succeeds on a dead socket (#25).</summary>
        void NotifyActivity();

        void Stop();
    }
}

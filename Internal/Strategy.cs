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
        void Start(IMqttClient client, Func<Task> onUnhealthy);
        void NotifyActivity();
        void Stop();
    }
}

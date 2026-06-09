using System;
using System.Collections.Generic;
using System.Text;
using MQTTnet;

namespace ResilientMqtt.Internal
{
    /// <summary>
    /// No-op health check used when <see cref="HealthCheckOptions.Enabled"/> is false.
    /// All operations are silent no-ops; no timer is started, no pings are sent.
    /// </summary>
    internal sealed class NullHealthCheck : IHealthCheck
    {
        public void Start(IMqttClient client, Func<Task> onUnhealthy) { }
        public void NotifyActivity() { }
        public void Stop() { }
        public void Dispose() { }
    }
}

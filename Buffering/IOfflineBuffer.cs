using System;
using System.Collections.Generic;
using System.Text;
using MQTTnet;

namespace ResilientMqtt.Buffering
{
    public interface IOfflineBuffer
    {
        int Count { get; }
        bool TryEnqueue(MqttApplicationMessage message);
        bool TryPeek(out MqttApplicationMessage? message);
        bool TryDequeue(out MqttApplicationMessage? message);
        Task ClearAsync(CancellationToken ct = default);
    }
}
